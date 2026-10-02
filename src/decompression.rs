// Copyright ScyllaDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Streaming response decompression.
//!
//! Wraps an `SdkBody` with lazy decompression based on `Content-Encoding` tokens.
//! The decompression is streaming: it does not buffer the entire compressed body
//! before producing decompressed output.

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use futures_util::stream::TryStreamExt;
use http_body::Frame;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::ResponseCompressionAlgorithm;

type BoxedAsyncRead = Pin<Box<dyn AsyncRead + Send + Sync>>;
type DecoderInput = tokio::io::BufReader<BoxedAsyncRead>;

#[derive(Debug)]
struct ResponseEncodingLayerLimitExceeded {
    actual: usize,
    maximum: usize,
}

impl fmt::Display for ResponseEncodingLayerLimitExceeded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "response Content-Encoding has {} layers; maximum supported is {}",
            self.actual, self.maximum
        )
    }
}

impl std::error::Error for ResponseEncodingLayerLimitExceeded {}

pub(crate) fn validate_response_encoding_layer_count(
    count: usize,
    maximum: usize,
) -> Result<(), BoxError> {
    if count > maximum {
        return Err(ResponseEncodingLayerLimitExceeded {
            actual: count,
            maximum,
        }
        .into());
    }

    Ok(())
}

/// Wraps an `SdkBody` with streaming decompression for the given encodings.
///
/// Encodings are listed in HTTP `Content-Encoding` application order (innermost first).
/// Decoding reverses this: the last listed encoding is decoded first from the raw bytes.
pub(crate) fn wrap_decompressed_body(
    body: SdkBody,
    encodings: Vec<ResponseCompressionAlgorithm>,
    max_encoding_layers: usize,
    max_decompressed_bytes: usize,
) -> Result<SdkBody, BoxError> {
    validate_response_encoding_layer_count(encodings.len(), max_encoding_layers)?;

    // Convert SdkBody into an AsyncRead via http-body -> Stream -> StreamReader
    let body_stream = http_body_util::BodyStream::new(body)
        .try_filter_map(|frame| async move { Ok(frame.into_data().ok()) });

    let reader: BoxedAsyncRead = Box::pin(StreamReader::new(
        body_stream.map_err(std::io::Error::other),
    ));

    // Decode in reverse application order, starting with the outermost layer.
    let reader: BoxedAsyncRead = encodings
        .into_iter()
        .rev()
        .fold(reader, |reader, algorithm| {
            let decoder: BoxedAsyncRead = Box::pin(CompleteStageDecoder::new(reader, algorithm));

            Box::pin(SizeLimitedReader::new(decoder, max_decompressed_bytes))
        });

    // Convert the decoded AsyncRead back into an SdkBody
    let decoded_stream = ReaderStream::new(reader);
    let body_impl = StreamingDecompressedBody::new(decoded_stream);

    Ok(SdkBody::from_body_1_x(body_impl))
}

/// A custom `http_body::Body` implementation that wraps a `ReaderStream`
/// producing decompressed `Bytes` chunks.
struct StreamingDecompressedBody {
    inner: ReaderStream<BoxedAsyncRead>,
}

impl StreamingDecompressedBody {
    fn new(stream: ReaderStream<BoxedAsyncRead>) -> Self {
        Self { inner: stream }
    }
}

impl http_body::Body for StreamingDecompressedBody {
    type Data = Bytes;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        use futures_util::Stream;

        let this = self.get_mut();
        let inner = Pin::new(&mut this.inner);

        match inner.poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            ))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

enum DecoderInner {
    Gzip(async_compression::tokio::bufread::GzipDecoder<DecoderInput>),
    Deflate(async_compression::tokio::bufread::ZlibDecoder<DecoderInput>),
}

impl DecoderInner {
    fn poll_decode(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self {
            Self::Gzip(decoder) => Pin::new(decoder).poll_read(cx, buffer),
            Self::Deflate(decoder) => Pin::new(decoder).poll_read(cx, buffer),
        }
    }

    fn poll_input(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self {
            Self::Gzip(decoder) => Pin::new(decoder.get_mut()).poll_read(cx, buffer),
            Self::Deflate(decoder) => Pin::new(decoder.get_mut()).poll_read(cx, buffer),
        }
    }
}

/// Drains a decoder's complete input before reporting EOF.
///
/// A nested decoder can finish its compressed member without polling its input
/// again. Draining ensures the preceding stage's size limiter observes its full
/// output, including an over-limit byte after an otherwise complete member.
struct CompleteStageDecoder {
    inner: DecoderInner,
    decoding_complete: bool,
}

impl CompleteStageDecoder {
    fn new(reader: BoxedAsyncRead, algorithm: ResponseCompressionAlgorithm) -> Self {
        let input = tokio::io::BufReader::new(reader);
        let inner = match algorithm {
            ResponseCompressionAlgorithm::Gzip => {
                DecoderInner::Gzip(async_compression::tokio::bufread::GzipDecoder::new(input))
            }
            ResponseCompressionAlgorithm::Deflate => {
                DecoderInner::Deflate(async_compression::tokio::bufread::ZlibDecoder::new(input))
            }
        };

        Self {
            inner,
            decoding_complete: false,
        }
    }
}

impl AsyncRead for CompleteStageDecoder {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let this = self.get_mut();
        if !this.decoding_complete {
            let filled_before = buffer.filled().len();
            match this.inner.poll_decode(cx, buffer) {
                Poll::Ready(Ok(())) if buffer.filled().len() == filled_before => {
                    this.decoding_complete = true;
                }
                result => return result,
            }
        }

        let mut discarded = [0; 8192];
        let mut drain_buffer = ReadBuf::new(&mut discarded);
        match this.inner.poll_input(cx, &mut drain_buffer) {
            Poll::Ready(Ok(())) if drain_buffer.filled().is_empty() => Poll::Ready(Ok(())),
            Poll::Ready(Ok(())) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            result => result,
        }
    }
}

struct SizeLimitedReader {
    inner: tokio::io::Take<BoxedAsyncRead>,
    limit: usize,
}

impl SizeLimitedReader {
    fn new(inner: BoxedAsyncRead, limit: usize) -> Self {
        Self {
            inner: inner.take(limit as u64),
            limit,
        }
    }
}

impl AsyncRead for SizeLimitedReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.inner.limit() > 0 || buffer.remaining() == 0 {
            return Pin::new(&mut this.inner).poll_read(cx, buffer);
        }

        // `Take` returns EOF at its limit. Probe one more byte so an oversized
        // stream fails explicitly instead of looking like a truncated response.
        let mut byte = [0];
        let mut probe = ReadBuf::new(&mut byte);
        match this.inner.get_mut().as_mut().poll_read(cx, &mut probe) {
            Poll::Ready(Ok(())) if probe.filled().is_empty() => Poll::Ready(Ok(())),
            Poll::Ready(Ok(())) => Poll::Ready(Err(std::io::Error::other(
                DecompressedResponseSizeLimitExceeded { limit: this.limit },
            ))),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
struct DecompressedResponseSizeLimitExceeded {
    limit: usize,
}

impl fmt::Display for DecompressedResponseSizeLimitExceeded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "decompressed response exceeds {} byte limit",
            self.limit
        )
    }
}

impl std::error::Error for DecompressedResponseSizeLimitExceeded {}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::{GzEncoder, ZlibEncoder};
    use http_body_util::BodyExt;
    use std::io::Write;

    fn encode_once(data: &[u8], algorithm: ResponseCompressionAlgorithm) -> Vec<u8> {
        match algorithm {
            ResponseCompressionAlgorithm::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(data).unwrap();
                encoder.finish().unwrap()
            }
            ResponseCompressionAlgorithm::Deflate => {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(data).unwrap();
                encoder.finish().unwrap()
            }
        }
    }

    fn encode(data: &[u8], encodings: &[ResponseCompressionAlgorithm]) -> Vec<u8> {
        encodings.iter().fold(data.to_vec(), |data, encoding| {
            encode_once(&data, *encoding)
        })
    }

    #[tokio::test]
    async fn accepts_response_at_expanded_size_limit() {
        let data = vec![b'a'; 1024];
        let compressed = encode_once(&data, ResponseCompressionAlgorithm::Gzip);
        let body = wrap_decompressed_body(
            SdkBody::from(compressed),
            vec![ResponseCompressionAlgorithm::Gzip],
            1,
            data.len(),
        )
        .unwrap();

        let decoded = body.collect().await.unwrap().to_bytes();
        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn rejects_response_over_expanded_size_limit() {
        let data = vec![b'a'; 1025];
        let compressed = encode_once(&data, ResponseCompressionAlgorithm::Gzip);
        let body = wrap_decompressed_body(
            SdkBody::from(compressed),
            vec![ResponseCompressionAlgorithm::Gzip],
            1,
            data.len() - 1,
        )
        .unwrap();

        let error = body.collect().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("decompressed response exceeds 1024 byte limit")
        );
    }

    #[tokio::test]
    async fn expanded_size_limit_applies_to_each_layer() {
        let data: Vec<_> = (0..=255).collect();
        let once_encoded = encode_once(&data, ResponseCompressionAlgorithm::Gzip);
        assert!(once_encoded.len() > data.len());
        let twice_encoded = encode_once(&once_encoded, ResponseCompressionAlgorithm::Gzip);
        let body = wrap_decompressed_body(
            SdkBody::from(twice_encoded),
            vec![
                ResponseCompressionAlgorithm::Gzip,
                ResponseCompressionAlgorithm::Gzip,
            ],
            2,
            data.len(),
        )
        .unwrap();

        let error = body.collect().await.unwrap_err();
        assert!(error.to_string().contains("decompressed response exceeds"));
    }

    #[tokio::test]
    async fn validates_complete_intermediate_stage_at_size_limit() {
        let data = b"bounded response decompression";
        let inner = encode_once(data, ResponseCompressionAlgorithm::Gzip);
        let encodings = vec![
            ResponseCompressionAlgorithm::Gzip,
            ResponseCompressionAlgorithm::Gzip,
        ];

        let exact = encode_once(&inner, ResponseCompressionAlgorithm::Gzip);
        let body = wrap_decompressed_body(SdkBody::from(exact), encodings.clone(), 2, inner.len())
            .unwrap();
        assert_eq!(body.collect().await.unwrap().to_bytes(), data.as_slice());

        let mut oversized_inner = inner.clone();
        oversized_inner.push(0);
        let oversized = encode_once(&oversized_inner, ResponseCompressionAlgorithm::Gzip);
        let body =
            wrap_decompressed_body(SdkBody::from(oversized), encodings, 2, inner.len()).unwrap();
        let error = body.collect().await.unwrap_err();
        assert!(error.to_string().contains("decompressed response exceeds"));
    }

    #[tokio::test]
    async fn accepts_maximum_encoding_layers() {
        let encodings = vec![
            ResponseCompressionAlgorithm::Gzip,
            ResponseCompressionAlgorithm::Deflate,
            ResponseCompressionAlgorithm::Gzip,
            ResponseCompressionAlgorithm::Deflate,
            ResponseCompressionAlgorithm::Gzip,
        ];
        let data = b"bounded response decompression";
        let compressed = encode(data, &encodings);
        let body = wrap_decompressed_body(SdkBody::from(compressed), encodings, 5, 1024).unwrap();

        let decoded = body.collect().await.unwrap().to_bytes();
        assert_eq!(decoded, data.as_slice());
    }

    #[test]
    fn rejects_too_many_encoding_layers() {
        let error = wrap_decompressed_body(
            SdkBody::empty(),
            vec![ResponseCompressionAlgorithm::Gzip; 6],
            5,
            1024,
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "response Content-Encoding has 6 layers; maximum supported is 5"
        );
    }
}
