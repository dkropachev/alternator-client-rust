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

use aws_smithy_runtime_api::http::Request;

/// To be used in an interceptor
///
/// Removes unwanted headers from the given request
pub(crate) fn strip_headers(request: &mut Request, preserve_auth_headers: bool) {
    let headers = request.headers_mut();

    const BASE_WHITELIST: [&str; 6] = [
        "host",
        "x-amz-target",
        "content-length",
        "content-type",
        "accept-encoding",
        "content-encoding",
    ];
    const AUTH_WHITELIST: [&str; 4] = [
        "authorization",
        "x-amz-date",
        "x-amz-user-agent",
        "x-amz-security-token",
    ];

    let signed_headers = if preserve_auth_headers {
        let mut authorization_values = headers.get_all("authorization");
        let Some(authorization) = authorization_values.next() else {
            // Header stripping is only an optimization. If a signed request
            // uses an authorization format we do not understand, leave it
            // intact rather than risk invalidating its signature.
            return;
        };
        if authorization_values.next().is_some() {
            return;
        }
        let Some(signed_headers) = parse_signed_headers(authorization) else {
            return;
        };
        signed_headers
    } else {
        Vec::new()
    };

    let unallowed_keys: Vec<String> = headers
        .iter()
        .map(|(key, _)| key.to_string())
        .filter(|key| {
            !(BASE_WHITELIST.contains(&key.as_str())
                || preserve_auth_headers
                    && (AUTH_WHITELIST.contains(&key.as_str())
                        || signed_headers.iter().any(|signed| *signed == key)))
        })
        .collect();

    for key in unallowed_keys {
        headers.remove(key);
    }
}

fn parse_signed_headers(authorization: &str) -> Option<Vec<&str>> {
    let (algorithm, parameters) = authorization.split_once(' ')?;
    if algorithm != "AWS4-HMAC-SHA256" {
        return None;
    }

    let mut signed_headers = None;
    for parameter in parameters.split(',') {
        let parameter = parameter.trim();
        if parameter.is_empty() || parameter.bytes().any(|byte| byte.is_ascii_whitespace()) {
            return None;
        }
        let (name, value) = parameter.split_once('=')?;
        if name.is_empty() || value.is_empty() || value.contains('=') {
            return None;
        }
        if name != "SignedHeaders" {
            continue;
        }
        if signed_headers.is_some() {
            return None;
        }
        signed_headers = Some(value.trim());
    }

    let headers: Vec<_> = signed_headers?.split(';').collect();
    let canonical = headers.windows(2).all(|pair| pair[0] < pair[1]);
    (canonical
        && headers
            .iter()
            .all(|header| is_canonical_header_name(header)))
    .then_some(headers)
}

fn is_canonical_header_name(header: &str) -> bool {
    !header.is_empty()
        && header.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with_headers() -> Request {
        let mut request = Request::empty();
        request.headers_mut().insert("host", "localhost:8000");
        request
            .headers_mut()
            .insert("x-amz-target", "DynamoDB_20120810.PutItem");
        request.headers_mut().insert("content-length", "10");
        request
            .headers_mut()
            .insert("content-type", "application/x-amz-json-1.0");
        request.headers_mut().insert("accept-encoding", "gzip");
        request.headers_mut().insert("content-encoding", "gzip");
        request.headers_mut().insert(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20261001/us-east-1/dynamodb/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date;x-amz-security-token;x-amz-user-agent;x-custom-signed, \
             Signature=signature",
        );
        request
            .headers_mut()
            .insert("x-amz-date", "20260626T120000Z");
        request
            .headers_mut()
            .insert("x-amz-user-agent", "aws-sdk-rust/test");
        request
            .headers_mut()
            .insert("x-amz-security-token", "session-token");
        request
            .headers_mut()
            .insert("x-custom-signed", "preserve me");
        request
            .headers_mut()
            .insert("x-custom-unsigned", "remove me");
        request.headers_mut().insert("user-agent", "test");
        request.headers_mut().insert("amz-sdk-request", "attempt=1");
        request
    }

    #[test]
    fn no_auth_allowlist_drops_auth_headers_and_keeps_compression_headers() {
        let mut request = request_with_headers();

        strip_headers(&mut request, false);

        assert!(request.headers().contains_key("host"));
        assert!(request.headers().contains_key("x-amz-target"));
        assert!(request.headers().contains_key("content-length"));
        assert!(request.headers().contains_key("content-type"));
        assert!(request.headers().contains_key("accept-encoding"));
        assert!(request.headers().contains_key("content-encoding"));
        assert!(!request.headers().contains_key("authorization"));
        assert!(!request.headers().contains_key("x-amz-date"));
        assert!(!request.headers().contains_key("x-amz-user-agent"));
        assert!(!request.headers().contains_key("x-amz-security-token"));
        assert!(!request.headers().contains_key("x-custom-signed"));
        assert!(!request.headers().contains_key("x-custom-unsigned"));
        assert!(!request.headers().contains_key("user-agent"));
        assert!(!request.headers().contains_key("amz-sdk-request"));
    }

    #[test]
    fn signed_allowlist_preserves_auth_headers_and_compression_headers() {
        let mut request = request_with_headers();

        strip_headers(&mut request, true);

        assert!(request.headers().contains_key("host"));
        assert!(request.headers().contains_key("x-amz-target"));
        assert!(request.headers().contains_key("content-length"));
        assert!(request.headers().contains_key("content-type"));
        assert!(request.headers().contains_key("accept-encoding"));
        assert!(request.headers().contains_key("content-encoding"));
        assert!(request.headers().contains_key("authorization"));
        assert!(request.headers().contains_key("x-amz-date"));
        assert!(request.headers().contains_key("x-amz-user-agent"));
        assert!(request.headers().contains_key("x-amz-security-token"));
        assert!(request.headers().contains_key("x-custom-signed"));
        assert!(!request.headers().contains_key("x-custom-unsigned"));
        assert!(!request.headers().contains_key("user-agent"));
        assert!(!request.headers().contains_key("amz-sdk-request"));
    }

    #[test]
    fn signed_request_with_unrecognized_authorization_is_left_intact() {
        let mut request = request_with_headers();
        request.headers_mut().insert(
            "authorization",
            "Future-Signing-Algorithm SignedHeaders=host;x-custom-signed, Signature=signature",
        );

        strip_headers(&mut request, true);

        assert!(request.headers().contains_key("x-custom-signed"));
        assert!(request.headers().contains_key("x-custom-unsigned"));
        assert!(request.headers().contains_key("amz-sdk-request"));
    }

    #[test]
    fn signed_request_with_multiple_authorization_values_is_left_intact() {
        let mut request = request_with_headers();
        request.headers_mut().append(
            "authorization",
            "AWS4-HMAC-SHA256 SignedHeaders=host;x-custom-signed, Signature=other-signature",
        );

        strip_headers(&mut request, true);

        assert!(request.headers().contains_key("x-custom-signed"));
        assert!(request.headers().contains_key("x-custom-unsigned"));
        assert!(request.headers().contains_key("amz-sdk-request"));
    }

    #[test]
    fn signed_headers_parser_matches_an_exact_reordered_parameter() {
        let authorization = "AWS4-HMAC-SHA256 Signature=signature, \
            NotSignedHeaders=x-ignored, SignedHeaders=host;x-custom-signed, \
            Credential=test/20261001/us-east-1/dynamodb/aws4_request";

        assert_eq!(
            parse_signed_headers(authorization),
            Some(vec!["host", "x-custom-signed"])
        );
    }

    #[test]
    fn signed_headers_parser_rejects_missing_or_empty_names() {
        assert_eq!(parse_signed_headers("AWS4-HMAC-SHA256 Signature=x"), None);
        assert_eq!(
            parse_signed_headers("AWS4-HMAC-SHA256 SignedHeaders=, Signature=x"),
            None
        );
        assert_eq!(
            parse_signed_headers("AWS4-HMAC-SHA256 SignedHeaders=host;;x-test, Signature=x"),
            None
        );
        assert_eq!(
            parse_signed_headers("Future-Signing-Algorithm SignedHeaders=host, Signature=x"),
            None
        );
        assert_eq!(
            parse_signed_headers(
                "AWS4-HMAC-SHA256 SignedHeaders=host, SignedHeaders=host;x-test, Signature=x"
            ),
            None
        );
        assert_eq!(
            parse_signed_headers(
                "AWS4-HMAC-SHA256 SignedHeaders=host, Credential=test SignedHeaders=host;x-test, Signature=x"
            ),
            None
        );
        assert_eq!(
            parse_signed_headers("AWS4-HMAC-SHA256 SignedHeaders=Host;x-test, Signature=x"),
            None
        );
        assert_eq!(
            parse_signed_headers("AWS4-HMAC-SHA256 SignedHeaders=x-test;host, Signature=x"),
            None
        );
    }
}
