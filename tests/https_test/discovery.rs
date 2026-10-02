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

//! HTTPS test for the `/localnodes` discovery path plus a follow-up API call.

use crate::https_test::https_test_context::*;
use crate::https_test::proxy::forward_on_request;

use alternator_driver::AlternatorClient;
use alternator_driver::AlternatorConfig;
use aws_sdk_dynamodb::config::Credentials;
use aws_smithy_http_client::tls::rustls_provider::CryptoMode;
use aws_smithy_http_client::tls::{Provider, TlsContext, TrustStore};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::{Method, Request, Response};
use serial_test::serial;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use test_context::test_context;
use tokio::sync::Mutex;

const POLLING_TIMEOUT: Duration = Duration::from_secs(5);
const POLLING_INTERVAL: Duration = Duration::from_millis(50);

struct RequestCounts {
    localnodes_gets: Arc<AtomicUsize>,
    api_posts: Arc<AtomicUsize>,
}

async fn configure_discovery_proxy(ctx: &HttpsTestContext) -> RequestCounts {
    let proxy_local = ctx.get_proxy_address();
    let localnodes_gets = Arc::new(AtomicUsize::new(0));
    let api_posts = Arc::new(AtomicUsize::new(0));
    let localnodes_gets_for_proxy = localnodes_gets.clone();
    let api_posts_for_proxy = api_posts.clone();

    ctx.set_on_request(
        move |request: Request<Incoming>, sender: Arc<Mutex<SendRequest<Full<Bytes>>>>| {
            let proxy_local = proxy_local.clone();
            let localnodes_gets = localnodes_gets_for_proxy.clone();
            let api_posts = api_posts_for_proxy.clone();
            async move {
                if request.method() == Method::GET && request.uri().path() == "/localnodes" {
                    // Return the proxy itself as the discovered node so both discovery and
                    // the subsequent API call stay on the HTTPS test path.
                    localnodes_gets.fetch_add(1, Ordering::Relaxed);
                    let body = format!("[\"{}\"]", proxy_local);
                    Response::new(Full::new(Bytes::from(body)))
                } else {
                    if request.method() == Method::POST && request.uri().path() == "/" {
                        api_posts.fetch_add(1, Ordering::Relaxed);
                    }
                    forward_on_request(request, sender).await
                }
            }
        },
    )
    .await;

    RequestCounts {
        localnodes_gets,
        api_posts,
    }
}

async fn assert_discovery_and_api_succeed(client: AlternatorClient, counts: RequestCounts) {
    // Poll for the discovery request instead of sleeping for an arbitrary interval.
    tokio::time::timeout(POLLING_TIMEOUT, async {
        loop {
            if counts.localnodes_gets.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::time::sleep(POLLING_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "timed out waiting for /localnodes after {:?}",
            POLLING_TIMEOUT
        )
    });

    let result = client.list_tables().send().await;
    assert!(
        result.is_ok(),
        "ListTables after discovery failed: {:?}",
        result.err()
    );
    // Confirm a regular Alternator API request also went through the HTTPS proxy.
    assert!(
        counts.api_posts.load(Ordering::Relaxed) > 0,
        "expected at least one API POST request through the proxy"
    );
}

#[test_context(HttpsTestContext)]
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn test_https_discovery(ctx: &mut HttpsTestContext) {
    let counts = configure_discovery_proxy(ctx).await;
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .scheme("https")
            .seed_hosts([ctx.get_proxy_host()])
            .port(ctx.get_proxy_port())
            .credentials_provider(Credentials::for_tests_with_session_token())
            .build(),
    );

    assert_discovery_and_api_succeed(client, counts).await;
}

#[test_context(HttpsTestContext)]
#[tokio::test(flavor = "current_thread")]
#[serial]
async fn test_custom_tls_client_applies_to_https_discovery(ctx: &mut HttpsTestContext) {
    let counts = configure_discovery_proxy(ctx).await;
    let tls_context = TlsContext::builder()
        .with_trust_store(TrustStore::empty().with_pem_certificate(ctx.get_ca_pem()))
        .build()
        .expect("custom TLS context should be valid");
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
        .tls_context(tls_context)
        .build_https();

    // If discovery ignores the custom HTTP client, this generated CA is unknown
    // to its native trust store and the /localnodes request cannot succeed.
    ctx.remove_ca_from_native_roots();
    let client = AlternatorClient::from_conf(
        AlternatorConfig::builder()
            .scheme("https")
            .seed_hosts([ctx.get_proxy_host()])
            .port(ctx.get_proxy_port())
            .http_client(http_client)
            .credentials_provider(Credentials::for_tests_with_session_token())
            .build(),
    );

    assert_discovery_and_api_succeed(client, counts).await;
}
