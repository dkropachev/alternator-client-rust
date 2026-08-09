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

use alternator_driver::{AlternatorClient, AlternatorConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

#[test]
fn discovery_ignores_environment_http_proxy() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!("http://{}", listener.local_addr().unwrap());
        let proxy_hits = Arc::new(AtomicUsize::new(0));
        let task_hits = proxy_hits.clone();
        let proxy = tokio::spawn(async move {
            while listener.accept().await.is_ok() {
                task_hits.fetch_add(1, Ordering::SeqCst);
            }
        });

        // SAFETY: this integration-test binary contains only this test and
        // mutates its environment before constructing any HTTP client.
        unsafe {
            std::env::set_var("HTTP_PROXY", &proxy_url);
            std::env::set_var("http_proxy", &proxy_url);
            std::env::remove_var("NO_PROXY");
            std::env::remove_var("no_proxy");
            std::env::remove_var("ALL_PROXY");
            std::env::remove_var("all_proxy");
        }

        let client = AlternatorClient::from_conf(
            AlternatorConfig::builder()
                .behavior_version_latest()
                .endpoint_url("http://discovery-proxy.invalid:8000")
                .build(),
        );
        client
            .config()
            .live_nodes()
            .unwrap()
            .update_live_nodes()
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            proxy_hits.load(Ordering::SeqCst),
            0,
            "discovery escaped strict DNS/socket routing through HTTP_PROXY"
        );
        drop(client);
        proxy.abort();
        let _ = proxy.await;
    });
}
