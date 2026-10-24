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

//! Deterministic discovery shutdown tests using a programmable SDK connector.

use alternator_driver::{AlternatorClient, AlternatorConfig, KeyRouteAffinityType};
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
struct DiscoveryHttp {
    hold_topology: bool,
    hold_partition_key: bool,
    scans: Arc<AtomicUsize>,
    describes: Arc<AtomicUsize>,
    entered: Arc<Semaphore>,
    dropped: Arc<Semaphore>,
}

impl DiscoveryHttp {
    fn new(hold_topology: bool, hold_partition_key: bool) -> Self {
        Self {
            hold_topology,
            hold_partition_key,
            scans: Arc::default(),
            describes: Arc::default(),
            entered: Arc::new(Semaphore::new(0)),
            dropped: Arc::new(Semaphore::new(0)),
        }
    }

    fn config(&self) -> AlternatorConfig {
        AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .http_client(self.clone())
            .key_route_affinity(KeyRouteAffinityType::AnyWrite)
            .active_interval(Duration::from_secs(3600))
            .idle_interval(Duration::from_secs(3600))
            .build()
    }

    fn client(&self) -> AlternatorClient {
        AlternatorClient::from_conf(self.config())
    }

    async fn wait_entered(&self) {
        tokio::time::timeout(Duration::from_secs(1), self.entered.acquire())
            .await
            .expect("discovery must enter transport")
            .unwrap()
            .forget();
    }

    async fn wait_dropped(&self) {
        tokio::time::timeout(Duration::from_secs(1), self.dropped.acquire())
            .await
            .expect("shutdown must drop in-flight transport")
            .unwrap()
            .forget();
    }
}

impl HttpClient for DiscoveryHttp {
    fn http_connector(
        &self,
        _: &HttpConnectorSettings,
        _: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(self.clone())
    }
}

struct OnDrop(Arc<Semaphore>);

impl Drop for OnDrop {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

impl HttpConnector for DiscoveryHttp {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let target = request.headers().get("x-amz-target").unwrap_or_default();
        let (hold, response) = if target.ends_with(".Scan") {
            self.scans.fetch_add(1, Ordering::SeqCst);
            (
                self.hold_topology,
                r#"{"Items":[{"rpc_address":{"S":"127.0.0.1"},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}}]}"#,
            )
        } else if target.ends_with(".DescribeTable") {
            self.describes.fetch_add(1, Ordering::SeqCst);
            (
                self.hold_partition_key,
                r#"{"Table":{"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}]}}"#,
            )
        } else {
            (false, "{}")
        };
        let entered = self.entered.clone();
        let dropped = self.dropped.clone();
        HttpConnectorFuture::new(async move {
            if hold {
                let _on_drop = OnDrop(dropped);
                entered.add_permits(1);
                std::future::pending::<()>().await;
            }
            Ok(HttpResponse::try_from(
                http::Response::builder()
                    .status(200)
                    .header("content-type", "application/x-amz-json-1.0")
                    .body(SdkBody::from(response))
                    .unwrap(),
            )
            .unwrap())
        })
    }
}

async fn put_item(client: &AlternatorClient, table: &str) {
    client
        .put_item()
        .table_name(table)
        .item(
            "pk",
            aws_sdk_dynamodb::types::AttributeValue::S("key".into()),
        )
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn shutdown_from_clones_joins_topology_io_and_isolates_other_clients() {
    let http = DiscoveryHttp::new(true, false);
    let client = http.client();
    let clone = client.clone();
    http.wait_entered().await;
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(client.shutdown(), clone.shutdown());
    })
    .await
    .expect("shutdown must interrupt topology I/O");
    http.wait_dropped().await;
    let scans = http.scans.load(Ordering::SeqCst);
    put_item(&clone, "after_shutdown").await;
    clone.shutdown().await;
    assert_eq!(http.scans.load(Ordering::SeqCst), scans);
    assert_eq!(http.describes.load(Ordering::SeqCst), 0);

    let independent_http = DiscoveryHttp::new(true, false);
    let independent = independent_http.client();
    independent_http.wait_entered().await;
    put_item(&independent, "other_client").await;
    independent.shutdown().await;
    independent_http.wait_dropped().await;
    assert_eq!(tokio::spawn(async { 42 }).await.unwrap(), 42);
}

#[tokio::test]
async fn shutdown_joins_partition_key_io_and_blocks_new_discovery() {
    let http = DiscoveryHttp::new(false, true);
    let client = http.client();
    put_item(&client, "first_table").await;
    http.wait_entered().await;
    tokio::time::timeout(Duration::from_secs(1), client.shutdown())
        .await
        .expect("shutdown must interrupt DescribeTable I/O");
    http.wait_dropped().await;
    assert_eq!(http.describes.load(Ordering::SeqCst), 1);
    put_item(&client, "second_table").await;
    assert_eq!(http.describes.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn last_client_drop_stops_discovery_but_dropping_one_clone_keeps_it_running() {
    let http = DiscoveryHttp::new(true, false);
    let client = http.client();
    let clone = client.clone();
    http.wait_entered().await;
    drop(client);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), http.dropped.acquire())
            .await
            .is_err(),
        "dropping one clone must keep in-flight discovery running"
    );
    drop(clone);
    http.wait_dropped().await;
}

#[test]
fn shutdown_can_be_awaited_on_a_different_runtime() {
    let http = DiscoveryHttp::new(true, false);
    let owner = tokio::runtime::Runtime::new().unwrap();
    let client = owner.block_on(async {
        let client = http.client();
        http.wait_entered().await;
        client
    });
    let waiter = tokio::runtime::Runtime::new().unwrap();
    waiter.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), client.shutdown())
            .await
            .unwrap();
        http.wait_dropped().await;
    });
}

#[tokio::test]
async fn shutdown_joins_both_rack_and_cross_rack_topology_discovery() {
    let http = DiscoveryHttp::new(true, false);
    let client = AlternatorClient::from_conf(
        http.config()
            .to_builder()
            .routing_scope(alternator_driver::RoutingScope::from_rack(
                "dc1".into(),
                "rack1".into(),
            ))
            .build(),
    );
    http.wait_entered().await;
    http.wait_entered().await;
    tokio::time::timeout(Duration::from_secs(1), client.shutdown())
        .await
        .unwrap();
    http.wait_dropped().await;
    http.wait_dropped().await;
    assert_eq!(http.scans.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn rack_affinity_uses_cached_topology_after_shutdown() {
    let http = DiscoveryHttp::new(false, false);
    let client = AlternatorClient::from_conf(
        http.config()
            .to_builder()
            .routing_scope(alternator_driver::RoutingScope::from_rack(
                "dc1".into(),
                "rack1".into(),
            ))
            .key_route_affinity(
                alternator_driver::KeyRouteAffinityConfig::builder()
                    .with_type(KeyRouteAffinityType::AnyWrite)
                    .with_pk_info("users", "pk")
                    .build(),
            )
            .build(),
    );
    put_item(&client, "users").await;
    client.shutdown().await;
    let scans = http.scans.load(Ordering::SeqCst);
    assert!(
        scans >= 4,
        "both topology views must finish before affinity routing"
    );
    put_item(&client, "users").await;
    assert_eq!(http.scans.load(Ordering::SeqCst), scans);
    assert_eq!(http.describes.load(Ordering::SeqCst), 0);
}

#[test]
fn shutdown_before_first_runtime_prevents_lazy_discovery() {
    let http = DiscoveryHttp::new(true, true);
    let client = http.client();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        client.shutdown().await;
        put_item(&client, "users").await;
        client.shutdown().await;
    });
    assert_eq!(http.scans.load(Ordering::SeqCst), 0);
    assert_eq!(http.describes.load(Ordering::SeqCst), 0);
}

#[test]
fn shutdown_does_not_wait_for_an_unrelated_saturated_blocking_pool() {
    struct ReleaseOnDrop(Option<std::sync::mpsc::Sender<()>>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }
    let owner = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let observer = tokio::runtime::Runtime::new().unwrap();
    let (started, started_rx) = std::sync::mpsc::sync_channel(0);
    let (release, release_rx) = std::sync::mpsc::channel();
    // Declared after the owner so failed assertions release its blocker before
    // runtime destruction. The successful path releases it only AFTER shutdown.
    let release = ReleaseOnDrop(Some(release));
    let blocker = owner.spawn_blocking(move || {
        started.send(()).unwrap();
        let _ = release_rx.recv();
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let http = DiscoveryHttp::new(true, false);
    let client = owner.block_on(async {
        let client = http.client();
        http.wait_entered().await;
        client
    });
    observer.block_on(async {
        // Routing a request from another runtime requests an owner liveness
        // acknowledgement while its blocking pool has no available worker.
        put_item(&client, "users").await;
        tokio::time::timeout(Duration::from_secs(1), client.shutdown())
            .await
            .expect("discovery shutdown must finish before releasing unrelated work");
        http.wait_dropped().await;
        assert!(!blocker.is_finished());
    });
    drop(release);
    observer.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), blocker)
            .await
            .unwrap()
            .unwrap();
    });
}

#[tokio::test(start_paused = true)]
async fn partition_key_discovery_uses_explicit_short_and_long_deadlines() {
    for discovery_timeout in [Duration::from_millis(250), Duration::from_secs(10)] {
        let http = DiscoveryHttp::new(false, true);
        let client = AlternatorClient::from_conf(
            http.config()
                .to_builder()
                .timeout_config(
                    aws_smithy_types::timeout::TimeoutConfig::builder()
                        .operation_timeout(Duration::from_secs(1))
                        .operation_attempt_timeout(Duration::from_secs(1))
                        .build(),
                )
                .discovery_timeout(discovery_timeout)
                .build(),
        );
        put_item(&client, "users").await;
        http.wait_entered().await;
        let before_deadline = if discovery_timeout > Duration::from_secs(5) {
            Duration::from_secs(6)
        } else {
            discovery_timeout / 2
        };
        tokio::time::advance(before_deadline).await;
        assert_eq!(
            http.dropped.available_permits(),
            0,
            "explicit discovery deadline must replace inherited data deadline"
        );
        assert_eq!(http.describes.load(Ordering::SeqCst), 1);
        tokio::time::advance(discovery_timeout - before_deadline).await;
        // Remain runnable so paused time cannot advance through retry backoff.
        for _ in 0..1000 {
            if http.dropped.available_permits() > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(http.dropped.available_permits(), 1);
        assert_eq!(http.describes.load(Ordering::SeqCst), 1);
        client.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn unset_discovery_timeout_keeps_partition_key_sdk_deadline() {
    let http = DiscoveryHttp::new(false, true);
    let client = AlternatorClient::from_conf(
        http.config()
            .to_builder()
            .timeout_config(
                aws_smithy_types::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(8))
                    .build(),
            )
            .build(),
    );
    assert_eq!(client.config().discovery_timeout(), None);
    put_item(&client, "users").await;
    http.wait_entered().await;
    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(
        http.dropped.available_permits(),
        0,
        "legacy PK timeout must not gain topology's default5s cap"
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..1000 {
        if http.dropped.available_permits() > 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(http.dropped.available_permits(), 1);
    client.shutdown().await;
}

#[test]
fn invalid_discovery_timeouts_fail_before_network() {
    let http = DiscoveryHttp::new(true, true);
    for timeout in [Duration::ZERO, Duration::MAX] {
        let error = AlternatorClient::try_from_conf(
            http.config()
                .to_builder()
                .discovery_timeout(timeout)
                .build(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("discovery_timeout"));
    }
    assert_eq!(http.scans.load(Ordering::SeqCst), 0);
    assert_eq!(http.describes.load(Ordering::SeqCst), 0);
    assert_eq!(http.entered.available_permits(), 0);
}
