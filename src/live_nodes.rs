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

//! Maintains and updates a list of known live Alternator nodes using the `/localnodes` endpoint.
//!
//! # Overview
//!
//! [`LiveNodes`] is constructed from an [`AlternatorConfig`] and seeded with a list of hosts.
//! Once [`start`] is called, a background Tokio task
//! periodically calls the [`update_live_nodes`] function which requests the known
//! nodes in a random order to get an updated list of live nodes. After a
//! successful refresh, the list is updated to nodes from the highest available
//! scope in the fallback chain provided by the user.
//! Underneath it uses a basic [`reqwest::Client`] with timeouts.
//!
//! # Polling cadence
//!
//! The refresh loop has two cadences:
//!
//! - **Active** ([`active_interval`]): used while the client is being called
//!   regularly. Polls run frequently to keep the view fresh under load.
//! - **Idle** ([`idle_interval`]): used when no caller has touched
//!   [`LiveNodes`] recently. An incoming request wakes the loop early via a [`Notify`].
//!
//! Activity is tracked through [`mark_activity`], which every read path calls.
//!
//! # Discovery mechanism
//!
//! Each scoped refresh starts from the highest scope in the fallback chain,
//! shuffles the current node list, and then tries the original seed endpoints.
//! A hostname is freshly resolved, and each unique resolved address is tried in
//! order while the request URL retains the logical hostname. Transport errors,
//! non-success responses, and malformed or unusable data advance to the next
//! address and candidate. A configured fallback scope is considered only after
//! all candidates return an empty list for the current scope.
//!
//! For cluster-wide scope, the refresh queries `/localnodes` from configured
//! seed nodes and already-known live nodes, then unions the responses. To cover
//! all datacenters, the initial configuration must include at least one working
//! seed host from every datacenter that should receive traffic.
//!
//! A non-empty successful result atomically replaces [`live_nodes`] using
//! [`ArcSwap`]. A fully failed refresh preserves the previous snapshot and the
//! original seeds. A conclusive empty scoped result removes seeds from routing
//! while retaining them as future discovery candidates.
//!
//!  # Lifetime
//!
//! The background task holds a [`Weak`] reference to its [`LiveNodes`], so it
//! terminates on its own once the last external [`Arc`] is dropped. [`Drop`]
//! additionally aborts the task to avoid waiting out the current sleep.
//!
//! # Start-up
//!
//! The task is launched via [`tokio::spawn`], which requires an active Tokio runtime on the calling thread or else it panics.
//! The client's [`from_conf`] constructor, however, is synchronous and can be called from anywhere.
//! It is handled by funneling start-up through a single idempotent entry point:
//! [`ensure_discovery_started`]. It does three things, in order:
//!
//! 1. If discovery is already running, return immediately (an atomic load, essentially free).
//! 2. Runtime check: if no Tokio runtime is available on the current thread, return without spawning.
//!    The task will be started lazily on the first [`get_next_node_round_robin`] or [`get_live_nodes`] call,
//!    which is typically invoked from within the request pipeline and therefore from within a runtime.
//! 3. A `compare_exchange` on `discovery_started` ensures that
//!    exactly one caller wins the right to spawn the task, even under
//!    concurrent first-access from multiple threads.
//!
//! [`AlternatorConfig`]: crate::config::AlternatorConfig
//! [`RoutingScope`]: crate::routing_scope::RoutingScope
//! [`ArcSwap`]: arc_swap::ArcSwap
//! [`Notify`]: tokio::sync::Notify
//! [`Weak`]: std::sync::Weak
//! [`Arc`]: std::sync::Arc
//! [`active_interval`]: LiveNodes::active_interval
//! [`idle_interval`]: LiveNodes::idle_interval
//! [`mark_activity`]: LiveNodes::mark_activity
//! [`ensure_discovery_started`]: LiveNodes::ensure_discovery_started
//! [`start`]: LiveNodes::start
//! [`update_live_nodes`]: LiveNodes::update_live_nodes
//! [`get_next_node_round_robin`]: LiveNodes::get_next_node_round_robin
//! [`get_live_nodes`]: LiveNodes::get_live_nodes
//! [`live_nodes`]: LiveNodes::live_nodes
//! [`from_conf`]: crate::client::AlternatorClient::from_conf

use crate::routing_scope::RoutingScope;
use arc_swap::ArcSwap;
use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use url::{Host, Url};

const DEFAULT_ACTIVE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_IDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const DNS_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);
const DISCOVERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DISCOVERY_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_RESOLVED_ADDRESSES: usize = 32;
const MAX_CACHED_DISCOVERY_CLIENTS: usize = 64;
const MAX_DISCOVERY_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_IN_FLIGHT_DNS_LOOKUPS: usize = 1;

type ResolveFuture<'a> =
    Pin<Box<dyn Future<Output = std::io::Result<Vec<SocketAddr>>> + Send + 'a>>;

trait DiscoveryResolver: std::fmt::Debug + Send + Sync {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a>;
}

#[derive(Debug)]
struct SystemDiscoveryResolver;

impl DiscoveryResolver for SystemDiscoveryResolver {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
        Box::pin(async move { Ok(tokio::net::lookup_host((host, port)).await?.collect()) })
    }
}

/// Keeps a timed-out OS resolver task alive behind its permit so later refreshes
/// wait instead of creating an unbounded number of blocking DNS lookups.
#[derive(Debug)]
struct BoundedDiscoveryResolver {
    inner: Arc<dyn DiscoveryResolver>,
    lookup_slots: Arc<tokio::sync::Semaphore>,
}

impl BoundedDiscoveryResolver {
    fn new(inner: Arc<dyn DiscoveryResolver>) -> Self {
        Self {
            inner,
            lookup_slots: Arc::new(tokio::sync::Semaphore::new(MAX_IN_FLIGHT_DNS_LOOKUPS)),
        }
    }
}

impl DiscoveryResolver for BoundedDiscoveryResolver {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
        let host = host.to_string();
        let inner = self.inner.clone();
        let lookup_slots = self.lookup_slots.clone();
        Box::pin(async move {
            let permit = lookup_slots
                .acquire_owned()
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            let lookup = tokio::spawn(async move {
                let _permit = permit;
                inner.resolve(&host, port).await
            });
            lookup
                .await
                .map_err(|error| std::io::Error::other(error.to_string()))?
        })
    }
}

#[derive(Debug, Default)]
struct DiscoveryClientCache {
    clients: HashMap<(String, SocketAddr), reqwest::Client>,
    insertion_order: VecDeque<(String, SocketAddr)>,
    #[cfg(test)]
    additional_root_certificate: Option<reqwest::Certificate>,
}

impl DiscoveryClientCache {
    fn get_or_insert(
        &mut self,
        logical_host: &str,
        address: SocketAddr,
        is_domain: bool,
    ) -> Option<reqwest::Client> {
        let key = (logical_host.to_string(), address);
        if let Some(client) = self.clients.get(&key) {
            return Some(client.clone());
        }

        let mut builder = reqwest::Client::builder()
            .timeout(DISCOVERY_REQUEST_TIMEOUT)
            .connect_timeout(DISCOVERY_CONNECT_TIMEOUT);
        #[cfg(test)]
        if let Some(certificate) = &self.additional_root_certificate {
            builder = builder.add_root_certificate(certificate.clone());
        }
        if is_domain {
            // Keep the configured hostname in the URL and override only the
            // socket destination. This preserves HTTP Host and TLS server name.
            builder = builder.resolve(logical_host, address);
        }
        let client = builder.build().ok()?;

        while self.clients.len() >= MAX_CACHED_DISCOVERY_CLIENTS {
            let Some(oldest) = self.insertion_order.pop_front() else {
                break;
            };
            self.clients.remove(&oldest);
        }
        self.insertion_order.push_back(key.clone());
        self.clients.insert(key, client.clone());
        Some(client)
    }
}

#[derive(Debug)]
pub struct LiveNodes {
    routing_scope: RoutingScope,
    active_interval: Duration,
    idle_interval: Duration,
    counter: Arc<AtomicUsize>,
    live_nodes: ArcSwap<Vec<Arc<Url>>>,
    seed_urls: Vec<Arc<Url>>,
    alternator_scheme: String,
    port: Option<u16>,
    resolver: Arc<dyn DiscoveryResolver>,
    discovery_clients: Mutex<DiscoveryClientCache>,
    update_lock: tokio::sync::Mutex<()>,
    last_activity: Arc<Mutex<Instant>>,
    notify: Arc<tokio::sync::Notify>,
    bg_task: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
    discovery_started: AtomicBool,
}

impl LiveNodes {
    pub fn new(config: &crate::config::AlternatorConfig) -> Option<Arc<Self>> {
        let active_interval = config
            .active_interval()
            .unwrap_or(DEFAULT_ACTIVE_REFRESH_INTERVAL);
        let idle_interval = config
            .idle_interval()
            .unwrap_or(DEFAULT_IDLE_REFRESH_INTERVAL);
        let routing_scope = config
            .routing_scope()
            .unwrap_or(RoutingScope::from_cluster());
        let alternator_scheme = config.scheme().unwrap_or("http".to_string());
        let port = config.port();
        let seed_nodes = config.seed_hosts().unwrap_or_default();

        let mut seed_urls = seed_nodes
            .iter()
            .filter_map(|addr| {
                build_node_url(&alternator_scheme, addr, port)
                    .ok()
                    .map(Arc::new)
            })
            .collect::<Vec<_>>();
        seed_urls.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        if seed_urls.is_empty() {
            return None;
        }

        Some(Arc::new(Self {
            routing_scope,
            active_interval,
            idle_interval,
            counter: Arc::new(AtomicUsize::new(0)),
            live_nodes: ArcSwap::from_pointee(seed_urls.clone()),
            seed_urls,
            alternator_scheme,
            port,
            resolver: Arc::new(BoundedDiscoveryResolver::new(Arc::new(
                SystemDiscoveryResolver,
            ))),
            discovery_clients: Mutex::new(DiscoveryClientCache::default()),
            update_lock: tokio::sync::Mutex::new(()),
            last_activity: Arc::new(Mutex::new(Instant::now())),
            notify: Arc::new(tokio::sync::Notify::new()),
            bg_task: std::sync::Mutex::new(None),
            discovery_started: AtomicBool::new(false),
        }))
    }

    fn host_to_uri(&self, addr: &str) -> Result<Url, url::ParseError> {
        build_node_url(&self.alternator_scheme, addr, self.port)
    }

    async fn resolve_node_addresses(
        &self,
        node_addr: &Url,
    ) -> Option<(String, bool, Vec<SocketAddr>)> {
        let port = node_addr.port_or_known_default()?;
        let (logical_host, is_domain, addresses) = match node_addr.host()? {
            Host::Domain(host) => {
                let addresses =
                    tokio::time::timeout(DNS_LOOKUP_TIMEOUT, self.resolver.resolve(host, port))
                        .await
                        .ok()?
                        .ok()?;
                (host.to_string(), true, addresses)
            }
            Host::Ipv4(ip) => (
                ip.to_string(),
                false,
                vec![SocketAddr::new(IpAddr::V4(ip), port)],
            ),
            Host::Ipv6(ip) => (
                ip.to_string(),
                false,
                vec![SocketAddr::new(IpAddr::V6(ip), port)],
            ),
        };

        let mut seen = HashSet::new();
        let addresses = addresses
            .into_iter()
            .map(|address| SocketAddr::new(address.ip(), port))
            .filter(|address| seen.insert(address.ip()))
            .take(MAX_RESOLVED_ADDRESSES)
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return None;
        }

        Some((logical_host, is_domain, addresses))
    }

    fn discovery_client(
        &self,
        logical_host: &str,
        address: SocketAddr,
        is_domain: bool,
    ) -> Option<reqwest::Client> {
        self.discovery_clients
            .lock()
            .ok()?
            .get_or_insert(logical_host, address, is_domain)
    }

    async fn read_bounded_response(mut response: reqwest::Response) -> Option<Vec<u8>> {
        if response
            .content_length()
            .is_some_and(|length| length > MAX_DISCOVERY_RESPONSE_BYTES as u64)
        {
            return None;
        }

        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if body.len().saturating_add(chunk.len()) > MAX_DISCOVERY_RESPONSE_BYTES {
                return None;
            }
            body.extend_from_slice(&chunk);
        }
        Some(body)
    }

    async fn fetch_live_nodes_for_scope(
        &self,
        scope: &RoutingScope,
        node_addr: &Url,
    ) -> Option<Vec<Arc<Url>>> {
        let url = scope.build_localnodes_url(node_addr.clone());
        let (logical_host, is_domain, addresses) = self.resolve_node_addresses(node_addr).await?;
        let mut saw_empty_response = false;

        for address in addresses {
            let Some(client) = self.discovery_client(&logical_host, address, is_domain) else {
                continue;
            };
            let Ok(response) = client.get(url.clone()).send().await else {
                continue;
            };
            if !response.status().is_success() {
                // Drain normal error responses so a cached HTTP/1 connection can
                // remain reusable on a later discovery cycle.
                let _ = Self::read_bounded_response(response).await;
                continue;
            }
            let Some(body) = Self::read_bounded_response(response).await else {
                continue;
            };
            let Ok(nodes) = serde_json::from_slice::<Vec<String>>(&body) else {
                continue;
            };
            if nodes.is_empty() {
                saw_empty_response = true;
                continue;
            }

            let mut valid_nodes = nodes
                .into_iter()
                .filter_map(|addr| self.host_to_uri(&addr).ok().map(Arc::new))
                .collect::<Vec<_>>();
            valid_nodes.sort_by(|left, right| left.as_str().cmp(right.as_str()));
            valid_nodes.dedup_by(|left, right| left.as_str() == right.as_str());
            if !valid_nodes.is_empty() {
                return Some(valid_nodes);
            }
        }

        saw_empty_response.then(Vec::new)
    }

    fn cluster_discovery_candidates(&self) -> Vec<Arc<Url>> {
        let mut candidates = self.live_nodes.load().as_ref().clone();
        candidates.extend(self.seed_urls.iter().cloned());
        candidates.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        candidates.dedup_by(|a, b| a.as_str() == b.as_str());
        candidates.shuffle(&mut rand::rng());
        candidates
    }

    fn scoped_discovery_candidates(&self) -> Vec<Arc<Url>> {
        let mut candidates = self.live_nodes.load().as_ref().clone();
        candidates.shuffle(&mut rand::rng());

        let mut seen = candidates
            .iter()
            .map(|candidate| candidate.as_str().to_string())
            .collect::<HashSet<_>>();
        for seed in &self.seed_urls {
            if seen.insert(seed.as_str().to_string()) {
                candidates.push(seed.clone());
            }
        }
        candidates
    }

    async fn discover_cluster_live_nodes(&self) -> Option<Vec<Arc<Url>>> {
        let scope = RoutingScope::from_cluster();
        let mut new_nodes = Vec::new();
        let mut got_response = false;

        for node_addr in self.cluster_discovery_candidates() {
            if node_is_in_list(&node_addr, &new_nodes) {
                continue;
            }

            if let Some(mut nodes) = self.fetch_live_nodes_for_scope(&scope, &node_addr).await {
                got_response = true;
                new_nodes.append(&mut nodes);
                new_nodes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
                new_nodes.dedup_by(|a, b| a.as_str() == b.as_str());
            }
        }

        if !got_response {
            return None;
        }

        new_nodes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        new_nodes.dedup_by(|a, b| a.as_str() == b.as_str());
        Some(new_nodes)
    }

    async fn discover_scoped_live_nodes(&self, scope: &RoutingScope) -> Option<Vec<Arc<Url>>> {
        let mut saw_empty_response = false;

        for node_addr in self.scoped_discovery_candidates() {
            match self.fetch_live_nodes_for_scope(scope, &node_addr).await {
                Some(nodes) if !nodes.is_empty() => return Some(nodes),
                Some(_) => saw_empty_response = true,
                None => {}
            }
        }

        saw_empty_response.then(Vec::new)
    }

    /// Ensures the background discovery task is running.
    ///
    /// Idempotent and safe to call from any context: returns immediately if
    /// discovery is already started, or if no Tokio runtime is available.
    pub fn ensure_discovery_started(self: &Arc<Self>) {
        if self.discovery_started.load(Ordering::Acquire) {
            return;
        }

        if Handle::try_current().is_err() {
            return;
        }

        if self
            .discovery_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Arc::clone(self).start();
        }
    }

    fn start(self: Arc<Self>) {
        let weak_self = Arc::downgrade(&self);
        let notify = self.notify.clone();

        self.mark_activity();
        let handle = tokio::spawn(async move {
            loop {
                let (idle_interval, active_interval, is_idle) = {
                    let Some(strong_self) = weak_self.upgrade() else {
                        break;
                    };

                    strong_self.update_live_nodes().await;

                    let last = *strong_self.last_activity.lock().unwrap();
                    (
                        strong_self.idle_interval,
                        strong_self.active_interval,
                        last.elapsed() >= strong_self.idle_interval,
                    )
                };

                if !is_idle {
                    tokio::time::sleep(active_interval).await;
                } else {
                    tokio::select! {
                        _ = tokio::time::sleep(idle_interval) => {}
                        _ = notify.notified() => {}
                    }
                }
            }
        });

        if let Ok(mut guard) = self.bg_task.lock() {
            *guard = Some(handle.abort_handle());
        }
    }

    fn mark_activity(&self) {
        let now = Instant::now();
        let mut last = self.last_activity.lock().unwrap();
        let was_idle = now.duration_since(*last) > self.idle_interval;
        *last = now;
        if was_idle {
            self.notify.notify_one();
        }
    }

    /// Returns a list of all current live nodes and updates the last activity timestamp.
    pub fn get_live_nodes(self: &Arc<Self>) -> Vec<Arc<Url>> {
        self.ensure_discovery_started();
        self.mark_activity();
        self.live_nodes.load().as_ref().clone()
    }

    /// Returns the first live node not in `used_nodes` starting with the next node in round-robin order.
    /// Used by [`crate::QueryPlan`] round-robin strategy.
    pub fn get_next_node_round_robin(
        self: &Arc<Self>,
        used_nodes: &std::collections::HashSet<Arc<Url>>,
    ) -> Option<Arc<Url>> {
        self.ensure_discovery_started();
        self.mark_activity();
        let live_nodes = self.live_nodes.load();

        let len = live_nodes.len();
        if len == 0 {
            return None;
        }

        let start = self.counter.fetch_add(1, Ordering::Relaxed) % len;
        for i in 0..len {
            let idx = (start + i) % len;
            let node = &live_nodes[idx];
            if !used_nodes.contains(node) {
                return Some(node.clone());
            }
        }
        None
    }

    pub async fn update_live_nodes(&self) {
        let _update_guard = self.update_lock.lock().await;
        let mut scope = &self.routing_scope;

        loop {
            let result = if scope.is_cluster() {
                self.discover_cluster_live_nodes().await
            } else {
                self.discover_scoped_live_nodes(scope).await
            };
            let Some(new_nodes) = result else {
                // DNS, transport, and validation failures are not evidence that
                // the previous learned snapshot is invalid.
                return;
            };

            if !new_nodes.is_empty() {
                if **self.live_nodes.load() != new_nodes {
                    self.live_nodes.store(Arc::new(new_nodes));
                }
                return;
            }

            if let Some(fallback) = scope.fallback() {
                scope = fallback;
                continue;
            }

            // A scoped empty response conclusively means no node matches the
            // requested scope. Keep seeds for discovery, but do not route
            // application requests through an out-of-scope seed.
            if !self.routing_scope.is_cluster() {
                self.live_nodes.store(Arc::new(Vec::new()));
            }
            return;
        }
    }
}

fn build_node_url(scheme: &str, addr: &str, port: Option<u16>) -> Result<Url, url::ParseError> {
    let authority = if addr.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{addr}]")
    } else {
        addr.to_string()
    };
    let mut url = Url::parse(&format!("{scheme}://{authority}"))?;
    url.set_port(port)
        .map_err(|()| url::ParseError::InvalidPort)?;
    Ok(url)
}

fn node_is_in_list(node: &Url, nodes: &[Arc<Url>]) -> bool {
    nodes.iter().any(|known| {
        known.host_str() == node.host_str()
            && known.port_or_known_default() == node.port_or_known_default()
    })
}

impl Drop for LiveNodes {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.bg_task.lock()
            && let Some(task) = guard.take()
        {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AlternatorConfig;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Notify;

    #[derive(Debug)]
    struct StaticResolver {
        addresses: Vec<IpAddr>,
    }

    impl DiscoveryResolver for StaticResolver {
        fn resolve<'a>(&'a self, _host: &'a str, port: u16) -> ResolveFuture<'a> {
            let addresses = self
                .addresses
                .iter()
                .map(|ip| SocketAddr::new(*ip, port))
                .collect();
            Box::pin(async move { Ok(addresses) })
        }
    }

    #[derive(Clone, Debug)]
    enum Resolution {
        Addresses(Vec<IpAddr>),
        Error(std::io::ErrorKind),
        Pending,
        Delayed {
            addresses: Vec<IpAddr>,
            release: Arc<Notify>,
        },
    }

    #[derive(Debug)]
    struct ScriptedResolver {
        answers: Mutex<HashMap<String, VecDeque<Resolution>>>,
        calls: Mutex<Vec<String>>,
    }

    impl ScriptedResolver {
        fn new(entries: Vec<(&str, Vec<Resolution>)>) -> Arc<Self> {
            Arc::new(Self {
                answers: Mutex::new(
                    entries
                        .into_iter()
                        .map(|(host, answers)| (host.to_string(), answers.into()))
                        .collect(),
                ),
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls_for(&self, host: &str) -> usize {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|called| called.as_str() == host)
                .count()
        }
    }

    impl DiscoveryResolver for ScriptedResolver {
        fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
            self.calls.lock().unwrap().push(host.to_string());
            let answer = self
                .answers
                .lock()
                .unwrap()
                .get_mut(host)
                .and_then(VecDeque::pop_front)
                .unwrap_or(Resolution::Error(std::io::ErrorKind::NotFound));

            match answer {
                Resolution::Addresses(addresses) => Box::pin(async move {
                    Ok(addresses
                        .into_iter()
                        .map(|ip| SocketAddr::new(ip, port))
                        .collect())
                }),
                Resolution::Error(kind) => Box::pin(async move { Err(std::io::Error::from(kind)) }),
                Resolution::Pending => Box::pin(std::future::pending()),
                Resolution::Delayed { addresses, release } => Box::pin(async move {
                    release.notified().await;
                    Ok(addresses
                        .into_iter()
                        .map(|ip| SocketAddr::new(ip, port))
                        .collect())
                }),
            }
        }
    }

    #[derive(Debug)]
    enum ServerReply {
        Http {
            status: u16,
            body: String,
        },
        Truncated(String),
        OversizedDeclared,
        OversizedChunked,
        Stall,
        Reset,
        Delayed {
            body: String,
            entered: Arc<Notify>,
            release: Arc<Notify>,
        },
    }

    #[derive(Debug)]
    struct ExpectedReply {
        path_and_query: String,
        expected_host: Option<String>,
        reply: ServerReply,
    }

    impl ExpectedReply {
        fn json(body: impl Into<String>) -> Self {
            Self::for_path(
                "/localnodes",
                ServerReply::Http {
                    status: 200,
                    body: body.into(),
                },
            )
        }

        fn for_path(path_and_query: impl Into<String>, reply: ServerReply) -> Self {
            Self {
                path_and_query: path_and_query.into(),
                expected_host: None,
                reply,
            }
        }

        fn for_host(mut self, expected_host: impl Into<String>) -> Self {
            self.expected_host = Some(expected_host.into());
            self
        }
    }

    async fn start_address_servers(
        logical_host: &str,
        specs: Vec<(Ipv4Addr, Vec<ExpectedReply>)>,
    ) -> (u16, Vec<tokio::task::JoinHandle<()>>) {
        let mut specs = specs.into_iter();
        let (first_ip, first_replies) = specs.next().expect("at least one server");
        let first_listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(first_ip), 0))
            .await
            .unwrap();
        let port = first_listener.local_addr().unwrap().port();
        let mut listeners = vec![(first_listener, first_replies)];
        for (ip, replies) in specs {
            let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(ip), port))
                .await
                .unwrap();
            listeners.push((listener, replies));
        }

        let handles = listeners
            .into_iter()
            .map(|(listener, replies)| {
                let logical_host = logical_host.to_string();
                tokio::spawn(async move {
                    for expected in replies {
                        let (mut stream, _) = listener.accept().await.unwrap();
                        let mut request = Vec::new();
                        loop {
                            let mut chunk = [0; 1024];
                            let count = stream.read(&mut chunk).await.unwrap();
                            if count == 0 {
                                break;
                            }
                            request.extend_from_slice(&chunk[..count]);
                            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                                break;
                            }
                            assert!(request.len() < 16 * 1024, "request headers too large");
                        }
                        let request = String::from_utf8(request).unwrap();
                        assert!(request.starts_with(&format!(
                            "GET {} HTTP/1.1",
                            expected.path_and_query
                        )));
                        let expected_host = expected
                            .expected_host
                            .as_deref()
                            .unwrap_or(&logical_host);
                        assert!(request.to_ascii_lowercase().contains(&format!(
                            "\r\nhost: {expected_host}:{port}\r\n"
                        )));

                        match expected.reply {
                            ServerReply::Http { status, body } => {
                                let response = format!(
                                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                    body.len()
                                );
                                stream.write_all(response.as_bytes()).await.unwrap();
                            }
                            ServerReply::Truncated(body) => {
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                    body.len() + 16
                                );
                                stream.write_all(response.as_bytes()).await.unwrap();
                            }
                            ServerReply::OversizedDeclared => {
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    MAX_DISCOVERY_RESPONSE_BYTES + 1
                                );
                                stream.write_all(response.as_bytes()).await.unwrap();
                            }
                            ServerReply::OversizedChunked => {
                                let body = vec![b'x'; MAX_DISCOVERY_RESPONSE_BYTES + 1];
                                let header = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n",
                                    body.len()
                                );
                                if stream.write_all(header.as_bytes()).await.is_ok() {
                                    let _ = stream.write_all(&body).await;
                                }
                            }
                            ServerReply::Stall => {
                                tokio::time::sleep(
                                    DISCOVERY_REQUEST_TIMEOUT + Duration::from_millis(100),
                                )
                                .await;
                            }
                            ServerReply::Reset => {}
                            ServerReply::Delayed {
                                body,
                                entered,
                                release,
                            } => {
                                entered.notify_one();
                                release.notified().await;
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                    body.len()
                                );
                                stream.write_all(response.as_bytes()).await.unwrap();
                            }
                        }
                    }
                })
            })
            .collect();

        (port, handles)
    }

    async fn join_servers(handles: Vec<tokio::task::JoinHandle<()>>) {
        for handle in handles {
            tokio::time::timeout(Duration::from_secs(2), handle)
                .await
                .expect("server did not receive its expected requests")
                .unwrap();
        }
    }

    fn live_nodes_with_resolver(
        config: &AlternatorConfig,
        resolver: Arc<dyn DiscoveryResolver>,
    ) -> Arc<LiveNodes> {
        let mut nodes = LiveNodes::new(config).unwrap();
        Arc::get_mut(&mut nodes).unwrap().resolver = resolver;
        nodes
    }

    fn test_config() -> AlternatorConfig {
        AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://127.0.0.1:1".to_string())
            .build()
    }

    async fn start_localnodes_server(body: &'static str) -> (u16, tokio::task::JoinHandle<()>) {
        start_localnodes_server_on("127.0.0.1:0", "localhost", body).await
    }

    async fn start_localnodes_server_on(
        bind_address: &str,
        expected_host: &str,
        body: &'static str,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind(bind_address).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let expected_host = expected_host.to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 1024];
            let n = stream.read(&mut buffer).await.unwrap();
            let request = String::from_utf8_lossy(&buffer[..n]);
            assert!(request.starts_with("GET /localnodes HTTP/1.1"));
            assert!(
                request.contains(&format!("host: {expected_host}:{port}"))
                    || request.contains(&format!("Host: {expected_host}:{port}"))
            );

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });

        (port, server)
    }

    #[test]
    fn start_without_runtime_does_not_panic() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!nodes.discovery_started.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn start_with_runtime_starts_correctly() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(nodes.discovery_started.load(Ordering::Acquire));
    }

    #[test]
    fn start_on_first_access_round_robin() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!nodes.discovery_started.load(Ordering::Acquire));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _ = nodes.get_next_node_round_robin(&std::collections::HashSet::new());
        });
        assert!(nodes.discovery_started.load(Ordering::Acquire));
    }

    #[test]
    fn start_on_first_access() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!nodes.discovery_started.load(Ordering::Acquire));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _ = nodes.get_live_nodes();
        });
        assert!(nodes.discovery_started.load(Ordering::Acquire));
    }

    #[test]
    fn ipv6_address_parsing() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://[::1]:8000".to_string())
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        assert_eq!(nodes.seed_urls[0].scheme(), "http");
        assert_eq!(nodes.seed_urls[0].host_str(), Some("[::1]"));
        assert_eq!(nodes.seed_urls[0].port(), Some(8000));
        assert_eq!(nodes.seed_urls[0].to_string(), "http://[::1]:8000/");
    }

    #[test]
    fn raw_ipv6_seed_host_is_bracketed() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(["::1"])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        assert_eq!(nodes.seed_urls[0].host_str(), Some("[::1]"));
        assert_eq!(nodes.seed_urls[0].to_string(), "http://[::1]:8000/");
    }

    #[tokio::test]
    async fn raw_ipv6_seed_discovers_raw_ipv6_node() {
        let (port, server) = start_localnodes_server_on("[::1]:0", "[::1]", r#"["::1"]"#).await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["::1"])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        nodes.update_live_nodes().await;

        server.await.unwrap();
        assert_eq!(
            nodes.live_nodes.load()[0].as_str(),
            format!("http://[::1]:{port}/")
        );
    }

    #[tokio::test]
    async fn dns_entrypoint_discovers_dns_node_records() {
        let (port, server) = start_localnodes_server(r#"["localhost","node-a.internal"]"#).await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(vec!["localhost".to_string()])
            .active_interval(std::time::Duration::from_millis(10))
            .idle_interval(std::time::Duration::from_secs(10))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        nodes.update_live_nodes().await;

        server.await.unwrap();
        let snapshot = nodes.live_nodes.load();
        let hosts = snapshot
            .iter()
            .map(|url| url.host_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(hosts, vec!["localhost", "node-a.internal"]);
    }

    #[tokio::test]
    async fn dns_entrypoint_applies_configured_port_to_dns_node_records() {
        let (port, server) =
            start_localnodes_server(r#"["node-a.internal:9000","node-b.internal"]"#).await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(vec!["localhost".to_string()])
            .active_interval(std::time::Duration::from_millis(10))
            .idle_interval(std::time::Duration::from_secs(10))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        nodes.update_live_nodes().await;

        server.await.unwrap();
        let snapshot = nodes.live_nodes.load();
        let hosts_and_ports = snapshot
            .iter()
            .map(|url| (url.host_str().unwrap().to_string(), url.port()))
            .collect::<Vec<_>>();
        assert_eq!(
            hosts_and_ports,
            vec![
                ("node-a.internal".to_string(), Some(port)),
                ("node-b.internal".to_string(), Some(port)),
            ]
        );
    }

    #[tokio::test]
    async fn dns_entrypoint_supports_single_family_and_cross_family_fallback() {
        assert_dns_discovery("127.0.0.1:0", &[IpAddr::V4(Ipv4Addr::LOCALHOST)]).await;
        assert_dns_discovery("[::1]:0", &[IpAddr::V6(Ipv6Addr::LOCALHOST)]).await;
        assert_dns_discovery(
            "127.0.0.1:0",
            &[
                IpAddr::V6(Ipv6Addr::LOCALHOST),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
            ],
        )
        .await;
        assert_dns_discovery(
            "[::1]:0",
            &[
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                IpAddr::V6(Ipv6Addr::LOCALHOST),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn dns_address_fallback_rejects_unusable_localnodes_responses() {
        let cases = vec![
            (
                "non-success",
                ServerReply::Http {
                    status: 503,
                    body: r#"{"message":"busy"}"#.to_string(),
                },
            ),
            (
                "malformed-json",
                ServerReply::Http {
                    status: 200,
                    body: "{".to_string(),
                },
            ),
            (
                "empty-list",
                ServerReply::Http {
                    status: 200,
                    body: "[]".to_string(),
                },
            ),
            (
                "unusable-list",
                ServerReply::Http {
                    status: 200,
                    body: r#"["[not-an-ip]",":"]"#.to_string(),
                },
            ),
            (
                "truncated-body",
                ServerReply::Truncated(r#"["stale.internal"]"#.to_string()),
            ),
            ("oversized-declared-body", ServerReply::OversizedDeclared),
            ("oversized-chunked-body", ServerReply::OversizedChunked),
            ("transport-close", ServerReply::Reset),
        ];

        for (case, bad_reply) in cases {
            let bad_ip = Ipv4Addr::new(127, 0, 0, 21);
            let good_ip = Ipv4Addr::new(127, 0, 0, 22);
            let (port, servers) = start_address_servers(
                "entry.test",
                vec![
                    (
                        bad_ip,
                        vec![ExpectedReply::for_path("/localnodes", bad_reply)],
                    ),
                    (
                        good_ip,
                        vec![ExpectedReply::json(r#"["learned.internal"]"#)],
                    ),
                ],
            )
            .await;
            let resolver = ScriptedResolver::new(vec![(
                "entry.test",
                vec![Resolution::Addresses(vec![
                    IpAddr::V4(bad_ip),
                    IpAddr::V4(good_ip),
                ])],
            )]);
            let config = AlternatorConfig::builder()
                .behavior_version_latest()
                .scheme("http")
                .port(port)
                .seed_hosts(["entry.test"])
                .build();
            let nodes = live_nodes_with_resolver(&config, resolver);

            nodes.update_live_nodes().await;
            join_servers(servers).await;

            assert_eq!(
                nodes.live_nodes.load()[0].host_str(),
                Some("learned.internal"),
                "case {case} did not use the later valid address"
            );
        }
    }

    #[tokio::test]
    async fn several_leading_and_duplicate_dns_addresses_receive_bounded_attempts() {
        let first_bad = Ipv4Addr::new(127, 0, 0, 31);
        let second_bad = Ipv4Addr::new(127, 0, 0, 32);
        let good = Ipv4Addr::new(127, 0, 0, 33);
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![
                (
                    first_bad,
                    vec![ExpectedReply::for_path(
                        "/localnodes",
                        ServerReply::Http {
                            status: 500,
                            body: "temporary".to_string(),
                        },
                    )],
                ),
                (second_bad, vec![ExpectedReply::json("not-json")]),
                (good, vec![ExpectedReply::json(r#"["learned.internal"]"#)]),
            ],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![Resolution::Addresses(vec![
                IpAddr::V4(first_bad),
                IpAddr::V4(first_bad),
                IpAddr::V4(second_bad),
                IpAddr::V4(good),
                IpAddr::V4(good),
            ])],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver.clone());

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("learned.internal")
        );
        assert_eq!(resolver.calls_for("entry.test"), 1);
    }

    #[tokio::test]
    async fn mixed_localnodes_data_keeps_valid_unique_entries() {
        let address = Ipv4Addr::new(127, 0, 0, 34);
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![(
                address,
                vec![ExpectedReply::json(
                    r#"["[not-an-ip]","learned.internal",":","learned.internal"]"#,
                )],
            )],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![Resolution::Addresses(vec![IpAddr::V4(address)])],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        let snapshot = nodes.live_nodes.load();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].host_str(), Some("learned.internal"));
    }

    #[tokio::test]
    async fn address_fallback_preserves_https_server_name_and_host_header() {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(vec!["Test CA".to_string()]).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let server_key = rcgen::KeyPair::generate().unwrap();
        let server_params = rcgen::CertificateParams::new(vec!["entry.test".to_string()]).unwrap();
        let server_cert = server_params
            .signed_by(&server_key, &ca_cert, &ca_key)
            .unwrap();
        let tls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![rustls::pki_types::CertificateDer::from(
                    server_cert.der().to_vec(),
                )],
                rustls::pki_types::PrivateKeyDer::try_from(server_key.serialize_der()).unwrap(),
            )
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls_config));
        let address = Ipv4Addr::new(127, 0, 0, 36);
        let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(address), 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut request = [0; 2048];
            let count = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..count]).to_ascii_lowercase();
            assert!(request.starts_with("get /localnodes http/1.1"));
            assert!(request.contains(&format!("\r\nhost: entry.test:{port}\r\n")));
            let body = r#"["learned.internal"]"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![Resolution::Addresses(vec![IpAddr::V4(address)])],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("https")
            .port(port)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);
        nodes
            .discovery_clients
            .lock()
            .unwrap()
            .additional_root_certificate =
            Some(reqwest::Certificate::from_pem(ca_cert.pem().as_bytes()).unwrap());

        nodes.update_live_nodes().await;
        server.await.unwrap();

        let snapshot = nodes.live_nodes.load();
        assert_eq!(snapshot[0].scheme(), "https");
        assert_eq!(snapshot[0].host_str(), Some("learned.internal"));
        assert_eq!(snapshot[0].port(), Some(port));
    }

    #[tokio::test]
    async fn wholly_invalid_scoped_data_preserves_the_previous_snapshot() {
        let address = Ipv4Addr::new(127, 0, 0, 35);
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![(
                address,
                vec![ExpectedReply::for_path(
                    "/localnodes?dc=dc1",
                    ServerReply::Http {
                        status: 200,
                        body: r#"["[not-an-ip]",":"]"#.to_string(),
                    },
                )],
            )],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![Resolution::Addresses(vec![IpAddr::V4(address)])],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .routing_scope(RoutingScope::from_datacenter("dc1".to_string()))
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("entry.test"));
    }

    #[tokio::test]
    async fn dns_errors_and_empty_answers_recover_on_later_resolution() {
        let address = Ipv4Addr::new(127, 0, 0, 41);
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![(
                address,
                vec![ExpectedReply::json(r#"["learned.internal"]"#)],
            )],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![
                Resolution::Error(std::io::ErrorKind::NotFound),
                Resolution::Addresses(Vec::new()),
                Resolution::Addresses(vec![IpAddr::V4(address)]),
            ],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver.clone());

        nodes.update_live_nodes().await;
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("entry.test"));
        nodes.update_live_nodes().await;
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("entry.test"));
        nodes.update_live_nodes().await;
        join_servers(servers).await;

        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("learned.internal")
        );
        assert_eq!(resolver.calls_for("entry.test"), 3);
    }

    #[tokio::test]
    async fn dns_lookup_timeout_is_bounded_and_preserves_seed() {
        let resolver = ScriptedResolver::new(vec![("entry.test", vec![Resolution::Pending])]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);
        let started = Instant::now();

        tokio::time::timeout(Duration::from_secs(3), nodes.update_live_nodes())
            .await
            .expect("DNS timeout must bound the refresh");

        assert!(started.elapsed() >= DNS_LOOKUP_TIMEOUT);
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("entry.test"));
    }

    #[tokio::test]
    async fn stalled_dns_lookup_does_not_spawn_unbounded_resolver_tasks() {
        let address = Ipv4Addr::new(127, 0, 0, 40);
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![(
                address,
                vec![ExpectedReply::json(r#"["learned.internal"]"#)],
            )],
        )
        .await;
        let release = Arc::new(Notify::new());
        let inner = ScriptedResolver::new(vec![(
            "entry.test",
            vec![
                Resolution::Delayed {
                    addresses: Vec::new(),
                    release: release.clone(),
                },
                Resolution::Addresses(vec![IpAddr::V4(address)]),
            ],
        )]);
        let resolver: Arc<dyn DiscoveryResolver> =
            Arc::new(BoundedDiscoveryResolver::new(inner.clone()));
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);

        nodes.update_live_nodes().await;
        assert_eq!(inner.calls_for("entry.test"), MAX_IN_FLIGHT_DNS_LOOKUPS);

        let second_nodes = nodes.clone();
        let second = tokio::spawn(async move { second_nodes.update_live_nodes().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            inner.calls_for("entry.test"),
            MAX_IN_FLIGHT_DNS_LOOKUPS,
            "a second OS lookup must wait behind the timed-out lookup"
        );

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .expect("refresh should resume when the stalled lookup finishes")
            .unwrap();
        join_servers(servers).await;

        assert_eq!(inner.calls_for("entry.test"), 2);
        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("learned.internal")
        );
    }

    #[tokio::test]
    async fn stalled_address_times_out_then_later_address_succeeds() {
        let stalled_address = Ipv4Addr::new(127, 0, 0, 53);
        let good_address = Ipv4Addr::new(127, 0, 0, 54);
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![
                (
                    stalled_address,
                    vec![ExpectedReply::for_path("/localnodes", ServerReply::Stall)],
                ),
                (
                    good_address,
                    vec![ExpectedReply::json(r#"["learned.internal"]"#)],
                ),
            ],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![Resolution::Addresses(vec![
                IpAddr::V4(stalled_address),
                IpAddr::V4(good_address),
            ])],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);
        let started = Instant::now();

        tokio::time::timeout(
            DISCOVERY_REQUEST_TIMEOUT + Duration::from_secs(2),
            nodes.update_live_nodes(),
        )
        .await
        .expect("per-address timeout must advance to the next address");
        join_servers(servers).await;

        assert!(started.elapsed() >= DISCOVERY_REQUEST_TIMEOUT);
        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("learned.internal")
        );
    }

    #[tokio::test]
    async fn failed_active_refresh_retains_nodes_then_reresolves_seed_for_recovery() {
        let first_address = Ipv4Addr::new(127, 0, 0, 42);
        let recovered_address = Ipv4Addr::new(127, 0, 0, 43);
        let unavailable_address = Ipv4Addr::new(127, 0, 0, 44);
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![
                (
                    first_address,
                    vec![ExpectedReply::json(r#"["old-node.test"]"#)],
                ),
                (
                    recovered_address,
                    vec![ExpectedReply::json(r#"["new-node.test"]"#)],
                ),
            ],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![
            (
                "entry.test",
                vec![
                    Resolution::Addresses(vec![IpAddr::V4(first_address)]),
                    Resolution::Error(std::io::ErrorKind::NotFound),
                    Resolution::Addresses(vec![IpAddr::V4(recovered_address)]),
                ],
            ),
            (
                "old-node.test",
                vec![
                    Resolution::Addresses(vec![IpAddr::V4(unavailable_address)]),
                    Resolution::Addresses(vec![IpAddr::V4(unavailable_address)]),
                ],
            ),
        ]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver.clone());

        nodes.update_live_nodes().await;
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("old-node.test"));

        nodes.update_live_nodes().await;
        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("old-node.test"),
            "a fully failed refresh must retain the last valid snapshot"
        );

        nodes.update_live_nodes().await;
        join_servers(servers).await;
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("new-node.test"));
        assert_eq!(resolver.calls_for("entry.test"), 3);

        nodes.discovery_started.store(true, Ordering::Release);
        let routed = nodes
            .get_next_node_round_robin(&HashSet::new())
            .expect("recovered node should be routable");
        assert_eq!(routed.host_str(), Some("new-node.test"));
    }

    #[tokio::test]
    async fn partial_scoped_failure_uses_reachable_learned_node_without_seed() {
        let live_address = Ipv4Addr::new(127, 0, 0, 45);
        let unavailable_address = Ipv4Addr::new(127, 0, 0, 46);
        let (port, servers) = start_address_servers(
            "live-node.test",
            vec![(
                live_address,
                vec![ExpectedReply::for_path(
                    "/localnodes?dc=dc1",
                    ServerReply::Http {
                        status: 200,
                        body: r#"["live-node.test"]"#.to_string(),
                    },
                )],
            )],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![
            (
                "dead-node.test",
                vec![Resolution::Addresses(vec![IpAddr::V4(unavailable_address)])],
            ),
            (
                "live-node.test",
                vec![Resolution::Addresses(vec![IpAddr::V4(live_address)])],
            ),
        ]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .routing_scope(RoutingScope::from_datacenter("dc1".to_string()))
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver.clone());
        nodes.live_nodes.store(Arc::new(vec![
            Arc::new(Url::parse(&format!("http://dead-node.test:{port}/")).unwrap()),
            Arc::new(Url::parse(&format!("http://live-node.test:{port}/")).unwrap()),
        ]));

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("live-node.test")
        );
        assert_eq!(resolver.calls_for("entry.test"), 0);
    }

    #[tokio::test]
    async fn wrong_scope_without_fallback_removes_seed_from_routing() {
        let address = Ipv4Addr::new(127, 0, 0, 47);
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![(
                address,
                vec![ExpectedReply::for_path(
                    "/localnodes?dc=missing",
                    ServerReply::Http {
                        status: 200,
                        body: "[]".to_string(),
                    },
                )],
            )],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![Resolution::Addresses(vec![IpAddr::V4(address)])],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .routing_scope(RoutingScope::from_datacenter("missing".to_string()))
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        assert!(nodes.live_nodes.load().is_empty());
        assert_eq!(nodes.seed_urls[0].host_str(), Some("entry.test"));
        nodes.discovery_started.store(true, Ordering::Release);
        assert!(nodes.get_next_node_round_robin(&HashSet::new()).is_none());
    }

    #[tokio::test]
    async fn wrong_scope_uses_configured_fallback_scope() {
        let address = Ipv4Addr::new(127, 0, 0, 48);
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![(
                address,
                vec![
                    ExpectedReply::for_path(
                        "/localnodes?dc=missing",
                        ServerReply::Http {
                            status: 200,
                            body: "[]".to_string(),
                        },
                    ),
                    ExpectedReply::json(r#"["fallback-node.test"]"#),
                ],
            )],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![
                Resolution::Addresses(vec![IpAddr::V4(address)]),
                Resolution::Addresses(vec![IpAddr::V4(address)]),
            ],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .routing_scope(
                RoutingScope::from_datacenter("missing".to_string())
                    .with_fallback(RoutingScope::from_cluster()),
            )
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver.clone());

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("fallback-node.test")
        );
        assert_eq!(resolver.calls_for("entry.test"), 2);
    }

    #[tokio::test]
    async fn invalid_first_seed_does_not_block_another_configured_seed() {
        let invalid_address = Ipv4Addr::new(127, 0, 0, 49);
        let valid_address = Ipv4Addr::new(127, 0, 0, 50);
        let (port, servers) = start_address_servers(
            "unused.test",
            vec![
                (
                    invalid_address,
                    vec![ExpectedReply::json("not-json").for_host("seed-a.test")],
                ),
                (
                    valid_address,
                    vec![ExpectedReply::json(r#"["learned.internal"]"#).for_host("seed-b.test")],
                ),
            ],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![
            (
                "seed-a.test",
                vec![Resolution::Addresses(vec![IpAddr::V4(invalid_address)])],
            ),
            (
                "seed-b.test",
                vec![Resolution::Addresses(vec![IpAddr::V4(valid_address)])],
            ),
        ]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["seed-a.test", "seed-b.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("learned.internal")
        );
    }

    #[tokio::test]
    async fn overlapping_refreshes_are_serialized_and_publish_complete_snapshots() {
        let first_address = Ipv4Addr::new(127, 0, 0, 51);
        let second_address = Ipv4Addr::new(127, 0, 0, 52);
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![
                (
                    first_address,
                    vec![ExpectedReply::for_path(
                        "/localnodes",
                        ServerReply::Delayed {
                            body: r#"["entry.test"]"#.to_string(),
                            entered: entered.clone(),
                            release: release.clone(),
                        },
                    )],
                ),
                (
                    second_address,
                    vec![ExpectedReply::json(r#"["new-node.test"]"#)],
                ),
            ],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![
                Resolution::Addresses(vec![IpAddr::V4(first_address)]),
                Resolution::Addresses(vec![IpAddr::V4(second_address)]),
            ],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver.clone());

        let first_nodes = nodes.clone();
        let first = tokio::spawn(async move { first_nodes.update_live_nodes().await });
        entered.notified().await;
        let second_nodes = nodes.clone();
        let second = tokio::spawn(async move { second_nodes.update_live_nodes().await });
        tokio::task::yield_now().await;

        assert_eq!(resolver.calls_for("entry.test"), 1);
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("entry.test"));
        release.notify_one();
        first.await.unwrap();
        second.await.unwrap();
        join_servers(servers).await;

        assert_eq!(nodes.live_nodes.load().len(), 1);
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("new-node.test"));
    }

    #[tokio::test]
    async fn resolved_address_list_is_deduplicated_and_capped() {
        let unique_addresses = (1..=40)
            .map(|last| IpAddr::V4(Ipv4Addr::new(127, 1, 0, last)))
            .collect::<Vec<_>>();
        let mut answers = unique_addresses.clone();
        answers.splice(1..1, [unique_addresses[0], unique_addresses[0]]);
        let resolver =
            ScriptedResolver::new(vec![("entry.test", vec![Resolution::Addresses(answers)])]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);

        let (_, _, addresses) = nodes
            .resolve_node_addresses(&nodes.seed_urls[0])
            .await
            .unwrap();

        assert_eq!(addresses.len(), MAX_RESOLVED_ADDRESSES);
        assert_eq!(addresses[0].ip(), unique_addresses[0]);
        assert_eq!(addresses[1].ip(), unique_addresses[1]);
    }

    #[test]
    fn discovery_client_cache_is_bounded() {
        let mut cache = DiscoveryClientCache::default();
        for last in 1..=(MAX_CACHED_DISCOVERY_CLIENTS + 10) {
            let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 2, 0, last as u8)), 8000);
            cache
                .get_or_insert(&format!("entry-{last}.test"), address, true)
                .unwrap();
        }

        assert_eq!(cache.clients.len(), MAX_CACHED_DISCOVERY_CLIENTS);
        assert_eq!(cache.insertion_order.len(), MAX_CACHED_DISCOVERY_CLIENTS);
    }

    #[tokio::test]
    async fn all_unavailable_dns_records_return_without_clearing_seed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let nodes = dns_live_nodes(
            port,
            &[
                IpAddr::V6(Ipv6Addr::LOCALHOST),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
            ],
        );
        tokio::time::timeout(Duration::from_secs(1), nodes.update_live_nodes())
            .await
            .expect("discovery must not hang when both address families are unavailable");

        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("dual.test"));
    }

    #[tokio::test]
    async fn refresh_recovers_through_original_raw_ipv6_seed() {
        let (port, server) = start_localnodes_server_on("[::1]:0", "[::1]", r#"["::1"]"#).await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["::1"])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        nodes.live_nodes.store(Arc::new(vec![Arc::new(
            Url::parse(&format!("http://127.0.0.1:{port}/")).unwrap(),
        )]));

        nodes.update_live_nodes().await;

        server.await.unwrap();
        assert_eq!(
            nodes.live_nodes.load()[0].as_str(),
            format!("http://[::1]:{port}/")
        );
    }

    async fn assert_dns_discovery(bind_address: &str, resolved_ips: &[IpAddr]) {
        let (port, server) =
            start_localnodes_server_on(bind_address, "dual.test", r#"["dual.test"]"#).await;
        let nodes = dns_live_nodes(port, resolved_ips);

        nodes.update_live_nodes().await;

        server.await.unwrap();
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("dual.test"));
    }

    fn dns_live_nodes(port: u16, resolved_ips: &[IpAddr]) -> Arc<LiveNodes> {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["dual.test"])
            .build();
        let mut nodes = LiveNodes::new(&config).unwrap();
        Arc::get_mut(&mut nodes).unwrap().resolver = Arc::new(StaticResolver {
            addresses: resolved_ips.to_vec(),
        });
        nodes
    }
}
