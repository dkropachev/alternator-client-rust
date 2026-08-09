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
//! Underneath it uses a basic [`reqwest::Client`] with timeouts, redirects and
//! environment proxies disabled, and strict per-address socket overrides.
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
//! A refresh is capped at 30 seconds, 64 candidates, 32 socket addresses per
//! hostname, and 256 parsed topology nodes. The original seed set is capped at
//! 16 entries. DNS work is globally limited to eight coalesced in-flight
//! lookups so a stalled hostname cannot create unbounded resolver tasks.
//!
//! For cluster-wide scope, the refresh queries `/localnodes` from configured
//! seed nodes and already-known live nodes, then unions the responses. To cover
//! all datacenters, the initial configuration must include at least one working
//! seed host from every datacenter that should receive traffic.
//!
//! A non-empty successful result atomically replaces [`live_nodes`] using
//! [`ArcSwap`]. A fully failed refresh preserves the previous snapshot and the
//! original seeds. Non-cluster seeds are discovery-only until a scoped or
//! fallback response validates a routing snapshot. A conclusive empty scoped
//! result retains the empty routing snapshot and the seeds as future discovery
//! candidates.
//!
//!  # Lifetime
//!
//! The background task owns only cloned refresh state, not [`LiveNodes`]. This
//! lets [`Drop`] run as soon as the last external [`Arc`] is released and abort
//! an in-progress refresh without waiting for network timeouts.
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
use serde::de::{Error as _, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use url::{Host, Url};

const DEFAULT_ACTIVE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_IDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const DNS_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);
const DISCOVERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DISCOVERY_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const DISCOVERY_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_SEED_NODES: usize = 16;
const MAX_RESOLVED_ADDRESSES: usize = 32;
const MAX_DISCOVERY_CANDIDATES: usize = 64;
const MAX_DISCOVERED_NODES: usize = 256;
const MAX_CACHED_DISCOVERY_CLIENTS: usize = 64;
const MAX_DISCOVERY_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_IN_FLIGHT_DNS_LOOKUPS: usize = 8;

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

#[derive(Clone, Debug)]
enum SharedLookupResult {
    Addresses(Vec<SocketAddr>),
    Error {
        kind: std::io::ErrorKind,
        message: String,
    },
}

impl SharedLookupResult {
    fn from_io_result(result: std::io::Result<Vec<SocketAddr>>) -> Self {
        match result {
            Ok(addresses) => Self::Addresses(addresses),
            Err(error) => Self::Error {
                kind: error.kind(),
                message: error.to_string(),
            },
        }
    }

    fn into_io_result(self) -> std::io::Result<Vec<SocketAddr>> {
        match self {
            Self::Addresses(addresses) => Ok(addresses),
            Self::Error { kind, message } => Err(std::io::Error::new(kind, message)),
        }
    }
}

#[derive(Debug)]
struct InFlightLookup {
    result: tokio::sync::watch::Sender<Option<SharedLookupResult>>,
}

impl InFlightLookup {
    fn new() -> Self {
        let (result, _receiver) = tokio::sync::watch::channel(None);
        Self { result }
    }

    fn complete(&self, result: SharedLookupResult) {
        self.result.send_replace(Some(result));
    }

    async fn wait(&self) -> std::io::Result<Vec<SocketAddr>> {
        let mut result = self.result.subscribe();
        loop {
            if let Some(result) = result.borrow().clone() {
                return result.into_io_result();
            }
            result.changed().await.map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "DNS lookup result channel closed",
                )
            })?;
        }
    }
}

type LookupKey = (String, u16);

#[derive(Debug)]
struct BoundedResolverState {
    lookup_slots: Arc<tokio::sync::Semaphore>,
    in_flight: Mutex<HashMap<LookupKey, Arc<InFlightLookup>>>,
    peak_in_flight: AtomicUsize,
}

impl BoundedResolverState {
    fn new() -> Self {
        Self {
            lookup_slots: Arc::new(tokio::sync::Semaphore::new(MAX_IN_FLIGHT_DNS_LOOKUPS)),
            in_flight: Mutex::new(HashMap::new()),
            peak_in_flight: AtomicUsize::new(0),
        }
    }

    fn register(self: &Arc<Self>, key: LookupKey) -> std::io::Result<(Arc<InFlightLookup>, bool)> {
        let mut in_flight = self.in_flight.lock().unwrap();
        if let Some(lookup) = in_flight.get(&key) {
            return Ok((lookup.clone(), false));
        }
        if in_flight.len() >= MAX_IN_FLIGHT_DNS_LOOKUPS {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "DNS lookup capacity exhausted",
            ));
        }

        let lookup = Arc::new(InFlightLookup::new());
        in_flight.insert(key, lookup.clone());
        self.peak_in_flight
            .fetch_max(in_flight.len(), Ordering::Relaxed);
        Ok((lookup, true))
    }
}

struct LookupRegistration {
    state: Weak<BoundedResolverState>,
    key: LookupKey,
    lookup: Arc<InFlightLookup>,
}

impl Drop for LookupRegistration {
    fn drop(&mut self) {
        let incomplete = self.lookup.result.borrow().is_none();
        if let Some(state) = self.state.upgrade() {
            let mut in_flight = state.in_flight.lock().unwrap();
            if in_flight
                .get(&self.key)
                .is_some_and(|current| Arc::ptr_eq(current, &self.lookup))
            {
                in_flight.remove(&self.key);
            }
        }
        if incomplete {
            self.lookup.complete(SharedLookupResult::Error {
                kind: std::io::ErrorKind::Interrupted,
                message: "DNS lookup task was cancelled".to_string(),
            });
        }
    }
}

/// Coalesces lookups for the same hostname while allowing unrelated hostnames
/// to progress through a small shared worker budget. Timed-out callers do not
/// cancel or duplicate the underlying OS resolver work.
#[derive(Debug)]
struct BoundedDiscoveryResolver {
    inner: Arc<dyn DiscoveryResolver>,
    state: Arc<BoundedResolverState>,
}

impl BoundedDiscoveryResolver {
    fn new(inner: Arc<dyn DiscoveryResolver>) -> Self {
        Self {
            inner,
            state: Arc::new(BoundedResolverState::new()),
        }
    }

    #[cfg(test)]
    fn in_flight_count(&self) -> usize {
        self.state.in_flight.lock().unwrap().len()
    }

    #[cfg(test)]
    fn peak_in_flight_count(&self) -> usize {
        self.state.peak_in_flight.load(Ordering::Relaxed)
    }
}

impl DiscoveryResolver for BoundedDiscoveryResolver {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
        let host = host.to_string();
        let inner = self.inner.clone();
        let state = self.state.clone();
        Box::pin(async move {
            let key = (host.clone(), port);
            let (lookup, should_spawn) = state.register(key.clone())?;
            if should_spawn {
                let task_lookup = lookup.clone();
                let task_state = state.clone();
                tokio::spawn(async move {
                    let _registration = LookupRegistration {
                        state: Arc::downgrade(&task_state),
                        key,
                        lookup: task_lookup.clone(),
                    };
                    let Ok(_permit) = task_state.lookup_slots.clone().acquire_owned().await else {
                        return;
                    };
                    let result =
                        SharedLookupResult::from_io_result(inner.resolve(&host, port).await);
                    drop(_permit);
                    {
                        let mut in_flight = task_state.in_flight.lock().unwrap();
                        task_lookup.complete(result);
                        if in_flight
                            .get(&_registration.key)
                            .is_some_and(|current| Arc::ptr_eq(current, &task_lookup))
                        {
                            in_flight.remove(&_registration.key);
                        }
                    }
                });
            }
            lookup.wait().await
        })
    }
}

fn system_discovery_resolver() -> Arc<dyn DiscoveryResolver> {
    static RESOLVER: OnceLock<Arc<BoundedDiscoveryResolver>> = OnceLock::new();
    RESOLVER
        .get_or_init(|| {
            Arc::new(BoundedDiscoveryResolver::new(Arc::new(
                SystemDiscoveryResolver,
            )))
        })
        .clone()
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
            .connect_timeout(DISCOVERY_CONNECT_TIMEOUT)
            // Discovery must use exactly the selected DNS socket. Following a
            // redirect or an environment proxy would escape address fallback.
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy();
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

struct BoundedNodeList(Vec<String>);

impl<'de> Deserialize<'de> for BoundedNodeList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NodeListVisitor;

        impl<'de> Visitor<'de> for NodeListVisitor {
            type Value = BoundedNodeList;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    formatter,
                    "an array containing at most {MAX_DISCOVERED_NODES} node addresses"
                )
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut nodes = Vec::with_capacity(
                    sequence
                        .size_hint()
                        .unwrap_or_default()
                        .min(MAX_DISCOVERED_NODES),
                );
                while let Some(node) = sequence.next_element::<String>()? {
                    if nodes.len() >= MAX_DISCOVERED_NODES {
                        return Err(A::Error::custom("discovered node limit exceeded"));
                    }
                    nodes.push(node);
                }
                Ok(BoundedNodeList(nodes))
            }
        }

        deserializer.deserialize_seq(NodeListVisitor)
    }
}

#[derive(Clone)]
struct DiscoveryRefresh {
    routing_scope: RoutingScope,
    live_nodes: Arc<ArcSwap<Vec<Arc<Url>>>>,
    seed_urls: Arc<Vec<Arc<Url>>>,
    alternator_scheme: String,
    port: Option<u16>,
    resolver: Arc<dyn DiscoveryResolver>,
    discovery_clients: Arc<Mutex<DiscoveryClientCache>>,
    update_lock: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug)]
pub struct LiveNodes {
    routing_scope: RoutingScope,
    active_interval: Duration,
    idle_interval: Duration,
    counter: Arc<AtomicUsize>,
    live_nodes: Arc<ArcSwap<Vec<Arc<Url>>>>,
    seed_urls: Arc<Vec<Arc<Url>>>,
    alternator_scheme: String,
    port: Option<u16>,
    resolver: Arc<dyn DiscoveryResolver>,
    discovery_clients: Arc<Mutex<DiscoveryClientCache>>,
    update_lock: Arc<tokio::sync::Mutex<()>>,
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

        let mut seed_urls = Vec::new();
        let mut seen_seeds = HashSet::new();
        for addr in &seed_nodes {
            let Ok(url) = build_node_url(&alternator_scheme, addr, port) else {
                continue;
            };
            if seen_seeds.insert(url.as_str().to_string()) {
                seed_urls.push(Arc::new(url));
            }
            if seed_urls.len() >= MAX_SEED_NODES {
                break;
            }
        }
        seed_urls.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        if seed_urls.is_empty() {
            return None;
        }
        let initial_live_nodes = if routing_scope.is_cluster() {
            seed_urls.clone()
        } else {
            Vec::new()
        };

        Some(Arc::new(Self {
            routing_scope,
            active_interval,
            idle_interval,
            counter: Arc::new(AtomicUsize::new(0)),
            live_nodes: Arc::new(ArcSwap::from_pointee(initial_live_nodes)),
            seed_urls: Arc::new(seed_urls),
            alternator_scheme,
            port,
            resolver: system_discovery_resolver(),
            discovery_clients: Arc::new(Mutex::new(DiscoveryClientCache::default())),
            update_lock: Arc::new(tokio::sync::Mutex::new(())),
            last_activity: Arc::new(Mutex::new(Instant::now())),
            notify: Arc::new(tokio::sync::Notify::new()),
            bg_task: std::sync::Mutex::new(None),
            discovery_started: AtomicBool::new(false),
        }))
    }

    fn refresh_context(&self) -> DiscoveryRefresh {
        DiscoveryRefresh {
            routing_scope: self.routing_scope.clone(),
            live_nodes: self.live_nodes.clone(),
            seed_urls: self.seed_urls.clone(),
            alternator_scheme: self.alternator_scheme.clone(),
            port: self.port,
            resolver: self.resolver.clone(),
            discovery_clients: self.discovery_clients.clone(),
            update_lock: self.update_lock.clone(),
        }
    }

    #[cfg(test)]
    async fn resolve_node_addresses(
        &self,
        node_addr: &Url,
    ) -> Option<(String, bool, Vec<SocketAddr>)> {
        self.refresh_context()
            .resolve_node_addresses(node_addr)
            .await
    }
}

impl DiscoveryRefresh {
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
            let Ok(BoundedNodeList(nodes)) = serde_json::from_slice::<BoundedNodeList>(&body)
            else {
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

    fn discovery_candidates(&self) -> Vec<Arc<Url>> {
        let mut learned = self.live_nodes.load().as_ref().clone();
        learned.shuffle(&mut rand::rng());

        // Reserve room for every bounded original seed so recovery remains
        // possible even when the learned topology fills the candidate budget.
        learned.truncate(MAX_DISCOVERY_CANDIDATES.saturating_sub(self.seed_urls.len()));
        let mut candidates = learned;
        let mut seen = candidates
            .iter()
            .map(|candidate| candidate.as_str().to_string())
            .collect::<HashSet<_>>();
        for seed in self.seed_urls.iter() {
            if seen.insert(seed.as_str().to_string()) {
                candidates.push(seed.clone());
            }
        }
        candidates.truncate(MAX_DISCOVERY_CANDIDATES);
        candidates
    }

    async fn discover_cluster_live_nodes(&self) -> Option<Vec<Arc<Url>>> {
        let scope = RoutingScope::from_cluster();
        let mut new_nodes = Vec::new();
        let mut got_response = false;

        for node_addr in self.discovery_candidates() {
            if node_is_in_list(&node_addr, &new_nodes) {
                continue;
            }

            if let Some(mut nodes) = self.fetch_live_nodes_for_scope(&scope, &node_addr).await {
                got_response = true;
                new_nodes.append(&mut nodes);
                new_nodes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
                new_nodes.dedup_by(|a, b| a.as_str() == b.as_str());
                new_nodes.truncate(MAX_DISCOVERED_NODES);
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

        for node_addr in self.discovery_candidates() {
            match self.fetch_live_nodes_for_scope(scope, &node_addr).await {
                Some(nodes) if !nodes.is_empty() => return Some(nodes),
                Some(_) => saw_empty_response = true,
                None => {}
            }
        }

        saw_empty_response.then(Vec::new)
    }
}

impl LiveNodes {
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
        let refresh = self.refresh_context();
        let notify = self.notify.clone();
        let last_activity = self.last_activity.clone();
        let idle_interval = self.idle_interval;
        let active_interval = self.active_interval;

        self.mark_activity();
        let handle = tokio::spawn(async move {
            loop {
                refresh.update_live_nodes().await;
                let last = *last_activity.lock().unwrap();
                let is_idle = last.elapsed() >= idle_interval;

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

        *self
            .bg_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(handle.abort_handle());
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
        self.refresh_context().update_live_nodes().await;
    }
}

impl DiscoveryRefresh {
    async fn update_live_nodes(&self) {
        let _ = tokio::time::timeout(
            DISCOVERY_REFRESH_TIMEOUT,
            self.update_live_nodes_serialized(),
        )
        .await;
    }

    async fn update_live_nodes_serialized(&self) {
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
        if let Some(task) = self
            .bg_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AlternatorClient;
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
        Redirect {
            location: String,
        },
        Truncated(String),
        OversizedDeclared,
        OversizedChunked,
        Stall,
        Reset,
        WaitForDisconnect {
            entered: Arc<Notify>,
            disconnected: Arc<Notify>,
        },
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
                            ServerReply::Redirect { location } => {
                                let response = format!(
                                    "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
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
                            ServerReply::WaitForDisconnect {
                                entered,
                                disconnected,
                            } => {
                                entered.notify_one();
                                let mut byte = [0; 1];
                                while stream.read(&mut byte).await.unwrap_or_default() != 0 {}
                                disconnected.notify_one();
                            }
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
                                let _ = stream.write_all(response.as_bytes()).await;
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

    async fn start_redirect_target() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let task_hits = hits.clone();
        let task = tokio::spawn(async move {
            if let Ok(Ok((mut stream, _))) =
                tokio::time::timeout(Duration::from_millis(500), listener.accept()).await
            {
                task_hits.fetch_add(1, Ordering::SeqCst);
                let mut request = [0; 2048];
                let _ = stream.read(&mut request).await;
                let body = r#"["redirected.test"]"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        (format!("http://{address}/localnodes"), hits, task)
    }

    async fn read_request_headers<R>(stream: &mut R) -> String
    where
        R: tokio::io::AsyncRead + Unpin,
    {
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
        String::from_utf8(request).unwrap()
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

    #[tokio::test]
    async fn in_flight_lookup_completion_cannot_be_missed_by_waiters() {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000);

        let completed = InFlightLookup::new();
        completed.complete(SharedLookupResult::Addresses(vec![address]));
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), completed.wait())
                .await
                .expect("completion before subscription must remain observable")
                .unwrap(),
            vec![address]
        );

        for _ in 0..256 {
            let lookup = Arc::new(InFlightLookup::new());
            let waiter_lookup = lookup.clone();
            let waiter = tokio::spawn(async move { waiter_lookup.wait().await });
            tokio::task::yield_now().await;
            lookup.complete(SharedLookupResult::Addresses(vec![address]));
            assert_eq!(
                tokio::time::timeout(Duration::from_millis(100), waiter)
                    .await
                    .expect("racing completion must wake the waiter")
                    .unwrap()
                    .unwrap(),
                vec![address]
            );
        }
    }

    #[tokio::test]
    async fn resolver_rejects_work_beyond_the_global_task_bound() {
        let entries = (0..=MAX_IN_FLIGHT_DNS_LOOKUPS)
            .map(|index| (format!("pending-{index}.test"), vec![Resolution::Pending]))
            .collect::<Vec<_>>();
        let inner = ScriptedResolver::new(
            entries
                .iter()
                .map(|(host, answers)| (host.as_str(), answers.clone()))
                .collect(),
        );
        let resolver = BoundedDiscoveryResolver::new(inner);

        for (host, _) in entries.iter().take(MAX_IN_FLIGHT_DNS_LOOKUPS) {
            assert!(
                tokio::time::timeout(Duration::from_millis(10), resolver.resolve(host, 8000))
                    .await
                    .is_err()
            );
        }
        let overflow = resolver
            .resolve(&entries[MAX_IN_FLIGHT_DNS_LOOKUPS].0, 8000)
            .await
            .unwrap_err();

        assert_eq!(overflow.kind(), std::io::ErrorKind::WouldBlock);
        assert_eq!(resolver.in_flight_count(), MAX_IN_FLIGHT_DNS_LOOKUPS);
        assert_eq!(resolver.peak_in_flight_count(), MAX_IN_FLIGHT_DNS_LOOKUPS);
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
    async fn redirect_response_does_not_escape_selected_address() {
        let redirecting_address = Ipv4Addr::new(127, 0, 0, 56);
        let good_address = Ipv4Addr::new(127, 0, 0, 57);
        let (redirect_location, redirect_hits, redirect_target) = start_redirect_target().await;
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![
                (
                    redirecting_address,
                    vec![ExpectedReply::for_path(
                        "/localnodes",
                        ServerReply::Redirect {
                            location: redirect_location,
                        },
                    )],
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
                IpAddr::V4(redirecting_address),
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

        nodes.update_live_nodes().await;
        join_servers(servers).await;
        redirect_target.await.unwrap();

        assert_eq!(redirect_hits.load(Ordering::SeqCst), 0);
        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("learned.internal")
        );
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

        let make_server_config = |dns_name: &str| {
            let key = rcgen::KeyPair::generate().unwrap();
            let params = rcgen::CertificateParams::new(vec![dns_name.to_string()]).unwrap();
            let cert = params.signed_by(&key, &ca_cert, &ca_key).unwrap();
            Arc::new(
                rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(
                        vec![rustls::pki_types::CertificateDer::from(cert.der().to_vec())],
                        rustls::pki_types::PrivateKeyDer::try_from(key.serialize_der()).unwrap(),
                    )
                    .unwrap(),
            )
        };
        // The good certificate contains only the logical DNS name, never a
        // selected socket IP. The first address has a trusted but wrong name.
        let good_tls_config = make_server_config("localhost");
        let bad_tls_config = make_server_config("wrong-name.test");

        let good_address = Ipv4Addr::LOCALHOST;
        let bad_address = Ipv4Addr::new(127, 0, 0, 36);
        let good_listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(good_address), 0))
            .await
            .unwrap();
        let port = good_listener.local_addr().unwrap().port();
        let bad_listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(bad_address), port))
            .await
            .unwrap();
        let bad_sni = Arc::new(Mutex::new(Vec::new()));
        let good_sni = Arc::new(Mutex::new(Vec::new()));
        let good_hosts = Arc::new(Mutex::new(Vec::new()));

        let bad_server = {
            let bad_sni = bad_sni.clone();
            tokio::spawn(async move {
                let (stream, _) = bad_listener.accept().await.unwrap();
                let start = tokio_rustls::LazyConfigAcceptor::new(
                    rustls::server::Acceptor::default(),
                    stream,
                )
                .await
                .unwrap();
                bad_sni
                    .lock()
                    .unwrap()
                    .push(start.client_hello().server_name().unwrap().to_string());
                assert!(start.into_stream(bad_tls_config).await.is_err());
            })
        };
        let good_server = {
            let good_sni = good_sni.clone();
            let good_hosts = good_hosts.clone();
            tokio::spawn(async move {
                for _ in 0..2 {
                    let (stream, _) = good_listener.accept().await.unwrap();
                    let start = tokio_rustls::LazyConfigAcceptor::new(
                        rustls::server::Acceptor::default(),
                        stream,
                    )
                    .await
                    .unwrap();
                    good_sni
                        .lock()
                        .unwrap()
                        .push(start.client_hello().server_name().unwrap().to_string());
                    let mut stream = start.into_stream(good_tls_config.clone()).await.unwrap();
                    let request = read_request_headers(&mut stream).await;
                    let request_lower = request.to_ascii_lowercase();
                    assert!(request_lower.contains(&format!("\r\nhost: localhost:{port}\r\n")));
                    good_hosts.lock().unwrap().push(format!("localhost:{port}"));

                    let (content_type, body) = if request.starts_with("GET /localnodes HTTP/1.1") {
                        ("application/json", r#"["localhost"]"#)
                    } else {
                        assert!(request.starts_with("POST / HTTP/1.1"));
                        ("application/x-amz-json-1.0", r#"{"TableNames":[]}"#)
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                }
            })
        };

        let tls_context = aws_smithy_http_client::tls::TlsContext::builder()
            .with_trust_store(
                aws_smithy_http_client::tls::TrustStore::empty()
                    .with_pem_certificate(ca_cert.pem().into_bytes()),
            )
            .build()
            .unwrap();
        let http_client = aws_smithy_http_client::Builder::new()
            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
            ))
            .tls_context(tls_context)
            .build_https();
        let resolver = ScriptedResolver::new(vec![(
            "localhost",
            vec![Resolution::Addresses(vec![
                IpAddr::V4(bad_address),
                IpAddr::V4(good_address),
            ])],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .http_client(http_client)
            .endpoint_url(format!("https://localhost:{port}"))
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);
        nodes
            .discovery_clients
            .lock()
            .unwrap()
            .additional_root_certificate =
            Some(reqwest::Certificate::from_pem(ca_cert.pem().as_bytes()).unwrap());

        nodes.update_live_nodes().await;
        let snapshot = nodes.live_nodes.load();
        assert_eq!(snapshot[0].scheme(), "https");
        assert_eq!(snapshot[0].host_str(), Some("localhost"));
        assert_eq!(snapshot[0].port(), Some(port));
        drop(snapshot);

        nodes.discovery_started.store(true, Ordering::Release);
        let client = AlternatorClient::from_conf_with_live_nodes(config, nodes);
        client.list_tables().send().await.unwrap();

        bad_server.await.unwrap();
        good_server.await.unwrap();
        assert_eq!(bad_sni.lock().unwrap().as_slice(), &["localhost"]);
        assert_eq!(
            good_sni.lock().unwrap().as_slice(),
            &["localhost", "localhost"]
        );
        assert_eq!(
            good_hosts.lock().unwrap().as_slice(),
            &[format!("localhost:{port}"), format!("localhost:{port}")]
        );
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
        let previous = Arc::new(Url::parse(&format!("http://127.0.0.99:{port}/")).unwrap());
        nodes.live_nodes.store(Arc::new(vec![previous.clone()]));

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        assert_eq!(nodes.live_nodes.load().as_slice(), &[previous]);
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
        let resolver = Arc::new(BoundedDiscoveryResolver::new(inner.clone()));
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver.clone());

        nodes.update_live_nodes().await;
        assert_eq!(inner.calls_for("entry.test"), 1);
        assert_eq!(resolver.in_flight_count(), 1);
        assert_eq!(resolver.peak_in_flight_count(), 1);

        let second_nodes = nodes.clone();
        let second = tokio::spawn(async move { second_nodes.update_live_nodes().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            inner.calls_for("entry.test"),
            1,
            "a second refresh must coalesce with the timed-out lookup"
        );

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .expect("refresh should resume when the stalled lookup finishes")
            .unwrap();
        assert_eq!(resolver.in_flight_count(), 0);

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        assert_eq!(inner.calls_for("entry.test"), 2);
        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("learned.internal")
        );
    }

    #[tokio::test]
    async fn permanently_stalled_seed_does_not_poison_healthy_seed_or_retries() {
        let healthy_address = Ipv4Addr::new(127, 0, 0, 55);
        let healthy_reply = || {
            ExpectedReply::for_path(
                "/localnodes?dc=dc1",
                ServerReply::Http {
                    status: 200,
                    body: r#"["127.0.0.55"]"#.to_string(),
                },
            )
            .for_host("b-healthy.test")
        };
        let (port, servers) = start_address_servers(
            "b-healthy.test",
            vec![(
                healthy_address,
                vec![healthy_reply(), healthy_reply(), healthy_reply()],
            )],
        )
        .await;
        let inner = ScriptedResolver::new(vec![
            ("a-pending.test", vec![Resolution::Pending]),
            (
                "b-healthy.test",
                vec![
                    Resolution::Addresses(vec![IpAddr::V4(healthy_address)]),
                    Resolution::Addresses(vec![IpAddr::V4(healthy_address)]),
                    Resolution::Addresses(vec![IpAddr::V4(healthy_address)]),
                ],
            ),
        ]);
        let resolver = Arc::new(BoundedDiscoveryResolver::new(inner.clone()));
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["a-pending.test", "b-healthy.test"])
            .routing_scope(RoutingScope::from_datacenter("dc1".to_string()))
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver.clone());

        for _ in 0..3 {
            nodes.update_live_nodes().await;
            assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("127.0.0.55"));
            nodes.live_nodes.store(Arc::new(Vec::new()));
        }
        join_servers(servers).await;

        assert_eq!(inner.calls_for("a-pending.test"), 1);
        assert_eq!(inner.calls_for("b-healthy.test"), 3);
        assert_eq!(resolver.in_flight_count(), 1);
        assert_eq!(resolver.peak_in_flight_count(), 2);
        assert!(resolver.peak_in_flight_count() <= MAX_IN_FLIGHT_DNS_LOOKUPS);
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
    async fn dropping_last_owner_aborts_stalled_multi_address_refresh_promptly() {
        let first_address = Ipv4Addr::new(127, 0, 1, 1);
        let entered = Arc::new(Notify::new());
        let disconnected = Arc::new(Notify::new());
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![(
                first_address,
                vec![ExpectedReply::for_path(
                    "/localnodes",
                    ServerReply::WaitForDisconnect {
                        entered: entered.clone(),
                        disconnected: disconnected.clone(),
                    },
                )],
            )],
        )
        .await;
        let addresses = (1..=MAX_RESOLVED_ADDRESSES)
            .map(|last| IpAddr::V4(Ipv4Addr::new(127, 0, 1, last as u8)))
            .collect();
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, Arc::new(StaticResolver { addresses }));

        nodes.ensure_discovery_started();
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("background refresh did not reach the stalled address");
        let weak_nodes = Arc::downgrade(&nodes);
        let dropped_at = Instant::now();
        drop(nodes);

        tokio::time::timeout(Duration::from_millis(250), async {
            while weak_nodes.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background refresh retained the last LiveNodes owner");
        assert!(dropped_at.elapsed() < Duration::from_millis(250));

        tokio::time::timeout(Duration::from_millis(250), disconnected.notified())
            .await
            .expect("dropping LiveNodes did not cancel the stalled discovery request");
        join_servers(servers).await;
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
    async fn scoped_operations_fail_closed_until_discovery_recovers() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = seed_listener.local_addr().unwrap().port();
        let learned_listener = TcpListener::bind(format!("127.0.0.2:{port}"))
            .await
            .unwrap();
        let seed_posts = Arc::new(AtomicUsize::new(0));
        let learned_posts = Arc::new(AtomicUsize::new(0));

        let seed_task = {
            let seed_posts = seed_posts.clone();
            tokio::spawn(async move {
                let discovery_responses = ["[]", "not-json", r#"["127.0.0.2"]"#];
                let mut discovery_index = 0;
                while discovery_index < discovery_responses.len() {
                    let (mut stream, _) = seed_listener.accept().await.unwrap();
                    let request = read_request_headers(&mut stream).await;
                    if request.starts_with("POST / HTTP/1.1") {
                        seed_posts.fetch_add(1, Ordering::SeqCst);
                        let body = r#"{"TableNames":[]}"#;
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream.write_all(response.as_bytes()).await.unwrap();
                        continue;
                    }

                    assert!(request.starts_with("GET /localnodes?dc=dc1 HTTP/1.1"));
                    let body = discovery_responses[discovery_index];
                    discovery_index += 1;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream.write_all(response.as_bytes()).await.unwrap();
                }
            })
        };
        let learned_task = {
            let learned_posts = learned_posts.clone();
            tokio::spawn(async move {
                let (mut stream, _) = learned_listener.accept().await.unwrap();
                let request = read_request_headers(&mut stream).await;
                assert!(request.starts_with("POST / HTTP/1.1"));
                learned_posts.fetch_add(1, Ordering::SeqCst);
                let body = r#"{"TableNames":[]}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            })
        };

        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url(format!("http://127.0.0.1:{port}"))
            .routing_scope(RoutingScope::from_datacenter("dc1".to_string()))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        assert!(nodes.live_nodes.load().is_empty());
        nodes.discovery_started.store(true, Ordering::Release);
        let client = AlternatorClient::from_conf_with_live_nodes(config, nodes.clone());

        assert!(client.list_tables().send().await.is_err());
        assert_eq!(seed_posts.load(Ordering::SeqCst), 0);

        nodes.update_live_nodes().await;
        assert!(nodes.live_nodes.load().is_empty());
        assert!(client.list_tables().send().await.is_err());
        assert_eq!(seed_posts.load(Ordering::SeqCst), 0);

        nodes.update_live_nodes().await;
        assert!(nodes.live_nodes.load().is_empty());
        assert!(client.list_tables().send().await.is_err());
        assert_eq!(seed_posts.load(Ordering::SeqCst), 0);

        nodes.update_live_nodes().await;
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("127.0.0.2"));
        client.list_tables().send().await.unwrap();

        seed_task.await.unwrap();
        learned_task.await.unwrap();
        assert_eq!(seed_posts.load(Ordering::SeqCst), 0);
        assert_eq!(learned_posts.load(Ordering::SeqCst), 1);
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
    fn parsed_topology_and_refresh_candidates_are_bounded() {
        let maximum = vec!["127.0.0.1"; MAX_DISCOVERED_NODES];
        let parsed: BoundedNodeList =
            serde_json::from_slice(&serde_json::to_vec(&maximum).unwrap()).unwrap();
        assert_eq!(parsed.0.len(), MAX_DISCOVERED_NODES);

        let over_limit = vec!["127.0.0.1"; MAX_DISCOVERED_NODES + 1];
        assert!(
            serde_json::from_slice::<BoundedNodeList>(&serde_json::to_vec(&over_limit).unwrap())
                .is_err()
        );

        let seeds = (0..(MAX_SEED_NODES + 10))
            .map(|index| format!("seed-{index}.test"))
            .collect::<Vec<_>>();
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(seeds)
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        assert_eq!(nodes.seed_urls.len(), MAX_SEED_NODES);

        let learned = (0..MAX_DISCOVERED_NODES)
            .map(|index| {
                Arc::new(Url::parse(&format!("http://learned-{index}.test:8000/")).unwrap())
            })
            .collect();
        nodes.live_nodes.store(Arc::new(learned));
        let candidates = nodes.refresh_context().discovery_candidates();
        assert_eq!(candidates.len(), MAX_DISCOVERY_CANDIDATES);
        for seed in nodes.seed_urls.iter() {
            assert!(node_is_in_list(seed, &candidates));
        }
        assert_eq!(DISCOVERY_REFRESH_TIMEOUT, Duration::from_secs(30));
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
