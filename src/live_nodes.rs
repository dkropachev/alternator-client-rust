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
//! nodes in a persistent rotating order to get an updated list of live nodes. After a
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
//! Each scoped refresh starts from the highest scope in the fallback chain and
//! rotates fairly through every current node and original seed endpoint.
//! A hostname is resolved once per candidate pass, and the complete deduplicated
//! address generation is retained until every record has an outcome. Each
//! address is tried in a persistent rotating order while the request URL retains
//! the logical hostname. Transport errors, non-success responses, and malformed
//! or unusable data advance to the next address and candidate. The 30-second
//! refresh budget is split across scopes in priority order, so an unavailable
//! preferred scope cannot perpetually starve its fallbacks. Candidate and
//! address queues rotate before I/O, so repeated refreshes make progress past a
//! deadline. All valid nodes in a bounded 1 MiB response are retained. Seeds
//! and cluster unions share derived node and URL-memory bounds; an oversized
//! configuration or unique multi-response accumulation fails closed as a whole
//! rather than silently truncating configuration or replacing a valid snapshot.
//!
//! DNS work is globally limited to eight in-flight lookups, coalesced by
//! normalized hostname. Operating-system name resolution is not reliably
//! cancellable. If all eight unique lookups remain stuck, new unique hostnames
//! fail closed immediately rather than queue or spawn more work. A literal IP
//! seed bypasses OS name resolution and provides a cancellation-safe recovery
//! path through the separately bounded HTTP discovery request.
//!
//! HTTPS discovery uses a dedicated reqwest client and the platform's native
//! root store. A custom CA configured only on the AWS SDK HTTP client is not
//! reused for `/localnodes`; private discovery CAs must also be installed in
//! native trust (or exposed through the platform's supported trust-file
//! environment, such as `SSL_CERT_FILE`).
//!
//! For cluster-wide scope, the refresh queries `/localnodes` from configured
//! seed nodes and already-known live nodes, then unions the responses. To cover
//! all datacenters, the initial configuration must include at least one working
//! seed host from every datacenter that should receive traffic.
//!
//! A completed non-empty result atomically replaces [`live_nodes`] using
//! [`ArcSwap`]. A long-running cluster pass may publish bounded atomic unions of
//! newly validated nodes and the last-known-good snapshot, then replace the
//! union after every candidate has a terminal outcome. A fully failed refresh
//! preserves the previous snapshot and the original seeds. Non-cluster seeds
//! are discovery-only until a scoped or fallback response validates a routing
//! snapshot. A conclusive empty scoped result retains the empty routing snapshot
//! and the seeds as future discovery candidates.
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
use std::collections::{HashMap, HashSet, VecDeque};
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
const MIN_DISCOVERY_REFRESH_INTERVAL: Duration = Duration::from_millis(1);
const DNS_LOOKUP_TIMEOUT: Duration = Duration::from_secs(2);
const DISCOVERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DISCOVERY_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const DISCOVERY_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CACHED_DISCOVERY_CLIENTS: usize = 64;
const MAX_DISCOVERY_RESPONSE_BYTES: usize = 1024 * 1024;
// A non-empty valid node string needs at least four JSON bytes after the first
// entry (`,"a"`), plus the array delimiters. No accepted 1 MiB response can
// represent more unique nodes than this. Crossing it while unioning multiple
// cluster responses invalidates the whole pass instead of truncating topology.
const MAX_CLUSTER_PASS_NODES: usize = (MAX_DISCOVERY_RESPONSE_BYTES - 1) / 4;
// For the supported HTTP(S) schemes, converting each shortest possible JSON
// node string to a URL adds less than four response sizes of aggregate syntax
// overhead. Sixteen response sizes leave a conservative margin for URL/IDNA
// normalization while bounding unique multi-response cluster accumulation.
const MAX_CLUSTER_PASS_URL_BYTES: usize = MAX_DISCOVERY_RESPONSE_BYTES * 16;
const MAX_IN_FLIGHT_DNS_LOOKUPS: usize = 8;
const RESERVED_SEED_DNS_LOOKUPS: usize = 2;
const MAX_IN_FLIGHT_LEARNED_DNS_LOOKUPS: usize =
    MAX_IN_FLIGHT_DNS_LOOKUPS - RESERVED_SEED_DNS_LOOKUPS;
const DISCOVERY_SKIP_YIELD_INTERVAL: usize = 256;

fn push_unique_node_with_limits(
    nodes: &mut Vec<Arc<Url>>,
    keys: &mut HashSet<String>,
    url_bytes: &mut usize,
    node: Arc<Url>,
    max_nodes: usize,
    max_url_bytes: usize,
) -> Option<bool> {
    let key = node.as_str();
    if keys.contains(key) {
        return Some(false);
    }
    if nodes.len() >= max_nodes {
        return None;
    }
    let next_url_bytes = url_bytes.checked_add(key.len())?;
    if next_url_bytes > max_url_bytes {
        return None;
    }
    keys.insert(key.to_string());
    nodes.push(node);
    *url_bytes = next_url_bytes;
    Some(true)
}

type ResolveFuture<'a> =
    Pin<Box<dyn Future<Output = std::io::Result<Vec<SocketAddr>>> + Send + 'a>>;

trait DiscoveryResolver: std::fmt::Debug + Send + Sync {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a>;

    fn resolve_seed<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
        self.resolve(host, port)
    }
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

type LookupKey = String;

#[derive(Debug)]
struct RegisteredLookup {
    lookup: Arc<InFlightLookup>,
    is_seed: bool,
}

fn normalize_hostname(host: &str) -> String {
    // DNS names are case-insensitive, but a terminal dot is semantic: `foo`
    // may use a resolver search path while `foo.` is absolute.
    host.to_ascii_lowercase()
}

fn socket_address_with_port(mut address: SocketAddr, port: u16) -> SocketAddr {
    address.set_port(port);
    address
}

#[derive(Debug)]
struct BoundedResolverState {
    lookup_slots: Arc<tokio::sync::Semaphore>,
    in_flight: Mutex<HashMap<LookupKey, RegisteredLookup>>,
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

    fn register(
        self: &Arc<Self>,
        key: LookupKey,
        is_seed: bool,
    ) -> std::io::Result<(Arc<InFlightLookup>, bool)> {
        let mut in_flight = self.in_flight.lock().unwrap();
        if let Some(registered) = in_flight.get(&key) {
            return Ok((registered.lookup.clone(), false));
        }
        if in_flight.len() >= MAX_IN_FLIGHT_DNS_LOOKUPS {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "DNS lookup capacity exhausted",
            ));
        }
        if !is_seed
            && in_flight
                .values()
                .filter(|registered| !registered.is_seed)
                .count()
                >= MAX_IN_FLIGHT_LEARNED_DNS_LOOKUPS
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "DNS lookup capacity reserved for configured seeds",
            ));
        }

        let lookup = Arc::new(InFlightLookup::new());
        in_flight.insert(
            key,
            RegisteredLookup {
                lookup: lookup.clone(),
                is_seed,
            },
        );
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
                .is_some_and(|current| Arc::ptr_eq(&current.lookup, &self.lookup))
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

/// Coalesces normalized hostnames while allowing unrelated names to progress
/// through a small global worker budget. Timed-out callers do not cancel or
/// duplicate underlying OS resolver work, which is not reliably cancellable.
/// Learned-name saturation fails new unique learned names immediately while
/// reserving two slots for configured seeds. Literal IP seeds bypass it.
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

    fn resolve_with_priority<'a>(
        &'a self,
        host: &'a str,
        port: u16,
        is_seed: bool,
    ) -> ResolveFuture<'a> {
        let host = host.to_string();
        let inner = self.inner.clone();
        let state = self.state.clone();
        Box::pin(async move {
            // DNS answers are addresses; callers overwrite their ports after
            // lookup. Coalesce case variants independently of the requested
            // port, while keeping relative and terminal-dot absolute names
            // distinct.
            let key = normalize_hostname(&host);
            let (lookup, should_spawn) = state.register(key.clone(), is_seed)?;
            if should_spawn {
                let task_lookup = lookup.clone();
                let task_state = state.clone();
                // Construct cancellation ownership before spawning. If a
                // runtime is destroyed before the task receives its first
                // poll, dropping the unpolled future still removes the global
                // slot and wakes coalesced waiters.
                let registration = LookupRegistration {
                    state: Arc::downgrade(&task_state),
                    key,
                    lookup: task_lookup.clone(),
                };
                tokio::spawn(async move {
                    let _registration = registration;
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
                            .is_some_and(|current| Arc::ptr_eq(&current.lookup, &task_lookup))
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

impl DiscoveryResolver for BoundedDiscoveryResolver {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
        self.resolve_with_priority(host, port, false)
    }

    fn resolve_seed<'a>(&'a self, host: &'a str, port: u16) -> ResolveFuture<'a> {
        self.resolve_with_priority(host, port, true)
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

#[derive(Debug)]
enum DiscoveryOutcome {
    Found(Vec<Arc<Url>>),
    PartialFound(Vec<Arc<Url>>),
    AuthoritativeEmpty,
    Unavailable,
    Incomplete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TerminalScopeOutcome {
    Empty,
    Unavailable,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct AddressPassKey {
    scope_index: usize,
    candidate: String,
}

#[derive(Clone, Copy, Debug)]
struct AddressAttempt {
    completed: bool,
}

#[derive(Debug, Default)]
struct AddressDiscoveryPass {
    addresses: HashMap<SocketAddr, AddressAttempt>,
    pending_order: VecDeque<SocketAddr>,
    incomplete_count: usize,
    active_address: Option<SocketAddr>,
    saw_empty_response: bool,
    #[cfg(test)]
    queue_probes: usize,
}

impl AddressDiscoveryPass {
    fn reconcile(&mut self, addresses: &[SocketAddr]) {
        // An active address left behind here was canceled by an enclosing
        // scope/global deadline, not its own candidate deadline. Keep it
        // incomplete, but let least-recent ordering choose another identity.
        self.active_address = None;
        let current = addresses.iter().copied().collect::<HashSet<_>>();
        self.addresses
            .retain(|address, _| current.contains(address));
        self.pending_order
            .retain(|address| current.contains(address));
        for address in addresses {
            if !self.addresses.contains_key(address) {
                self.addresses
                    .insert(*address, AddressAttempt { completed: false });
                self.pending_order.push_back(*address);
            }
        }
        self.incomplete_count = self
            .addresses
            .values()
            .filter(|attempt| !attempt.completed)
            .count();
    }

    fn begin_next(&mut self) -> Option<SocketAddr> {
        while let Some(address) = self.pending_order.pop_front() {
            #[cfg(test)]
            {
                self.queue_probes += 1;
            }
            if self
                .addresses
                .get(&address)
                .is_some_and(|attempt| !attempt.completed)
            {
                // Rotate before I/O so outer cancellation advances stable
                // alternatives without an O(N) identity search.
                self.pending_order.push_back(address);
                self.active_address = Some(address);
                return Some(address);
            }
        }
        None
    }

    fn complete_active(&mut self, saw_empty_response: bool) {
        let Some(address) = self.active_address.take() else {
            return;
        };
        if let Some(attempt) = self.addresses.get_mut(&address)
            && !attempt.completed
        {
            attempt.completed = true;
            self.incomplete_count = self.incomplete_count.saturating_sub(1);
        }
        self.saw_empty_response |= saw_empty_response;
    }

    fn completed_outcome(&self) -> Option<DiscoveryOutcome> {
        if self.addresses.is_empty() || self.incomplete_count != 0 {
            return None;
        }
        Some(if self.saw_empty_response {
            DiscoveryOutcome::AuthoritativeEmpty
        } else {
            DiscoveryOutcome::Unavailable
        })
    }
}

#[derive(Debug, Default)]
struct DiscoveryCandidatePass {
    pending_learned: VecDeque<Arc<Url>>,
    pending_seeds: VecDeque<Arc<Url>>,
    active_candidate: Option<(Arc<Url>, bool)>,
    prefer_seed: bool,
}

impl DiscoveryCandidatePass {
    fn new(pending_learned: VecDeque<Arc<Url>>, pending_seeds: VecDeque<Arc<Url>>) -> Self {
        Self {
            pending_learned,
            pending_seeds,
            ..Self::default()
        }
    }

    fn len(&self) -> usize {
        self.pending_learned.len() + self.pending_seeds.len()
    }

    fn is_empty(&self) -> bool {
        self.pending_learned.is_empty() && self.pending_seeds.is_empty()
    }

    fn resume_after_cutoff(&mut self) {
        let Some((candidate, is_seed)) = self.active_candidate.take() else {
            return;
        };
        self.requeue_after_cutoff(candidate, is_seed);
    }

    fn begin_next(&mut self) -> Option<Arc<Url>> {
        let (candidate, is_seed) = if self.prefer_seed {
            self.pending_seeds
                .pop_front()
                .map(|candidate| (candidate, true))
                .or_else(|| {
                    self.pending_learned
                        .pop_front()
                        .map(|candidate| (candidate, false))
                })?
        } else {
            self.pending_learned
                .pop_front()
                .map(|candidate| (candidate, false))
                .or_else(|| {
                    self.pending_seeds
                        .pop_front()
                        .map(|candidate| (candidate, true))
                })?
        };
        self.prefer_seed = false;
        self.active_candidate = Some((candidate.clone(), is_seed));
        Some(candidate)
    }

    fn finish_active(&mut self) -> (Arc<Url>, bool) {
        self.active_candidate
            .take()
            .expect("selected discovery candidate remains active")
    }

    fn requeue_after_cutoff(&mut self, candidate: Arc<Url>, is_seed: bool) {
        if is_seed {
            self.pending_seeds.push_back(candidate);
        } else {
            self.pending_learned.push_back(candidate);
        }
        // A budget-consuming candidate must not monopolize consecutive
        // refreshes while the other recovery source still has work.
        self.prefer_seed = !is_seed;
    }

    fn alternate_after_budget_exhaustion(&mut self, last_was_seed: bool) {
        self.prefer_seed = !last_was_seed;
    }
}

#[derive(Debug, Default)]
struct ScopedDiscoveryPass {
    candidates: DiscoveryCandidatePass,
    saw_empty_response: bool,
}

#[derive(Debug, Default)]
struct ClusterDiscoveryPass {
    candidates: DiscoveryCandidatePass,
    discovered: Vec<Arc<Url>>,
    discovered_keys: HashSet<String>,
    discovered_url_bytes: usize,
    got_response: bool,
    saw_empty_response: bool,
    saw_unavailable: bool,
    overflowed: bool,
    partial_published_count: usize,
}

impl ClusterDiscoveryPass {
    fn merge_response(&mut self, nodes: Vec<Arc<Url>>) {
        self.got_response = true;
        for node in nodes {
            if push_unique_node_with_limits(
                &mut self.discovered,
                &mut self.discovered_keys,
                &mut self.discovered_url_bytes,
                node,
                MAX_CLUSTER_PASS_NODES,
                MAX_CLUSTER_PASS_URL_BYTES,
            )
            .is_none()
            {
                self.overflowed = true;
                return;
            }
        }
    }

    fn incomplete_outcome(&mut self) -> DiscoveryOutcome {
        if self.discovered.len() > self.partial_published_count {
            self.partial_published_count = self.discovered.len();
            DiscoveryOutcome::PartialFound(self.discovered.clone())
        } else {
            DiscoveryOutcome::Incomplete
        }
    }
}

#[derive(Debug, Default)]
struct DiscoveryProgress {
    address_passes: HashMap<AddressPassKey, AddressDiscoveryPass>,
    scoped_passes: HashMap<usize, ScopedDiscoveryPass>,
    cluster_passes: HashMap<usize, ClusterDiscoveryPass>,
    terminal_scope_outcomes: HashMap<usize, TerminalScopeOutcome>,
}

impl DiscoveryProgress {
    fn clear_address_passes(&mut self, scope_index: usize) {
        self.address_passes
            .retain(|key, _| key.scope_index != scope_index);
    }

    fn clear_scope(&mut self, scope_index: usize) {
        self.clear_address_passes(scope_index);
        self.scoped_passes.remove(&scope_index);
        self.cluster_passes.remove(&scope_index);
        self.terminal_scope_outcomes.remove(&scope_index);
    }

    fn clear_scope_and_lower_priorities(&mut self, scope_index: usize) {
        self.address_passes
            .retain(|key, _| key.scope_index < scope_index);
        self.scoped_passes.retain(|index, _| *index < scope_index);
        self.cluster_passes.retain(|index, _| *index < scope_index);
        self.terminal_scope_outcomes.clear();
    }

    fn clear_all(&mut self) {
        self.address_passes.clear();
        self.scoped_passes.clear();
        self.cluster_passes.clear();
        self.terminal_scope_outcomes.clear();
    }
}

#[derive(Clone)]
struct DiscoveryRefresh {
    routing_scope: RoutingScope,
    live_nodes: Arc<ArcSwap<Vec<Arc<Url>>>>,
    published_scope_index: Arc<Mutex<Option<usize>>>,
    seed_urls: Arc<Vec<Arc<Url>>>,
    alternator_scheme: String,
    port: Option<u16>,
    resolver: Arc<dyn DiscoveryResolver>,
    discovery_clients: Arc<Mutex<DiscoveryClientCache>>,
    update_lock: Arc<tokio::sync::Mutex<()>>,
    progress: Arc<Mutex<DiscoveryProgress>>,
}

#[derive(Debug)]
pub struct LiveNodes {
    routing_scope: RoutingScope,
    active_interval: Duration,
    idle_interval: Duration,
    counter: Arc<AtomicUsize>,
    live_nodes: Arc<ArcSwap<Vec<Arc<Url>>>>,
    published_scope_index: Arc<Mutex<Option<usize>>>,
    seed_urls: Arc<Vec<Arc<Url>>>,
    alternator_scheme: String,
    port: Option<u16>,
    resolver: Arc<dyn DiscoveryResolver>,
    discovery_clients: Arc<Mutex<DiscoveryClientCache>>,
    update_lock: Arc<tokio::sync::Mutex<()>>,
    progress: Arc<Mutex<DiscoveryProgress>>,
    last_activity: Arc<Mutex<Instant>>,
    notify: Arc<tokio::sync::Notify>,
    bg_task: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
    discovery_started: AtomicBool,
}

impl LiveNodes {
    pub fn new(config: &crate::config::AlternatorConfig) -> Option<Arc<Self>> {
        let active_interval = config
            .active_interval()
            .unwrap_or(DEFAULT_ACTIVE_REFRESH_INTERVAL)
            .max(MIN_DISCOVERY_REFRESH_INTERVAL);
        let idle_interval = config
            .idle_interval()
            .unwrap_or(DEFAULT_IDLE_REFRESH_INTERVAL)
            .max(MIN_DISCOVERY_REFRESH_INTERVAL);
        let routing_scope = config
            .routing_scope()
            .unwrap_or(RoutingScope::from_cluster());
        let alternator_scheme = config.scheme().unwrap_or("http".to_string());
        let port = config.port();
        let seed_nodes = config.seed_hosts()?;
        if seed_nodes.is_empty() {
            // An explicitly empty list is the established way to disable
            // client-side discovery and load balancing.
            return None;
        }

        let mut seed_urls = Vec::new();
        let mut seen_seeds = HashSet::new();
        let mut seed_url_bytes = 0usize;
        for addr in &seed_nodes {
            let Ok(url) = build_node_url(&alternator_scheme, addr, port) else {
                continue;
            };
            if push_unique_node_with_limits(
                &mut seed_urls,
                &mut seen_seeds,
                &mut seed_url_bytes,
                Arc::new(url),
                MAX_CLUSTER_PASS_NODES,
                MAX_CLUSTER_PASS_URL_BYTES,
            )
            .is_none()
            {
                // Reject an oversized configuration as a whole. Silently
                // truncating seeds could discard the only reachable recovery
                // endpoint and make discovery order-dependent.
                seed_urls.clear();
                break;
            }
        }
        seed_urls.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        // A non-empty but wholly malformed or oversized seed configuration
        // remains an enabled, empty routing plan. Returning `None` here would
        // remove the routing interceptor and silently fall through to the SDK
        // endpoint instead of failing closed.
        let initial_scope_index = routing_scope
            .scope_chain()
            .and_then(|chain| chain.iter().position(|scope| scope.is_cluster()));
        let initial_live_nodes = if initial_scope_index.is_some() {
            seed_urls.clone()
        } else {
            Vec::new()
        };
        let published_scope_index = if initial_live_nodes.is_empty() {
            None
        } else {
            initial_scope_index
        };

        Some(Arc::new(Self {
            routing_scope,
            active_interval,
            idle_interval,
            counter: Arc::new(AtomicUsize::new(0)),
            live_nodes: Arc::new(ArcSwap::from_pointee(initial_live_nodes)),
            published_scope_index: Arc::new(Mutex::new(published_scope_index)),
            seed_urls: Arc::new(seed_urls),
            alternator_scheme,
            port,
            resolver: system_discovery_resolver(),
            discovery_clients: Arc::new(Mutex::new(DiscoveryClientCache::default())),
            update_lock: Arc::new(tokio::sync::Mutex::new(())),
            progress: Arc::new(Mutex::new(DiscoveryProgress::default())),
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
            published_scope_index: self.published_scope_index.clone(),
            seed_urls: self.seed_urls.clone(),
            alternator_scheme: self.alternator_scheme.clone(),
            port: self.port,
            resolver: self.resolver.clone(),
            discovery_clients: self.discovery_clients.clone(),
            update_lock: self.update_lock.clone(),
            progress: self.progress.clone(),
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
                let is_configured_seed = self
                    .seed_urls
                    .iter()
                    .any(|seed| seed.as_str() == node_addr.as_str());
                let lookup = if is_configured_seed {
                    self.resolver.resolve_seed(host, port)
                } else {
                    self.resolver.resolve(host, port)
                };
                let addresses = tokio::time::timeout(DNS_LOOKUP_TIMEOUT, lookup)
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
            // `set_port` preserves IPv6 flowinfo and scope_id. Rebuilding from
            // `IpAddr` would collapse equal link-local addresses on different
            // interfaces into one unusable record.
            .map(|address| socket_address_with_port(address, port))
            .filter(|address| seen.insert(*address))
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
        scope_index: usize,
        scope: &RoutingScope,
        node_addr: &Url,
    ) -> DiscoveryOutcome {
        let url = scope.build_localnodes_url(node_addr.clone());
        let address_key = AddressPassKey {
            scope_index,
            candidate: node_addr.as_str().to_string(),
        };
        let has_address_snapshot = self
            .progress
            .lock()
            .unwrap()
            .address_passes
            .get(&address_key)
            .is_some_and(|pass| !pass.addresses.is_empty());
        let (logical_host, is_domain) = if has_address_snapshot {
            if let Some(pass) = self
                .progress
                .lock()
                .unwrap()
                .address_passes
                .get_mut(&address_key)
            {
                pass.active_address = None;
            }
            match node_addr.host() {
                Some(Host::Domain(host)) => (host.to_string(), true),
                Some(Host::Ipv4(ip)) => (ip.to_string(), false),
                Some(Host::Ipv6(ip)) => (ip.to_string(), false),
                None => return DiscoveryOutcome::Unavailable,
            }
        } else {
            let Some((logical_host, is_domain, addresses)) =
                self.resolve_node_addresses(node_addr).await
            else {
                return DiscoveryOutcome::Unavailable;
            };
            let mut progress = self.progress.lock().unwrap();
            let pass = progress
                .address_passes
                .entry(address_key.clone())
                .or_default();
            // Freeze one complete DNS answer generation until every address
            // has a terminal outcome. Later DNS failures or answer churn cannot
            // erase cached untried alternatives; the next candidate pass
            // resolves a fresh generation.
            pass.reconcile(&addresses);
            if let Some(outcome) = pass.completed_outcome() {
                progress.address_passes.remove(&address_key);
                return outcome;
            }
            (logical_host, is_domain)
        };

        loop {
            // Selection and fairness advancement happen before I/O. An outer
            // cancellation leaves this identity incomplete but least-recently
            // ordered behind other stable records on the next invocation.
            let address = self
                .progress
                .lock()
                .unwrap()
                .address_passes
                .get_mut(&address_key)
                .and_then(AddressDiscoveryPass::begin_next)
                .expect("an incomplete address pass has a next address");
            let Some(client) = self.discovery_client(&logical_host, address, is_domain) else {
                let mut progress = self.progress.lock().unwrap();
                let pass = progress.address_passes.get_mut(&address_key).unwrap();
                pass.complete_active(false);
                if let Some(outcome) = pass.completed_outcome() {
                    progress.address_passes.remove(&address_key);
                    return outcome;
                }
                continue;
            };
            let Ok(response) = client.get(url.clone()).send().await else {
                let mut progress = self.progress.lock().unwrap();
                let pass = progress.address_passes.get_mut(&address_key).unwrap();
                pass.complete_active(false);
                if let Some(outcome) = pass.completed_outcome() {
                    progress.address_passes.remove(&address_key);
                    return outcome;
                }
                continue;
            };
            if !response.status().is_success() {
                // Drain normal error responses so a cached HTTP/1 connection can
                // remain reusable on a later discovery cycle.
                let _ = Self::read_bounded_response(response).await;
                let mut progress = self.progress.lock().unwrap();
                let pass = progress.address_passes.get_mut(&address_key).unwrap();
                pass.complete_active(false);
                if let Some(outcome) = pass.completed_outcome() {
                    progress.address_passes.remove(&address_key);
                    return outcome;
                }
                continue;
            }
            let Some(body) = Self::read_bounded_response(response).await else {
                let mut progress = self.progress.lock().unwrap();
                let pass = progress.address_passes.get_mut(&address_key).unwrap();
                pass.complete_active(false);
                if let Some(outcome) = pass.completed_outcome() {
                    progress.address_passes.remove(&address_key);
                    return outcome;
                }
                continue;
            };
            let Ok(nodes) = serde_json::from_slice::<Vec<String>>(&body) else {
                let mut progress = self.progress.lock().unwrap();
                let pass = progress.address_passes.get_mut(&address_key).unwrap();
                pass.complete_active(false);
                if let Some(outcome) = pass.completed_outcome() {
                    progress.address_passes.remove(&address_key);
                    return outcome;
                }
                continue;
            };
            if nodes.is_empty() {
                let mut progress = self.progress.lock().unwrap();
                let pass = progress.address_passes.get_mut(&address_key).unwrap();
                pass.complete_active(true);
                if let Some(outcome) = pass.completed_outcome() {
                    progress.address_passes.remove(&address_key);
                    return outcome;
                }
                continue;
            }

            let mut valid_nodes = nodes
                .into_iter()
                .filter_map(|addr| self.host_to_uri(&addr).ok().map(Arc::new))
                .collect::<Vec<_>>();
            valid_nodes.sort_by(|left, right| left.as_str().cmp(right.as_str()));
            valid_nodes.dedup_by(|left, right| left.as_str() == right.as_str());
            if !valid_nodes.is_empty() {
                self.progress
                    .lock()
                    .unwrap()
                    .address_passes
                    .remove(&address_key);
                return DiscoveryOutcome::Found(valid_nodes);
            }

            let mut progress = self.progress.lock().unwrap();
            let pass = progress.address_passes.get_mut(&address_key).unwrap();
            pass.complete_active(false);
            if let Some(outcome) = pass.completed_outcome() {
                progress.address_passes.remove(&address_key);
                return outcome;
            }
        }
    }

    async fn fetch_live_nodes_with_terminal_timeout(
        &self,
        scope_index: usize,
        scope: &RoutingScope,
        node_addr: &Url,
        candidate_timeout: Duration,
    ) -> DiscoveryOutcome {
        let address_key = AddressPassKey {
            scope_index,
            candidate: node_addr.as_str().to_string(),
        };
        match tokio::time::timeout(
            candidate_timeout,
            self.fetch_live_nodes_for_scope(scope_index, scope, node_addr),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => {
                let mut progress = self.progress.lock().unwrap();
                let Some(pass) = progress.address_passes.get_mut(&address_key) else {
                    // A full fair candidate window expired in DNS resolution.
                    return DiscoveryOutcome::Unavailable;
                };
                if pass.active_address.is_none() {
                    return DiscoveryOutcome::Incomplete;
                }
                pass.complete_active(false);
                if let Some(outcome) = pass.completed_outcome() {
                    progress.address_passes.remove(&address_key);
                    outcome
                } else {
                    DiscoveryOutcome::Incomplete
                }
            }
        }
    }

    fn discovery_candidate_queues(&self) -> (VecDeque<Arc<Url>>, VecDeque<Arc<Url>>) {
        let seed_keys = self
            .seed_urls
            .iter()
            .map(|seed| seed.as_str())
            .collect::<HashSet<_>>();
        let mut learned = self.live_nodes.load().as_ref().clone();
        learned.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        learned.dedup_by(|left, right| left.as_str() == right.as_str());
        learned.retain(|candidate| !seed_keys.contains(candidate.as_str()));
        (
            learned.into(),
            self.seed_urls.iter().cloned().collect::<VecDeque<_>>(),
        )
    }

    #[cfg(test)]
    fn discovery_candidates(&self) -> Vec<Arc<Url>> {
        let (learned, seeds) = self.discovery_candidate_queues();
        learned.into_iter().chain(seeds).collect()
    }

    async fn discover_cluster_live_nodes(
        &self,
        scope_index: usize,
        candidate_timeout: Duration,
    ) -> DiscoveryOutcome {
        let scope_work_started = Instant::now();
        let scope = RoutingScope::from_cluster();
        {
            let mut progress = self.progress.lock().unwrap();
            progress
                .cluster_passes
                .entry(scope_index)
                .or_insert_with(|| {
                    let (learned, seeds) = self.discovery_candidate_queues();
                    ClusterDiscoveryPass {
                        candidates: DiscoveryCandidatePass::new(learned, seeds),
                        ..ClusterDiscoveryPass::default()
                    }
                });
        }

        let attempt_limit = {
            let mut progress = self.progress.lock().unwrap();
            let pass = progress.cluster_passes.get_mut(&scope_index).unwrap();
            // An active candidate here was canceled by the enclosing scope or
            // refresh. Rotate it behind its source and give the other source
            // the next fair turn.
            pass.candidates.resume_after_cutoff();
            pass.candidates.len()
        };
        let mut skipped_since_yield = 0;
        let mut used_full_candidate_window = false;
        for _ in 0..attempt_limit {
            if scope_work_started.elapsed() >= candidate_timeout {
                break;
            }
            let (node_addr, already_discovered) = {
                let mut progress = self.progress.lock().unwrap();
                let pass = progress.cluster_passes.get_mut(&scope_index).unwrap();
                if pass.overflowed {
                    break;
                }
                let node_addr = pass
                    .candidates
                    .begin_next()
                    .expect("attempt limit tracks pending candidates");
                let already_discovered = pass.discovered_keys.contains(node_addr.as_str());
                (node_addr, already_discovered)
            };

            if already_discovered {
                self.progress
                    .lock()
                    .unwrap()
                    .cluster_passes
                    .get_mut(&scope_index)
                    .unwrap()
                    .candidates
                    .finish_active();
                skipped_since_yield += 1;
                if skipped_since_yield >= DISCOVERY_SKIP_YIELD_INTERVAL {
                    skipped_since_yield = 0;
                    tokio::task::yield_now().await;
                }
                continue;
            }

            let result = if !used_full_candidate_window {
                used_full_candidate_window = true;
                self.fetch_live_nodes_with_terminal_timeout(
                    scope_index,
                    &scope,
                    &node_addr,
                    candidate_timeout,
                )
                .await
            } else {
                let remaining_candidate_budget = candidate_timeout
                    .saturating_sub(scope_work_started.elapsed())
                    .max(Duration::from_nanos(1));
                match tokio::time::timeout(
                    remaining_candidate_budget,
                    self.fetch_live_nodes_for_scope(scope_index, &scope, &node_addr),
                )
                .await
                {
                    Ok(result) => result,
                    // This is only a shared residual cutoff. Active markers
                    // remain incomplete and rotate into a full window later.
                    Err(_) => DiscoveryOutcome::Incomplete,
                }
            };
            let mut progress = self.progress.lock().unwrap();
            let pass = progress.cluster_passes.get_mut(&scope_index).unwrap();
            let (active, active_was_seed) = pass.candidates.finish_active();
            match result {
                DiscoveryOutcome::Found(nodes) => pass.merge_response(nodes),
                DiscoveryOutcome::PartialFound(_) => {
                    unreachable!("address discovery cannot return a partial cluster union")
                }
                DiscoveryOutcome::AuthoritativeEmpty => {
                    pass.got_response = true;
                    pass.saw_empty_response = true;
                }
                DiscoveryOutcome::Unavailable => {
                    pass.saw_unavailable = true;
                }
                DiscoveryOutcome::Incomplete => {
                    pass.candidates
                        .requeue_after_cutoff(active, active_was_seed);
                    return pass.incomplete_outcome();
                }
            }
            if scope_work_started.elapsed() >= candidate_timeout {
                pass.candidates
                    .alternate_after_budget_exhaustion(active_was_seed);
                break;
            }
        }

        let mut progress = self.progress.lock().unwrap();
        let complete = progress
            .cluster_passes
            .get(&scope_index)
            .is_some_and(|pass| pass.overflowed || pass.candidates.is_empty());
        if !complete {
            return progress
                .cluster_passes
                .get_mut(&scope_index)
                .unwrap()
                .incomplete_outcome();
        }
        let mut completed = progress.cluster_passes.remove(&scope_index).unwrap();
        progress.clear_address_passes(scope_index);
        if completed.overflowed {
            return DiscoveryOutcome::Unavailable;
        }
        completed
            .discovered
            .sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        completed
            .discovered
            .dedup_by(|left, right| left.as_str() == right.as_str());
        if !completed.discovered.is_empty() {
            if completed.saw_unavailable || completed.saw_empty_response {
                // A cluster pass is the union of independently discovered
                // datacenters. A non-empty response from one candidate makes
                // neither failures nor an HTTP-200 empty response from another
                // authoritative for the complete topology. Publish a bounded
                // union instead; a pass where every candidate returns nodes can
                // still authoritatively remove stale entries.
                return DiscoveryOutcome::PartialFound(completed.discovered);
            }
            return DiscoveryOutcome::Found(completed.discovered);
        }
        if completed.got_response && !completed.saw_unavailable {
            DiscoveryOutcome::AuthoritativeEmpty
        } else {
            DiscoveryOutcome::Unavailable
        }
    }

    async fn discover_scoped_live_nodes(
        &self,
        scope_index: usize,
        scope: &RoutingScope,
        candidate_timeout: Duration,
    ) -> DiscoveryOutcome {
        let scope_work_started = Instant::now();
        {
            let mut progress = self.progress.lock().unwrap();
            if !progress.scoped_passes.contains_key(&scope_index) {
                progress.clear_address_passes(scope_index);
                progress.scoped_passes.insert(scope_index, {
                    // This is a pass generation snapshot. Topology changes
                    // from lower-priority fallback publication do not reset
                    // it and starve later stable candidates.
                    let (learned, seeds) = self.discovery_candidate_queues();
                    ScopedDiscoveryPass {
                        candidates: DiscoveryCandidatePass::new(learned, seeds),
                        ..ScopedDiscoveryPass::default()
                    }
                });
            }
        }

        let attempt_limit = {
            let mut progress = self.progress.lock().unwrap();
            let pass = progress.scoped_passes.get_mut(&scope_index).unwrap();
            pass.candidates.resume_after_cutoff();
            pass.candidates.len()
        };
        let mut used_full_candidate_window = false;
        for _ in 0..attempt_limit {
            if scope_work_started.elapsed() >= candidate_timeout {
                break;
            }
            let node_addr = {
                let mut progress = self.progress.lock().unwrap();
                let pass = progress.scoped_passes.get_mut(&scope_index).unwrap();
                pass.candidates
                    .begin_next()
                    .expect("attempt limit tracks pending candidates")
            };

            let result = if !used_full_candidate_window {
                used_full_candidate_window = true;
                self.fetch_live_nodes_with_terminal_timeout(
                    scope_index,
                    scope,
                    &node_addr,
                    candidate_timeout,
                )
                .await
            } else {
                let remaining_candidate_budget = candidate_timeout
                    .saturating_sub(scope_work_started.elapsed())
                    .max(Duration::from_nanos(1));
                match tokio::time::timeout(
                    remaining_candidate_budget,
                    self.fetch_live_nodes_for_scope(scope_index, scope, &node_addr),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => DiscoveryOutcome::Incomplete,
                }
            };
            if let DiscoveryOutcome::Found(nodes) = result {
                self.progress.lock().unwrap().clear_scope(scope_index);
                return DiscoveryOutcome::Found(nodes);
            }
            let mut progress = self.progress.lock().unwrap();
            let pass = progress.scoped_passes.get_mut(&scope_index).unwrap();
            let (active, active_was_seed) = pass.candidates.finish_active();
            match result {
                DiscoveryOutcome::AuthoritativeEmpty => pass.saw_empty_response = true,
                DiscoveryOutcome::Unavailable => {}
                DiscoveryOutcome::Incomplete => {
                    pass.candidates
                        .requeue_after_cutoff(active, active_was_seed);
                    return DiscoveryOutcome::Incomplete;
                }
                DiscoveryOutcome::PartialFound(_) => {
                    unreachable!("address discovery cannot return a partial cluster union")
                }
                DiscoveryOutcome::Found(_) => unreachable!("found result returned above"),
            }
            if scope_work_started.elapsed() >= candidate_timeout {
                pass.candidates
                    .alternate_after_budget_exhaustion(active_was_seed);
                break;
            }
        }

        let mut progress = self.progress.lock().unwrap();
        if !progress
            .scoped_passes
            .get(&scope_index)
            .unwrap()
            .candidates
            .is_empty()
        {
            return DiscoveryOutcome::Incomplete;
        }
        let completed = progress.scoped_passes.remove(&scope_index).unwrap();
        progress.clear_address_passes(scope_index);
        if completed.saw_empty_response {
            DiscoveryOutcome::AuthoritativeEmpty
        } else {
            DiscoveryOutcome::Unavailable
        }
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
        self.update_live_nodes_with_timeout(DISCOVERY_REFRESH_TIMEOUT)
            .await;
    }

    fn merge_partial_cluster_snapshot(&self, discovered: Vec<Arc<Url>>) -> Option<Vec<Arc<Url>>> {
        Self::merge_cluster_snapshots_with_limits(
            discovered,
            self.live_nodes.load().iter().cloned(),
            MAX_CLUSTER_PASS_NODES,
            MAX_CLUSTER_PASS_URL_BYTES,
        )
    }

    fn merge_cluster_snapshots_with_limits(
        discovered: Vec<Arc<Url>>,
        current: impl IntoIterator<Item = Arc<Url>>,
        max_nodes: usize,
        max_url_bytes: usize,
    ) -> Option<Vec<Arc<Url>>> {
        let mut merged = Vec::with_capacity(discovered.len().min(max_nodes));
        let mut seen = HashSet::new();
        let mut url_bytes = 0usize;
        for node in discovered {
            if seen.contains(&node) {
                continue;
            }
            if merged.len() >= max_nodes {
                return None;
            }
            url_bytes = url_bytes.checked_add(node.as_str().len())?;
            if url_bytes > max_url_bytes {
                return None;
            }
            seen.insert(node.clone());
            merged.push(node);
        }
        for node in current {
            if seen.contains(&node) {
                continue;
            }
            if merged.len() >= max_nodes {
                break;
            }
            let Some(next_url_bytes) = url_bytes.checked_add(node.as_str().len()) else {
                break;
            };
            if next_url_bytes > max_url_bytes {
                break;
            }
            url_bytes = next_url_bytes;
            seen.insert(node.clone());
            merged.push(node);
        }
        merged.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        Some(merged)
    }

    async fn update_live_nodes_with_timeout(&self, refresh_timeout: Duration) {
        let _ = tokio::time::timeout(
            refresh_timeout,
            self.update_live_nodes_serialized(refresh_timeout),
        )
        .await;
    }

    async fn update_live_nodes_serialized(&self, refresh_timeout: Duration) {
        let _update_guard = self.update_lock.lock().await;
        let Some(scopes) = self.routing_scope.scope_chain() else {
            self.live_nodes.store(Arc::new(Vec::new()));
            *self.published_scope_index.lock().unwrap() = None;
            self.progress.lock().unwrap().clear_all();
            return;
        };
        let divisor = scopes.len();
        let scope_timeout = refresh_timeout
            .checked_div(u32::try_from(divisor).unwrap_or(u32::MAX))
            .unwrap_or(Duration::ZERO)
            .max(Duration::from_nanos(1));
        // Leave a small margin inside each scope slice. For the normal maximum
        // of four scopes, the remaining candidate budget still covers the
        // independent 2s DNS and 5s HTTP timeouts; the margin lets the next
        // fallback scope start before the enclosing refresh deadline.
        let scheduling_margin = scope_timeout
            .checked_div(10)
            .unwrap_or(Duration::ZERO)
            .min(Duration::from_millis(250));
        let candidate_timeout = scope_timeout
            .checked_sub(scheduling_margin)
            .unwrap_or(Duration::ZERO)
            .max(Duration::from_nanos(1));
        let scope_count = scopes.len();

        for (scope_index, scope) in scopes.iter().copied().enumerate() {
            let terminal_outcome = self
                .progress
                .lock()
                .unwrap()
                .terminal_scope_outcomes
                .get(&scope_index)
                .copied();
            let discovery = async {
                if let Some(outcome) = terminal_outcome {
                    match outcome {
                        TerminalScopeOutcome::Empty => DiscoveryOutcome::AuthoritativeEmpty,
                        TerminalScopeOutcome::Unavailable => DiscoveryOutcome::Unavailable,
                    }
                } else if scope.is_cluster() {
                    self.discover_cluster_live_nodes(scope_index, candidate_timeout)
                        .await
                } else {
                    self.discover_scoped_live_nodes(scope_index, scope, candidate_timeout)
                        .await
                }
            };
            let result = match tokio::time::timeout(scope_timeout, discovery).await {
                Ok(result) => result,
                Err(_) => continue,
            };
            match result {
                DiscoveryOutcome::Found(new_nodes) => {
                    if **self.live_nodes.load() != new_nodes {
                        self.live_nodes.store(Arc::new(new_nodes));
                    }
                    *self.published_scope_index.lock().unwrap() = Some(scope_index);
                    // A fallback success invalidates empty latches but must not
                    // discard unfinished higher-priority passes. Clear only
                    // this and lower-priority generations.
                    self.progress
                        .lock()
                        .unwrap()
                        .clear_scope_and_lower_priorities(scope_index);
                    return;
                }
                DiscoveryOutcome::PartialFound(discovered) => {
                    // A cluster pass may span many bounded refreshes when old
                    // learned nodes are blackholed. Publish an atomic union when
                    // newly validated nodes appear so requests can recover while
                    // the persistent pass continues pruning stale candidates.
                    if let Some(new_nodes) = self.merge_partial_cluster_snapshot(discovered) {
                        if **self.live_nodes.load() != new_nodes {
                            self.live_nodes.store(Arc::new(new_nodes));
                        }
                        *self.published_scope_index.lock().unwrap() = Some(scope_index);
                    }
                    return;
                }
                DiscoveryOutcome::AuthoritativeEmpty => {
                    self.progress
                        .lock()
                        .unwrap()
                        .terminal_scope_outcomes
                        .insert(scope_index, TerminalScopeOutcome::Empty);
                }
                DiscoveryOutcome::Unavailable => {
                    self.progress
                        .lock()
                        .unwrap()
                        .terminal_scope_outcomes
                        .insert(scope_index, TerminalScopeOutcome::Unavailable);
                }
                DiscoveryOutcome::Incomplete => {}
            }
        }

        let published_scope_index = *self.published_scope_index.lock().unwrap();
        let published_strict_scope_is_empty = {
            let mut progress = self.progress.lock().unwrap();
            let is_empty = published_scope_index.is_some_and(|scope_index| {
                scopes
                    .get(scope_index)
                    .is_some_and(|scope| !scope.is_cluster())
                    && progress
                        .terminal_scope_outcomes
                        .get(&scope_index)
                        .is_some_and(|outcome| *outcome == TerminalScopeOutcome::Empty)
            });
            if progress.terminal_scope_outcomes.len() == scope_count {
                progress.clear_all();
            }
            is_empty
        };
        if published_strict_scope_is_empty {
            self.live_nodes.store(Arc::new(Vec::new()));
            *self.published_scope_index.lock().unwrap() = None;
        }
    }
}

fn is_usable_domain_name(host: &str) -> bool {
    // `url` has already applied IDNA here, so label and total lengths are DNS
    // wire-format ASCII lengths. Preserve one terminal root dot, but reject an
    // empty root name and empty interior labels.
    let host = host.strip_suffix('.').unwrap_or(host);
    if host.is_empty() || host.len() > 253 {
        return false;
    }

    host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && label
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

fn build_node_url(scheme: &str, addr: &str, port: Option<u16>) -> Result<Url, url::ParseError> {
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return Err(url::ParseError::InvalidDomainCharacter);
    }
    // WHATWG URL parsing removes some ASCII whitespace and decodes percent
    // escapes before exposing the host. Discovery values are host authorities,
    // so accepting either would silently reinterpret malformed node data.
    if addr.contains('%')
        || addr
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(url::ParseError::InvalidDomainCharacter);
    }
    if addr.ends_with(':') && addr.parse::<std::net::Ipv6Addr>().is_err() {
        return Err(url::ParseError::InvalidPort);
    }
    let authority = if addr.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{addr}]")
    } else {
        addr.to_string()
    };
    let mut url = Url::parse(&format!("{scheme}://{authority}"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        // Seed and `/localnodes` values are authorities, not arbitrary URLs.
        // Accepting userinfo, paths, queries, or fragments would turn malformed
        // discovery data into routable nodes and could alter later probes. An
        // input port remains compatible with the established contract below:
        // the configured Alternator port replaces it.
        return Err(url::ParseError::InvalidDomainCharacter);
    }
    if matches!(url.host(), Some(Host::Domain(host)) if !is_usable_domain_name(host)) {
        // `url` accepts several strings that cannot be DNS hostnames (empty
        // labels, underscores, overlong labels, and edge hyphens). Publishing
        // one would stop address fallback even though no usable node was
        // learned from that address.
        return Err(url::ParseError::InvalidDomainCharacter);
    }
    if let Some(Host::Ipv4(ip)) = url.host()
        && addr.parse::<std::net::Ipv4Addr>().ok() != Some(ip)
        && addr
            .parse::<std::net::SocketAddrV4>()
            .ok()
            .is_none_or(|address| *address.ip() != ip)
    {
        // The WHATWG parser accepts legacy shorthand, octal, hexadecimal, and
        // integer IPv4 spellings. Reject those instead of silently routing a
        // discovery value to a canonical address it did not name explicitly.
        return Err(url::ParseError::InvalidIpv4Address);
    }
    url.set_port(port)
        .map_err(|()| url::ParseError::InvalidPort)?;
    Ok(url)
}

#[cfg(test)]
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
    use crate::config::AlternatorConfig;
    use crate::{AlternatorClient, QueryPlan};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
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

    #[derive(Debug)]
    struct SocketResolver {
        addresses: Vec<SocketAddr>,
    }

    impl DiscoveryResolver for SocketResolver {
        fn resolve<'a>(&'a self, _host: &'a str, _port: u16) -> ResolveFuture<'a> {
            let addresses = self.addresses.clone();
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
        Raw(Vec<u8>),
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
        Sleep {
            body: String,
            delay: Duration,
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
                            ServerReply::Raw(body) => {
                                let header = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    body.len()
                                );
                                stream.write_all(header.as_bytes()).await.unwrap();
                                stream.write_all(&body).await.unwrap();
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
                            ServerReply::Sleep { body, delay } => {
                                tokio::time::sleep(delay).await;
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                    body.len()
                                );
                                let _ = stream.write_all(response.as_bytes()).await;
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
    async fn resolver_reserves_global_capacity_for_two_seed_lookups() {
        let learned_entries = (0..=MAX_IN_FLIGHT_LEARNED_DNS_LOOKUPS)
            .map(|index| (format!("pending-{index}.test"), vec![Resolution::Pending]))
            .collect::<Vec<_>>();
        let mut entries = learned_entries
            .iter()
            .map(|(host, answers)| (host.as_str(), answers.clone()))
            .collect::<Vec<_>>();
        entries.extend([
            ("seed-a.test", vec![Resolution::Pending]),
            ("seed-b.test", vec![Resolution::Pending]),
            ("seed-overflow.test", vec![Resolution::Pending]),
        ]);
        let inner = ScriptedResolver::new(entries);
        let resolver = BoundedDiscoveryResolver::new(inner);

        for (host, _) in learned_entries
            .iter()
            .take(MAX_IN_FLIGHT_LEARNED_DNS_LOOKUPS)
        {
            assert!(
                tokio::time::timeout(Duration::from_millis(10), resolver.resolve(host, 8000))
                    .await
                    .is_err()
            );
        }
        let learned_overflow = resolver
            .resolve(&learned_entries[MAX_IN_FLIGHT_LEARNED_DNS_LOOKUPS].0, 8000)
            .await
            .unwrap_err();
        assert_eq!(learned_overflow.kind(), std::io::ErrorKind::WouldBlock);

        for seed in ["seed-a.test", "seed-b.test"] {
            assert!(
                tokio::time::timeout(Duration::from_millis(10), resolver.resolve_seed(seed, 8000),)
                    .await
                    .is_err()
            );
        }
        let overflow = resolver
            .resolve_seed("seed-overflow.test", 8000)
            .await
            .unwrap_err();

        assert_eq!(overflow.kind(), std::io::ErrorKind::WouldBlock);
        assert_eq!(resolver.in_flight_count(), MAX_IN_FLIGHT_DNS_LOOKUPS);
        assert_eq!(resolver.peak_in_flight_count(), MAX_IN_FLIGHT_DNS_LOOKUPS);
    }

    #[tokio::test]
    async fn saturated_learned_dns_and_stalled_seed_leave_next_seed_slot() {
        let learned_hosts = (0..MAX_IN_FLIGHT_LEARNED_DNS_LOOKUPS)
            .map(|index| format!("learned-{index}.test"))
            .collect::<Vec<_>>();
        let healthy_address = Ipv4Addr::new(127, 28, 0, 1);
        let mut entries = learned_hosts
            .iter()
            .map(|host| (host.as_str(), vec![Resolution::Pending]))
            .collect::<Vec<_>>();
        entries.extend([
            ("seed-a.test", vec![Resolution::Pending]),
            (
                "seed-b.test",
                vec![Resolution::Addresses(vec![IpAddr::V4(healthy_address)])],
            ),
        ]);
        let inner = ScriptedResolver::new(entries);
        let resolver = Arc::new(BoundedDiscoveryResolver::new(inner));

        for host in &learned_hosts {
            assert!(
                tokio::time::timeout(Duration::from_millis(10), resolver.resolve(host, 8000))
                    .await
                    .is_err()
            );
        }

        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(["seed-a.test", "seed-b.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver.clone());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                nodes.resolve_node_addresses(&nodes.seed_urls[0]),
            )
            .await
            .is_err()
        );

        let (_, is_domain, addresses) = tokio::time::timeout(
            Duration::from_millis(100),
            nodes.resolve_node_addresses(&nodes.seed_urls[1]),
        )
        .await
        .expect("second retained seed was starved by learned DNS work")
        .expect("second retained seed did not resolve");
        assert!(is_domain);
        assert_eq!(
            addresses,
            vec![SocketAddr::new(IpAddr::V4(healthy_address), 8000)]
        );
        assert_eq!(resolver.peak_in_flight_count(), MAX_IN_FLIGHT_DNS_LOOKUPS);
    }

    #[test]
    fn dropping_runtime_before_resolver_task_poll_releases_global_slot() {
        let address = Ipv4Addr::new(127, 21, 0, 1);
        let inner = ScriptedResolver::new(vec![(
            "entry.test",
            vec![Resolution::Addresses(vec![IpAddr::V4(address)])],
        )]);
        let resolver = Arc::new(BoundedDiscoveryResolver::new(inner));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut lookup = Box::pin(resolver.resolve("entry.test", 8000));
            std::future::poll_fn(|context| {
                assert!(lookup.as_mut().poll(context).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            assert_eq!(resolver.in_flight_count(), 1);
        });
        drop(runtime);

        assert_eq!(resolver.in_flight_count(), 0);
        let retry_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let resolved = retry_runtime
            .block_on(async { resolver.resolve("entry.test", 8000).await })
            .unwrap();
        assert_eq!(resolved, vec![SocketAddr::new(IpAddr::V4(address), 8000)]);
    }

    #[tokio::test]
    async fn resolver_coalesces_case_variants_but_not_terminal_dot_names() {
        let inner = ScriptedResolver::new(vec![
            ("Example.TEST.", vec![Resolution::Pending]),
            ("example.test", vec![Resolution::Pending]),
        ]);
        let resolver = BoundedDiscoveryResolver::new(inner.clone());

        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                resolver.resolve("Example.TEST.", 8000),
            )
            .await
            .is_err()
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                resolver.resolve("example.test.", 9000),
            )
            .await
            .is_err()
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                resolver.resolve("example.test", 9000),
            )
            .await
            .is_err()
        );

        assert_eq!(inner.calls_for("Example.TEST."), 1);
        assert_eq!(inner.calls_for("example.test."), 0);
        assert_eq!(inner.calls_for("example.test"), 1);
        assert_eq!(resolver.in_flight_count(), 2);
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

    #[test]
    fn zero_refresh_intervals_are_clamped_away_from_a_busy_loop() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://127.0.0.1:1")
            .active_interval(Duration::ZERO)
            .idle_interval(Duration::ZERO)
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        assert_eq!(nodes.active_interval, MIN_DISCOVERY_REFRESH_INTERVAL);
        assert_eq!(nodes.idle_interval, MIN_DISCOVERY_REFRESH_INTERVAL);
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

    #[test]
    fn node_authorities_require_usable_dns_hosts_without_losing_compatibility() {
        let accepted = [
            ("localhost", "localhost"),
            ("node-a.example.", "node-a.example."),
            ("bücher.example", "xn--bcher-kva.example"),
            ("127.0.0.1", "127.0.0.1"),
            ("::1", "[::1]"),
            ("[::1]", "[::1]"),
            ("node.example:9000", "node.example"),
        ];
        for (authority, expected_host) in accepted {
            let url = build_node_url("http", authority, Some(8000)).unwrap();
            assert_eq!(url.host_str(), Some(expected_host), "authority {authority}");
            assert_eq!(url.port(), Some(8000), "authority {authority}");
        }
        assert_eq!(
            build_node_url("HTTP", "localhost", Some(8000))
                .unwrap()
                .scheme(),
            "http"
        );

        let overlong_label = format!("{}.example", "a".repeat(64));
        let overlong_name = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(62)
        );
        let mut rejected = vec![
            "".to_string(),
            ".".to_string(),
            "bad..name".to_string(),
            "-bad.example".to_string(),
            "bad-.example".to_string(),
            "bad_name.example".to_string(),
            "[not-an-ip]".to_string(),
            "%65xample.test".to_string(),
            " space.test".to_string(),
            "line\nbreak.test".to_string(),
            "127.1".to_string(),
            "2130706433".to_string(),
            "0x7f000001".to_string(),
            "0177.0.0.1".to_string(),
            "127.0.0.1:".to_string(),
        ];
        rejected.extend([overlong_label, overlong_name]);
        for authority in rejected {
            assert!(
                build_node_url("http", &authority, Some(8000)).is_err(),
                "accepted unusable authority {authority:?}"
            );
        }
        for scheme in ["ftp", "file", "http+unix", ""] {
            assert!(
                build_node_url(scheme, "localhost", Some(8000)).is_err(),
                "accepted unsupported discovery scheme {scheme:?}"
            );
        }
    }

    #[test]
    fn exhausted_affinity_and_preferred_plans_observe_recovered_topology() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://old-node.test:8000")
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        // This is a synchronous plan-state test; suppress lazy background
        // startup when the plan reads the topology.
        nodes.discovery_started.store(true, Ordering::Release);
        let old_node = nodes.live_nodes.load()[0].clone();
        let recovered_node = Arc::new(Url::parse("http://recovered-node.test:8000/").unwrap());

        let affinity = QueryPlan::new_with_hash(nodes.clone(), 42);
        assert_eq!(affinity.next_node().as_ref(), Some(&old_node));
        assert!(affinity.next_node().is_none());
        nodes
            .live_nodes
            .store(Arc::new(vec![recovered_node.clone()]));
        assert_eq!(affinity.next_node().as_ref(), Some(&recovered_node));
        assert!(affinity.next_node().is_none());

        nodes.live_nodes.store(Arc::new(vec![old_node.clone()]));
        let preferred = QueryPlan::new_with_preferred_nodes(nodes.clone(), vec![old_node.clone()]);
        assert_eq!(preferred.next_node().as_ref(), Some(&old_node));
        assert!(preferred.next_node().is_none());
        nodes
            .live_nodes
            .store(Arc::new(vec![recovered_node.clone()]));
        assert_eq!(preferred.next_node().as_ref(), Some(&recovered_node));
        assert!(preferred.next_node().is_none());
    }

    #[test]
    fn nonempty_invalid_seed_configuration_fails_closed() {
        let invalid = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://127.0.0.1:8000")
            .seed_hosts(["bad..name", "bad_name.test", "%65xample.test"])
            .build();
        let nodes = LiveNodes::new(&invalid).expect("non-empty seed configuration stays enabled");
        assert!(nodes.seed_urls.is_empty());
        assert!(nodes.live_nodes.load().is_empty());
        assert!(
            nodes
                .get_next_node_round_robin(&std::collections::HashSet::new())
                .is_none(),
            "malformed seeds must not fall through to the SDK endpoint"
        );
        let client = AlternatorClient::from_conf(invalid);
        assert!(
            client.config().live_nodes().is_some(),
            "client construction must keep the fail-closed routing interceptor"
        );

        let disabled = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url("http://127.0.0.1:8000")
            .seed_hosts(Vec::<String>::new())
            .build();
        assert!(LiveNodes::new(&disabled).is_none());
    }

    #[test]
    fn cluster_anywhere_in_valid_chain_authorizes_initial_seeds() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(["seed.test"])
            .routing_scope(
                RoutingScope::from_rack("dc1".to_string(), "rack1".to_string())
                    .with_fallback(RoutingScope::from_datacenter("dc1".to_string()))
                    .with_fallback(RoutingScope::from_cluster()),
            )
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        assert_eq!(
            nodes.live_nodes.load().as_slice(),
            nodes.seed_urls.as_slice()
        );
        assert_eq!(*nodes.published_scope_index.lock().unwrap(), Some(2));
    }

    #[tokio::test]
    async fn overdepth_fallback_chain_fails_closed_without_seed_authorization() {
        let mut scope = RoutingScope::from_datacenter("dc-0".to_string());
        for index in 1..crate::routing_scope::MAX_ROUTING_SCOPE_CHAIN_DEPTH {
            scope = scope.with_fallback(RoutingScope::from_datacenter(format!("dc-{index}")));
        }
        scope = scope.with_fallback(RoutingScope::from_cluster());

        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(["seed.test"])
            .routing_scope(scope)
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        assert_eq!(nodes.seed_urls.len(), 1);
        assert!(nodes.live_nodes.load().is_empty());
        assert_eq!(*nodes.published_scope_index.lock().unwrap(), None);
        nodes.update_live_nodes().await;
        assert!(nodes.live_nodes.load().is_empty());
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
                "trailing-json-data",
                ServerReply::Http {
                    status: 200,
                    body: r#"["poison.test"] trailing"#.to_string(),
                },
            ),
            ("invalid-utf8", ServerReply::Raw(b"[\"bad\xff.test\"]".to_vec())),
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
                "dns-invalid-labels",
                ServerReply::Http {
                    status: 200,
                    body: r#"["bad..name","-bad.test","bad-.test","bad_name.test","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.test",".","%65xample.test"]"#.to_string(),
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
                    r#"["[not-an-ip]","learned.internal",":","learned.internal","user@ignored.test","ignored.test/path","ignored.test?query","ignored.test#fragment"]"#,
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
                        assert!(
                            request_lower
                                .contains("\r\nauthorization: aws4-hmac-sha256 credential=")
                        );
                        assert!(request_lower.contains("signedheaders="));
                        assert!(request_lower.contains("\r\nx-amz-date: "));
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
            .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::for_tests())
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
    async fn repeated_short_refreshes_reach_seventh_address_after_six_stalls() {
        let addresses = (1..=7)
            .map(|last| Ipv4Addr::new(127, 5, 0, last))
            .collect::<Vec<_>>();
        let mut stalls = Vec::new();
        let mut specs = Vec::new();
        for address in addresses.iter().take(6) {
            let entered = Arc::new(Notify::new());
            let disconnected = Arc::new(Notify::new());
            stalls.push((entered.clone(), disconnected.clone()));
            specs.push((
                *address,
                vec![ExpectedReply::for_path(
                    "/localnodes?dc=dc1",
                    ServerReply::WaitForDisconnect {
                        entered,
                        disconnected,
                    },
                )],
            ));
        }
        specs.push((
            addresses[6],
            vec![ExpectedReply::for_path(
                "/localnodes?dc=dc1",
                ServerReply::Http {
                    status: 200,
                    body: r#"["learned.internal"]"#.to_string(),
                },
            )],
        ));
        let (port, servers) = start_address_servers("entry.test", specs).await;
        let answers = addresses
            .iter()
            .copied()
            .map(IpAddr::V4)
            .collect::<Vec<_>>();
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![Resolution::Addresses(answers); 7],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .routing_scope(RoutingScope::from_datacenter("dc1".to_string()))
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);
        let refresh = nodes.refresh_context();

        for _ in 0..7 {
            if !nodes.live_nodes.load().is_empty() {
                break;
            }
            refresh
                .update_live_nodes_with_timeout(Duration::from_millis(300))
                .await;
        }
        assert!(!nodes.live_nodes.load().is_empty());
        for (entered, disconnected) in &stalls {
            tokio::time::timeout(Duration::from_secs(1), entered.notified())
                .await
                .expect("refresh did not attempt the expected stalled address");
            tokio::time::timeout(Duration::from_secs(1), disconnected.notified())
                .await
                .expect("deadline did not cancel the stalled address");
        }
        join_servers(servers).await;

        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("learned.internal")
        );
    }

    #[tokio::test]
    async fn thirty_third_dns_address_is_not_truncated() {
        let addresses = (1..=33)
            .map(|last| Ipv4Addr::new(127, 12, 0, last))
            .collect::<Vec<_>>();
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![(
                addresses[32],
                vec![ExpectedReply::json(r#"["learned.internal"]"#)],
            )],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![Resolution::Addresses(
                addresses.iter().copied().map(IpAddr::V4).collect(),
            )],
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
            Some("learned.internal")
        );
    }

    #[tokio::test]
    async fn address_fairness_survives_adversarial_dns_reordering() {
        let bad_address = Ipv4Addr::new(127, 6, 0, 1);
        let good_address = Ipv4Addr::new(127, 6, 0, 2);
        let entered = Arc::new(Notify::new());
        let disconnected = Arc::new(Notify::new());
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![
                (
                    bad_address,
                    vec![ExpectedReply::for_path(
                        "/localnodes?dc=dc1",
                        ServerReply::WaitForDisconnect {
                            entered: entered.clone(),
                            disconnected: disconnected.clone(),
                        },
                    )],
                ),
                (
                    good_address,
                    vec![ExpectedReply::for_path(
                        "/localnodes?dc=dc1",
                        ServerReply::Http {
                            status: 200,
                            body: r#"["learned.internal"]"#.to_string(),
                        },
                    )],
                ),
            ],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![
                Resolution::Addresses(vec![IpAddr::V4(bad_address), IpAddr::V4(good_address)]),
                // An index cursor would select index one and hit bad again.
                Resolution::Addresses(vec![IpAddr::V4(good_address), IpAddr::V4(bad_address)]),
            ],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .routing_scope(RoutingScope::from_datacenter("dc1".to_string()))
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);
        let refresh = nodes.refresh_context();

        refresh
            .update_live_nodes_with_timeout(Duration::from_millis(300))
            .await;
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), disconnected.notified())
            .await
            .unwrap();
        refresh
            .update_live_nodes_with_timeout(Duration::from_millis(500))
            .await;
        join_servers(servers).await;

        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("learned.internal")
        );
    }

    #[tokio::test]
    async fn cached_address_generation_survives_permanent_reresolution_failure() {
        let addresses = (1..=33)
            .map(|last| Ipv4Addr::new(127, 20, 0, last))
            .collect::<Vec<_>>();
        let bad_address = addresses[0];
        let good_address = addresses[32];
        let entered = Arc::new(Notify::new());
        let disconnected = Arc::new(Notify::new());
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![
                (
                    bad_address,
                    vec![ExpectedReply::for_path(
                        "/localnodes?dc=dc1",
                        ServerReply::WaitForDisconnect {
                            entered: entered.clone(),
                            disconnected: disconnected.clone(),
                        },
                    )],
                ),
                (
                    good_address,
                    vec![ExpectedReply::for_path(
                        "/localnodes?dc=dc1",
                        ServerReply::Http {
                            status: 200,
                            body: r#"["learned.internal"]"#.to_string(),
                        },
                    )],
                ),
            ],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![
                Resolution::Addresses(addresses.iter().copied().map(IpAddr::V4).collect()),
                Resolution::Error(std::io::ErrorKind::TimedOut),
            ],
        )]);
        let scope = RoutingScope::from_datacenter("dc1".to_string());
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .routing_scope(scope.clone())
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver.clone());
        let refresh = nodes.refresh_context();

        assert!(
            tokio::time::timeout(
                Duration::from_millis(800),
                refresh.discover_scoped_live_nodes(0, &scope, Duration::from_secs(5)),
            )
            .await
            .is_err()
        );
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), disconnected.notified())
            .await
            .unwrap();

        let recovered = refresh
            .discover_scoped_live_nodes(0, &scope, Duration::from_secs(2))
            .await;
        join_servers(servers).await;

        let DiscoveryOutcome::Found(recovered) = recovered else {
            panic!("cached untried address did not survive the cutoff");
        };
        assert_eq!(recovered[0].host_str(), Some("learned.internal"));
        assert_eq!(resolver.calls_for("entry.test"), 1);
    }

    #[tokio::test]
    async fn all_configured_seeds_are_retained_and_seed_seventeen_gets_a_turn() {
        let hosts = (0..17)
            .map(|index| format!("seed-{index:02}.test"))
            .collect::<Vec<_>>();
        let addresses = (1..=17)
            .map(|last| Ipv4Addr::new(127, 7, 0, last))
            .collect::<Vec<_>>();
        let mut stalls = Vec::new();
        let mut specs = Vec::new();
        for (index, (host, address)) in hosts.iter().zip(&addresses).enumerate() {
            let reply = if index < 16 {
                let entered = Arc::new(Notify::new());
                let disconnected = Arc::new(Notify::new());
                stalls.push((entered.clone(), disconnected.clone()));
                ServerReply::WaitForDisconnect {
                    entered,
                    disconnected,
                }
            } else {
                ServerReply::Http {
                    status: 200,
                    body: r#"["learned.internal"]"#.to_string(),
                }
            };
            specs.push((
                *address,
                vec![ExpectedReply::for_path("/localnodes?dc=dc1", reply).for_host(host.clone())],
            ));
        }
        let (port, servers) = start_address_servers("unused.test", specs).await;
        let resolver = ScriptedResolver::new(
            hosts
                .iter()
                .zip(&addresses)
                .map(|(host, address)| {
                    (
                        host.as_str(),
                        vec![Resolution::Addresses(vec![IpAddr::V4(*address)])],
                    )
                })
                .collect(),
        );
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(hosts.clone())
            .routing_scope(RoutingScope::from_datacenter("dc1".to_string()))
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);
        let refresh = nodes.refresh_context();
        assert_eq!(nodes.seed_urls.len(), 17);

        for _ in 0..17 {
            if !nodes.live_nodes.load().is_empty() {
                break;
            }
            refresh
                .update_live_nodes_with_timeout(Duration::from_millis(300))
                .await;
        }
        assert!(!nodes.live_nodes.load().is_empty());
        for (entered, disconnected) in &stalls {
            tokio::time::timeout(Duration::from_secs(1), entered.notified())
                .await
                .expect("later seed did not receive a fair candidate turn");
            tokio::time::timeout(Duration::from_secs(1), disconnected.notified())
                .await
                .expect("stalled seed request was not cancelled");
        }
        join_servers(servers).await;

        assert_eq!(nodes.seed_urls.len(), 17);
        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("learned.internal")
        );
    }

    #[tokio::test]
    async fn primary_scope_deadline_still_leaves_time_for_fallback_scope() {
        let address = Ipv4Addr::new(127, 8, 0, 1);
        let entered = Arc::new(Notify::new());
        let disconnected = Arc::new(Notify::new());
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![(
                address,
                vec![
                    ExpectedReply::for_path(
                        "/localnodes?dc=primary",
                        ServerReply::WaitForDisconnect {
                            entered: entered.clone(),
                            disconnected: disconnected.clone(),
                        },
                    ),
                    ExpectedReply::for_path(
                        "/localnodes?dc=fallback",
                        ServerReply::Http {
                            status: 200,
                            body: r#"["fallback.internal"]"#.to_string(),
                        },
                    ),
                ],
            )],
        )
        .await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .routing_scope(
                RoutingScope::from_datacenter("primary".to_string())
                    .with_fallback(RoutingScope::from_datacenter("fallback".to_string())),
            )
            .build();
        let nodes = live_nodes_with_resolver(
            &config,
            Arc::new(StaticResolver {
                addresses: vec![IpAddr::V4(address)],
            }),
        );

        nodes
            .refresh_context()
            .update_live_nodes_with_timeout(Duration::from_millis(900))
            .await;
        tokio::time::timeout(Duration::from_millis(200), entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(200), disconnected.notified())
            .await
            .unwrap();
        join_servers(servers).await;

        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("fallback.internal")
        );
    }

    #[tokio::test]
    async fn same_hostname_address_progress_is_isolated_between_scopes() {
        let primary_address = Ipv4Addr::new(127, 19, 0, 1);
        let fallback_address = Ipv4Addr::new(127, 19, 0, 2);
        let (port, mut servers) = start_address_servers(
            "entry.test",
            vec![
                (
                    primary_address,
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes?dc=primary",
                            ServerReply::Http {
                                status: 200,
                                body: "[]".to_string(),
                            },
                        ),
                        ExpectedReply::for_path(
                            "/localnodes?dc=fallback",
                            ServerReply::WaitForDisconnect {
                                entered: Arc::new(Notify::new()),
                                disconnected: Arc::new(Notify::new()),
                            },
                        ),
                    ],
                ),
                (
                    fallback_address,
                    vec![
                        ExpectedReply::for_path("/localnodes?dc=primary", ServerReply::Reset),
                        ExpectedReply::for_path(
                            "/localnodes?dc=fallback",
                            ServerReply::Http {
                                status: 200,
                                body: r#"["fallback.internal"]"#.to_string(),
                            },
                        ),
                    ],
                ),
            ],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![
                Resolution::Addresses(vec![
                    IpAddr::V4(primary_address),
                    IpAddr::V4(fallback_address),
                ]),
                Resolution::Addresses(vec![
                    IpAddr::V4(fallback_address),
                    IpAddr::V4(primary_address),
                ]),
            ],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .routing_scope(
                RoutingScope::from_datacenter("primary".to_string())
                    .with_fallback(RoutingScope::from_datacenter("fallback".to_string())),
            )
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);

        nodes
            .refresh_context()
            .update_live_nodes_with_timeout(Duration::from_secs(3))
            .await;

        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("fallback.internal")
        );
        // The preferred-address listener intentionally waits for a fallback
        // request that correct scope-isolated progress never makes.
        let preferred_server = servers.remove(0);
        preferred_server.abort();
        let _ = preferred_server.await;
        join_servers(servers).await;
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
        let addresses = (1..=40)
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
    async fn partial_cluster_refresh_preserves_unavailable_datacenter_lkg() {
        let learned_a = Ipv4Addr::new(127, 25, 0, 1);
        let learned_b = Ipv4Addr::new(127, 25, 0, 2);
        let seed_a = Ipv4Addr::new(127, 25, 0, 3);
        let seed_b = Ipv4Addr::new(127, 25, 0, 4);
        let (port, servers) = start_address_servers(
            "unused.test",
            vec![
                (
                    learned_a,
                    vec![
                        ExpectedReply::json(r#"["new-dc-a.test"]"#).for_host(learned_a.to_string()),
                    ],
                ),
                (
                    learned_b,
                    vec![
                        ExpectedReply::for_path("/localnodes", ServerReply::Reset)
                            .for_host(learned_b.to_string()),
                    ],
                ),
                (
                    seed_a,
                    vec![ExpectedReply::json(r#"["new-dc-a.test"]"#).for_host(seed_a.to_string())],
                ),
                (
                    seed_b,
                    vec![
                        ExpectedReply::for_path("/localnodes", ServerReply::Reset)
                            .for_host(seed_b.to_string()),
                    ],
                ),
            ],
        )
        .await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            // Each seed represents one datacenter. DC-A remains discoverable,
            // while DC-B has a transiently unavailable discovery path.
            .seed_hosts([seed_a.to_string(), seed_b.to_string()])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        nodes.live_nodes.store(Arc::new(vec![
            Arc::new(Url::parse(&format!("http://{learned_a}:{port}/")).unwrap()),
            Arc::new(Url::parse(&format!("http://{learned_b}:{port}/")).unwrap()),
        ]));

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        let hosts = nodes
            .live_nodes
            .load()
            .iter()
            .map(|node| node.host_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            hosts,
            vec![
                "127.25.0.1".to_string(),
                "127.25.0.2".to_string(),
                "new-dc-a.test".to_string(),
            ],
            "a partial cluster refresh must atomically union validated nodes with every unavailable datacenter's last-known-good coverage"
        );
        assert!(nodes.progress.lock().unwrap().cluster_passes.is_empty());
    }

    #[tokio::test]
    async fn cluster_http_empty_mixed_with_fresh_merges_last_known_good() {
        let fresh_address = Ipv4Addr::new(127, 27, 0, 1);
        let empty_address = Ipv4Addr::new(127, 27, 0, 2);
        let (port, servers) = start_address_servers(
            "unused.test",
            vec![
                (
                    fresh_address,
                    vec![
                        ExpectedReply::json(r#"["fresh.test"]"#)
                            .for_host(fresh_address.to_string()),
                    ],
                ),
                (
                    empty_address,
                    vec![ExpectedReply::json("[]").for_host(empty_address.to_string())],
                ),
            ],
        )
        .await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts([fresh_address.to_string(), empty_address.to_string()])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        let hosts = nodes
            .live_nodes
            .load()
            .iter()
            .map(|node| node.host_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            hosts,
            vec![
                fresh_address.to_string(),
                empty_address.to_string(),
                "fresh.test".to_string(),
            ]
        );
        assert_eq!(*nodes.published_scope_index.lock().unwrap(), Some(0));
    }

    #[tokio::test]
    async fn exhausted_application_plan_recovers_through_seed_to_a_new_live_node() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = seed_listener.local_addr().unwrap().port();
        let old_listener = TcpListener::bind(format!("127.0.0.2:{port}"))
            .await
            .unwrap();
        let recovered_listener = TcpListener::bind(format!("127.0.0.3:{port}"))
            .await
            .unwrap();

        let seed_server = tokio::spawn(async move {
            for body in [r#"["127.0.0.2"]"#, r#"["127.0.0.3"]"#] {
                let (mut stream, _) = seed_listener.accept().await.unwrap();
                let request = read_request_headers(&mut stream).await;
                assert!(request.starts_with("GET /localnodes HTTP/1.1"));
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let old_server = tokio::spawn(async move {
            let (mut application, _) = old_listener.accept().await.unwrap();
            let request = read_request_headers(&mut application).await;
            assert!(request.starts_with("POST / HTTP/1.1"));
            // Closing without a response exhausts the only node in this
            // operation's original query plan.
            drop(application);

            let (mut discovery, _) = old_listener.accept().await.unwrap();
            let request = read_request_headers(&mut discovery).await;
            assert!(request.starts_with("GET /localnodes HTTP/1.1"));
            drop(discovery);
        });
        let recovered_server = tokio::spawn(async move {
            let (mut stream, _) = recovered_listener.accept().await.unwrap();
            let request = read_request_headers(&mut stream).await;
            assert!(request.starts_with("POST / HTTP/1.1"));
            let body = r#"{"TableNames":[]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });

        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url(format!("http://127.0.0.1:{port}"))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        nodes.discovery_started.store(true, Ordering::Release);
        nodes.update_live_nodes().await;
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("127.0.0.2"));
        let client = AlternatorClient::from_conf_with_live_nodes(config, nodes.clone());

        assert!(client.list_tables().send().await.is_err());
        nodes.update_live_nodes().await;
        assert!(
            nodes
                .live_nodes
                .load()
                .iter()
                .any(|node| node.host_str() == Some("127.0.0.3")),
            "seed recovery did not publish the newly discovered node"
        );
        client.list_tables().send().await.unwrap();

        for server in [seed_server, old_server, recovered_server] {
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .expect("recovery server did not receive its expected requests")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn stalled_learned_nodes_cannot_starve_original_seed_recovery() {
        let stalled_address = Ipv4Addr::new(127, 23, 0, 1);
        let seed_address = Ipv4Addr::new(127, 23, 0, 2);
        let first_entered = Arc::new(Notify::new());
        let first_disconnected = Arc::new(Notify::new());
        let second_entered = Arc::new(Notify::new());
        let second_disconnected = Arc::new(Notify::new());
        let (port, servers) = start_address_servers(
            "unused.test",
            vec![
                (
                    stalled_address,
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes?dc=dc1",
                            ServerReply::WaitForDisconnect {
                                entered: first_entered.clone(),
                                disconnected: first_disconnected.clone(),
                            },
                        )
                        .for_host("learned-a.test"),
                        ExpectedReply::for_path(
                            "/localnodes?dc=dc1",
                            ServerReply::WaitForDisconnect {
                                entered: second_entered.clone(),
                                disconnected: second_disconnected.clone(),
                            },
                        )
                        .for_host("learned-b.test"),
                    ],
                ),
                (
                    seed_address,
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes?dc=dc1",
                            ServerReply::Http {
                                status: 200,
                                body: r#"["recovered.test"]"#.to_string(),
                            },
                        )
                        .for_host("seed.test"),
                    ],
                ),
            ],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![
            (
                "learned-a.test",
                vec![Resolution::Addresses(vec![IpAddr::V4(stalled_address)])],
            ),
            (
                "learned-b.test",
                vec![Resolution::Addresses(vec![IpAddr::V4(stalled_address)])],
            ),
            (
                "seed.test",
                vec![Resolution::Addresses(vec![IpAddr::V4(seed_address)])],
            ),
        ]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["seed.test"])
            .routing_scope(RoutingScope::from_datacenter("dc1".to_string()))
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver.clone());
        nodes.live_nodes.store(Arc::new(vec![
            Arc::new(Url::parse(&format!("http://learned-a.test:{port}/")).unwrap()),
            Arc::new(Url::parse(&format!("http://learned-b.test:{port}/")).unwrap()),
        ]));
        let refresh = nodes.refresh_context();

        refresh
            .update_live_nodes_with_timeout(Duration::from_millis(250))
            .await;
        tokio::time::timeout(Duration::from_millis(200), first_entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(200), first_disconnected.notified())
            .await
            .unwrap();
        refresh
            .update_live_nodes_with_timeout(Duration::from_millis(250))
            .await;

        for server in servers {
            server.abort();
            let _ = server.await;
        }

        assert_eq!(resolver.calls_for("seed.test"), 1);
        assert_eq!(nodes.live_nodes.load().len(), 1);
        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("recovered.test")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), second_entered.notified())
                .await
                .is_err(),
            "another stalled learned node ran before the recovery seed"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), second_disconnected.notified())
                .await
                .is_err(),
            "unexpected second stalled learned-node connection completed"
        );
    }

    #[tokio::test]
    async fn cluster_recovery_publishes_valid_partial_union_before_stale_pass_finishes() {
        let stalled_address = Ipv4Addr::new(127, 24, 0, 1);
        let seed_address = Ipv4Addr::new(127, 24, 0, 2);
        let first_entered = Arc::new(Notify::new());
        let first_disconnected = Arc::new(Notify::new());
        let second_entered = Arc::new(Notify::new());
        let second_disconnected = Arc::new(Notify::new());
        let (port, servers) = start_address_servers(
            "unused.test",
            vec![
                (
                    stalled_address,
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes",
                            ServerReply::WaitForDisconnect {
                                entered: first_entered.clone(),
                                disconnected: first_disconnected.clone(),
                            },
                        )
                        .for_host("learned-a.test"),
                        ExpectedReply::for_path(
                            "/localnodes",
                            ServerReply::WaitForDisconnect {
                                entered: second_entered.clone(),
                                disconnected: second_disconnected.clone(),
                            },
                        )
                        .for_host("learned-b.test"),
                    ],
                ),
                (
                    seed_address,
                    vec![ExpectedReply::json(r#"["recovered.test"]"#).for_host("seed.test")],
                ),
            ],
        )
        .await;
        let resolver = ScriptedResolver::new(vec![
            (
                "learned-a.test",
                vec![Resolution::Addresses(vec![IpAddr::V4(stalled_address)])],
            ),
            (
                "learned-b.test",
                vec![Resolution::Addresses(vec![IpAddr::V4(stalled_address)])],
            ),
            (
                "seed.test",
                vec![Resolution::Addresses(vec![IpAddr::V4(seed_address)])],
            ),
        ]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["seed.test"])
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);
        nodes.live_nodes.store(Arc::new(vec![
            Arc::new(Url::parse(&format!("http://learned-a.test:{port}/")).unwrap()),
            Arc::new(Url::parse(&format!("http://learned-b.test:{port}/")).unwrap()),
        ]));
        let refresh = nodes.refresh_context();

        refresh
            .update_live_nodes_with_timeout(Duration::from_millis(250))
            .await;
        tokio::time::timeout(Duration::from_millis(200), first_entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(200), first_disconnected.notified())
            .await
            .unwrap();
        refresh
            .update_live_nodes_with_timeout(Duration::from_millis(250))
            .await;
        tokio::time::timeout(Duration::from_millis(200), second_entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(200), second_disconnected.notified())
            .await
            .unwrap();

        for server in servers {
            server.abort();
            let _ = server.await;
        }

        assert!(
            nodes
                .live_nodes
                .load()
                .iter()
                .any(|node| node.host_str() == Some("recovered.test"))
        );
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

    async fn assert_authoritative_empty_clears_only_exact_scope_origin(
        routing_scope: RoutingScope,
        primary_path: &str,
        fallback_path: &str,
    ) {
        let address = Ipv4Addr::new(127, 26, 0, 1);
        let found_body = format!(r#"["{address}"]"#);
        let empty = || ServerReply::Http {
            status: 200,
            body: "[]".to_string(),
        };
        let found = || ServerReply::Http {
            status: 200,
            body: found_body.clone(),
        };
        let (port, servers) = start_address_servers(
            "unused.test",
            vec![(
                address,
                vec![
                    ExpectedReply::for_path(primary_path, found()).for_host(address.to_string()),
                    ExpectedReply::for_path(primary_path, empty()).for_host(address.to_string()),
                    ExpectedReply::for_path(fallback_path, ServerReply::Reset)
                        .for_host(address.to_string()),
                    ExpectedReply::for_path(primary_path, empty()).for_host(address.to_string()),
                    ExpectedReply::for_path(fallback_path, found()).for_host(address.to_string()),
                    ExpectedReply::for_path(primary_path, empty()).for_host(address.to_string()),
                    ExpectedReply::for_path(fallback_path, ServerReply::Reset)
                        .for_host(address.to_string()),
                ],
            )],
        )
        .await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts([address.to_string()])
            .routing_scope(routing_scope)
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        nodes.update_live_nodes().await;
        assert_eq!(nodes.live_nodes.load().len(), 1);
        assert_eq!(*nodes.published_scope_index.lock().unwrap(), Some(0));

        nodes.update_live_nodes().await;
        assert!(nodes.live_nodes.load().is_empty());
        assert_eq!(*nodes.published_scope_index.lock().unwrap(), None);

        nodes.update_live_nodes().await;
        assert_eq!(nodes.live_nodes.load().len(), 1);
        assert_eq!(*nodes.published_scope_index.lock().unwrap(), Some(1));

        nodes.update_live_nodes().await;
        join_servers(servers).await;
        assert_eq!(nodes.live_nodes.load().len(), 1);
        assert_eq!(*nodes.published_scope_index.lock().unwrap(), Some(1));
    }

    #[tokio::test]
    async fn datacenter_empty_clears_only_same_datacenter_snapshot() {
        assert_authoritative_empty_clears_only_exact_scope_origin(
            RoutingScope::from_datacenter("primary".to_string())
                .with_fallback(RoutingScope::from_datacenter("fallback".to_string())),
            "/localnodes?dc=primary",
            "/localnodes?dc=fallback",
        )
        .await;
    }

    #[tokio::test]
    async fn rack_empty_clears_only_same_rack_snapshot() {
        assert_authoritative_empty_clears_only_exact_scope_origin(
            RoutingScope::from_rack("dc1".to_string(), "primary".to_string()).with_fallback(
                RoutingScope::from_rack("dc1".to_string(), "fallback".to_string()),
            ),
            "/localnodes?dc=dc1&rack=primary",
            "/localnodes?dc=dc1&rack=fallback",
        )
        .await;
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
    async fn scoped_empty_pass_survives_a_scope_cutoff() {
        let addresses = (1..=3)
            .map(|last| Ipv4Addr::new(127, 15, 0, last))
            .collect::<Vec<_>>();
        let entered = Arc::new(Notify::new());
        let disconnected = Arc::new(Notify::new());
        let (port, servers) = start_address_servers(
            "unused.test",
            vec![
                (
                    addresses[0],
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes?dc=missing",
                            ServerReply::Http {
                                status: 200,
                                body: "[]".to_string(),
                            },
                        )
                        .for_host(addresses[0].to_string()),
                    ],
                ),
                (
                    addresses[1],
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes?dc=missing",
                            ServerReply::WaitForDisconnect {
                                entered: entered.clone(),
                                disconnected: disconnected.clone(),
                            },
                        )
                        .for_host(addresses[1].to_string()),
                        ExpectedReply::for_path(
                            "/localnodes?dc=missing",
                            ServerReply::Http {
                                status: 200,
                                body: "[]".to_string(),
                            },
                        )
                        .for_host(addresses[1].to_string()),
                    ],
                ),
                (
                    addresses[2],
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes?dc=missing",
                            ServerReply::Http {
                                status: 200,
                                body: "[]".to_string(),
                            },
                        )
                        .for_host(addresses[2].to_string()),
                    ],
                ),
            ],
        )
        .await;
        let scope = RoutingScope::from_datacenter("missing".to_string());
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(addresses.iter().map(ToString::to_string))
            .routing_scope(scope.clone())
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        let retained = nodes.seed_urls[0].clone();
        nodes.live_nodes.store(Arc::new(vec![retained.clone()]));
        *nodes.published_scope_index.lock().unwrap() = Some(0);
        let refresh = nodes.refresh_context();

        assert!(
            tokio::time::timeout(
                Duration::from_millis(800),
                refresh.discover_scoped_live_nodes(0, &scope, Duration::from_secs(5)),
            )
            .await
            .is_err()
        );
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), disconnected.notified())
            .await
            .unwrap();
        assert_eq!(nodes.live_nodes.load().as_slice(), &[retained]);

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        assert!(nodes.live_nodes.load().is_empty());
        assert!(nodes.progress.lock().unwrap().scoped_passes.is_empty());
        assert!(nodes.progress.lock().unwrap().address_passes.is_empty());
    }

    #[tokio::test]
    async fn staggered_scope_empty_results_latch_across_refreshes() {
        let first_address = Ipv4Addr::new(127, 16, 0, 1);
        let second_address = Ipv4Addr::new(127, 16, 0, 2);
        let entered = Arc::new(Notify::new());
        let disconnected = Arc::new(Notify::new());
        let (port, servers) = start_address_servers(
            "unused.test",
            vec![
                (
                    first_address,
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes?dc=primary",
                            ServerReply::Http {
                                status: 200,
                                body: "[]".to_string(),
                            },
                        )
                        .for_host(first_address.to_string()),
                        ExpectedReply::for_path(
                            "/localnodes?dc=fallback",
                            ServerReply::Http {
                                status: 200,
                                body: "[]".to_string(),
                            },
                        )
                        .for_host(first_address.to_string()),
                    ],
                ),
                (
                    second_address,
                    vec![
                        ExpectedReply::for_path("/localnodes?dc=primary", ServerReply::Reset)
                            .for_host(second_address.to_string()),
                        ExpectedReply::for_path(
                            "/localnodes?dc=fallback",
                            ServerReply::WaitForDisconnect {
                                entered: entered.clone(),
                                disconnected: disconnected.clone(),
                            },
                        )
                        .for_host(second_address.to_string()),
                        ExpectedReply::for_path(
                            "/localnodes?dc=fallback",
                            ServerReply::Http {
                                status: 200,
                                body: "[]".to_string(),
                            },
                        )
                        .for_host(second_address.to_string()),
                    ],
                ),
            ],
        )
        .await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts([first_address.to_string(), second_address.to_string()])
            .routing_scope(
                RoutingScope::from_datacenter("primary".to_string())
                    .with_fallback(RoutingScope::from_datacenter("fallback".to_string())),
            )
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        let retained = nodes.seed_urls[0].clone();
        nodes.live_nodes.store(Arc::new(vec![retained.clone()]));
        *nodes.published_scope_index.lock().unwrap() = Some(0);
        let refresh = nodes.refresh_context();

        refresh
            .update_live_nodes_with_timeout(Duration::from_secs(3))
            .await;
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), disconnected.notified())
            .await
            .unwrap();
        assert!(
            nodes.live_nodes.load().is_empty(),
            "the exact primary-scope snapshot must clear once that scope is authoritatively empty"
        );
        assert!(
            nodes
                .progress
                .lock()
                .unwrap()
                .terminal_scope_outcomes
                .get(&0)
                .is_some_and(|outcome| *outcome == TerminalScopeOutcome::Empty)
        );

        refresh
            .update_live_nodes_with_timeout(Duration::from_secs(3))
            .await;
        join_servers(servers).await;

        assert!(nodes.live_nodes.load().is_empty());
        assert!(
            nodes
                .progress
                .lock()
                .unwrap()
                .terminal_scope_outcomes
                .is_empty()
        );
    }

    #[tokio::test]
    async fn cluster_fallback_empty_plus_failure_is_unavailable() {
        let first_address = Ipv4Addr::new(127, 17, 0, 1);
        let second_address = Ipv4Addr::new(127, 17, 0, 2);
        let (port, servers) = start_address_servers(
            "unused.test",
            vec![
                (
                    first_address,
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes?dc=missing",
                            ServerReply::Http {
                                status: 200,
                                body: "[]".to_string(),
                            },
                        )
                        .for_host(first_address.to_string()),
                        ExpectedReply::json("[]").for_host(first_address.to_string()),
                    ],
                ),
                (
                    second_address,
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes?dc=missing",
                            ServerReply::Http {
                                status: 200,
                                body: "[]".to_string(),
                            },
                        )
                        .for_host(second_address.to_string()),
                        ExpectedReply::for_path("/localnodes", ServerReply::Reset)
                            .for_host(second_address.to_string()),
                    ],
                ),
            ],
        )
        .await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts([first_address.to_string(), second_address.to_string()])
            .routing_scope(
                RoutingScope::from_datacenter("missing".to_string())
                    .with_fallback(RoutingScope::from_cluster()),
            )
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        let retained = nodes.seed_urls[0].clone();
        nodes.live_nodes.store(Arc::new(vec![retained.clone()]));

        nodes.update_live_nodes().await;
        join_servers(servers).await;

        assert_eq!(nodes.live_nodes.load().as_slice(), &[retained]);
        assert!(
            nodes
                .progress
                .lock()
                .unwrap()
                .terminal_scope_outcomes
                .is_empty()
        );
    }

    #[tokio::test]
    async fn fallback_chain_allows_slow_dns_plus_delayed_valid_http() {
        let address = Ipv4Addr::new(127, 18, 0, 1);
        let (port, servers) = start_address_servers(
            "entry.test",
            vec![(
                address,
                vec![
                    ExpectedReply::for_path(
                        "/localnodes?dc=primary",
                        ServerReply::Http {
                            status: 200,
                            body: "[]".to_string(),
                        },
                    ),
                    ExpectedReply::for_path(
                        "/localnodes?dc=secondary",
                        ServerReply::Http {
                            status: 200,
                            body: "[]".to_string(),
                        },
                    ),
                    ExpectedReply::for_path(
                        "/localnodes",
                        ServerReply::Sleep {
                            body: r#"["slow-valid.internal"]"#.to_string(),
                            delay: Duration::from_millis(300),
                        },
                    ),
                ],
            )],
        )
        .await;
        let dns_release = Arc::new(Notify::new());
        let resolver = ScriptedResolver::new(vec![(
            "entry.test",
            vec![
                Resolution::Addresses(vec![IpAddr::V4(address)]),
                Resolution::Addresses(vec![IpAddr::V4(address)]),
                Resolution::Delayed {
                    addresses: vec![IpAddr::V4(address)],
                    release: dns_release.clone(),
                },
            ],
        )]);
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(["entry.test"])
            .routing_scope(
                RoutingScope::from_datacenter("primary".to_string()).with_fallback(
                    RoutingScope::from_datacenter("secondary".to_string())
                        .with_fallback(RoutingScope::from_cluster()),
                ),
            )
            .build();
        let nodes = live_nodes_with_resolver(&config, resolver);
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            dns_release.notify_one();
        });

        nodes
            .refresh_context()
            .update_live_nodes_with_timeout(Duration::from_millis(4500))
            .await;
        release.await.unwrap();
        join_servers(servers).await;

        assert_eq!(
            nodes.live_nodes.load()[0].host_str(),
            Some("slow-valid.internal")
        );
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
    async fn explicit_cluster_fallback_authorizes_seed_before_discovery() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let application_posts = Arc::new(AtomicUsize::new(0));
        let task_posts = application_posts.clone();
        let server = tokio::spawn(async move {
            for expected in ["application", "scoped", "cluster", "application"] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request_headers(&mut stream).await;
                let (content_type, body) = match expected {
                    "scoped" => {
                        assert!(request.starts_with("GET /localnodes?dc=missing HTTP/1.1"));
                        ("application/json", "[]".to_string())
                    }
                    "cluster" => {
                        assert!(request.starts_with("GET /localnodes HTTP/1.1"));
                        ("application/json", format!(r#"["{}"]"#, address.ip()))
                    }
                    "application" => {
                        assert!(request.starts_with("POST / HTTP/1.1"));
                        task_posts.fetch_add(1, Ordering::SeqCst);
                        (
                            "application/x-amz-json-1.0",
                            r#"{"TableNames":[]}"#.to_string(),
                        )
                    }
                    _ => unreachable!(),
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .endpoint_url(format!("http://{address}"))
            .routing_scope(
                RoutingScope::from_datacenter("missing".to_string())
                    .with_fallback(RoutingScope::from_cluster()),
            )
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        assert_eq!(
            nodes.live_nodes.load().as_slice(),
            nodes.seed_urls.as_slice()
        );
        assert_eq!(*nodes.published_scope_index.lock().unwrap(), Some(1));
        nodes.discovery_started.store(true, Ordering::Release);
        let client = AlternatorClient::from_conf_with_live_nodes(config, nodes.clone());

        client.list_tables().send().await.unwrap();
        assert_eq!(application_posts.load(Ordering::SeqCst), 1);

        nodes.update_live_nodes().await;
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("127.0.0.1"));
        client.list_tables().send().await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server did not receive fallback discovery and application requests")
            .unwrap();
        assert_eq!(application_posts.load(Ordering::SeqCst), 2);
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
    async fn resolved_address_list_is_deduplicated_without_truncation() {
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

        assert_eq!(addresses.len(), unique_addresses.len());
        assert_eq!(addresses[0].ip(), unique_addresses[0]);
        assert_eq!(addresses[1].ip(), unique_addresses[1]);
    }

    #[tokio::test]
    async fn resolved_ipv6_addresses_preserve_flowinfo_and_scope_id() {
        let ip = "fe80::1".parse::<Ipv6Addr>().unwrap();
        let first = SocketAddr::V6(SocketAddrV6::new(ip, 1234, 7, 11));
        let second = SocketAddr::V6(SocketAddrV6::new(ip, 5678, 9, 22));
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(["entry.test"])
            .build();
        let nodes = live_nodes_with_resolver(
            &config,
            Arc::new(SocketResolver {
                addresses: vec![first, second],
            }),
        );

        let (_, _, addresses) = nodes
            .resolve_node_addresses(&nodes.seed_urls[0])
            .await
            .unwrap();

        assert_eq!(addresses.len(), 2);
        assert_eq!(
            addresses[0],
            SocketAddr::V6(SocketAddrV6::new(ip, 8000, 7, 11))
        );
        assert_eq!(
            addresses[1],
            SocketAddr::V6(SocketAddrV6::new(ip, 8000, 9, 22))
        );
    }

    #[test]
    fn configured_seeds_candidates_and_cluster_union_are_not_silently_truncated() {
        let seeds = (0..80)
            .map(|index| format!("seed-{index}.test"))
            .collect::<Vec<_>>();
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(seeds.clone())
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        assert_eq!(nodes.seed_urls.len(), seeds.len());

        let learned = (0..1024)
            .map(|index| {
                Arc::new(Url::parse(&format!("http://learned-{index}.test:8000/")).unwrap())
            })
            .collect::<Vec<_>>();
        nodes.live_nodes.store(Arc::new(learned.clone()));
        let candidates = nodes.refresh_context().discovery_candidates();
        assert_eq!(candidates.len(), seeds.len() + learned.len());
        for seed in nodes.seed_urls.iter() {
            assert!(node_is_in_list(seed, &candidates));
        }

        let mut pass = ClusterDiscoveryPass::default();
        for _ in 0..16 {
            pass.merge_response(learned.clone());
        }
        assert_eq!(pass.discovered.len(), learned.len());
        assert_eq!(pass.discovered_keys.len(), learned.len());
        assert!(pass.discovered_url_bytes <= MAX_CLUSTER_PASS_URL_BYTES);
        assert!(!pass.overflowed);
        assert!(MAX_CLUSTER_PASS_NODES > learned.len());
        assert_eq!(DISCOVERY_REFRESH_TIMEOUT, Duration::from_secs(30));
    }

    #[test]
    fn seed_and_topology_accumulator_rejects_instead_of_truncating_overflow() {
        let first = Arc::new(Url::parse("http://first.test:8000/").unwrap());
        let second = Arc::new(Url::parse("http://second.test:8000/").unwrap());
        let third = Arc::new(Url::parse("http://third.test:8000/").unwrap());
        let mut nodes = Vec::new();
        let mut keys = HashSet::new();
        let mut url_bytes = 0usize;

        assert_eq!(
            push_unique_node_with_limits(
                &mut nodes,
                &mut keys,
                &mut url_bytes,
                first.clone(),
                2,
                1_000,
            ),
            Some(true)
        );
        assert_eq!(
            push_unique_node_with_limits(&mut nodes, &mut keys, &mut url_bytes, first, 2, 1_000,),
            Some(false),
            "duplicates must not consume a configured-seed slot"
        );
        assert_eq!(
            push_unique_node_with_limits(&mut nodes, &mut keys, &mut url_bytes, second, 2, 1_000,),
            Some(true)
        );
        assert_eq!(
            push_unique_node_with_limits(
                &mut nodes,
                &mut keys,
                &mut url_bytes,
                third.clone(),
                2,
                1_000,
            ),
            None
        );
        assert_eq!(nodes.len(), 2, "overflow must not publish a prefix");

        let existing_bytes = url_bytes;
        assert_eq!(
            push_unique_node_with_limits(
                &mut nodes,
                &mut keys,
                &mut url_bytes,
                third,
                3,
                existing_bytes,
            ),
            None
        );
        assert_eq!(url_bytes, existing_bytes);
        assert_eq!(nodes.len(), 2);
    }

    #[tokio::test]
    async fn repeated_large_cluster_responses_publish_complete_topology_atomically() {
        let learned = (0..300)
            .map(|index| format!("node-{index:03}.test"))
            .collect::<Vec<_>>();
        let body = serde_json::to_string(&learned).unwrap();
        let addresses = (1..=4)
            .map(|last| Ipv4Addr::new(127, 9, 0, last))
            .collect::<Vec<_>>();
        let final_entered = Arc::new(Notify::new());
        let final_release = Arc::new(Notify::new());
        let specs = addresses
            .iter()
            .enumerate()
            .map(|(index, address)| {
                let reply = if index + 1 == addresses.len() {
                    ServerReply::Delayed {
                        body: body.clone(),
                        entered: final_entered.clone(),
                        release: final_release.clone(),
                    }
                } else {
                    ServerReply::Http {
                        status: 200,
                        body: body.clone(),
                    }
                };
                (
                    *address,
                    vec![
                        ExpectedReply::for_path("/localnodes", reply).for_host(address.to_string()),
                    ],
                )
            })
            .collect();
        let (port, servers) = start_address_servers("unused.test", specs).await;
        let seed_hosts = addresses
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts(seed_hosts)
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        let update_nodes = nodes.clone();
        let update = tokio::spawn(async move { update_nodes.update_live_nodes().await });

        tokio::time::timeout(Duration::from_secs(1), final_entered.notified())
            .await
            .expect("cluster pass did not reach its final duplicate response");
        assert_eq!(
            nodes.live_nodes.load().len(),
            addresses.len(),
            "partial cluster pass became visible before atomic completion"
        );
        {
            let progress = nodes.progress.lock().unwrap();
            let pass = progress.cluster_passes.get(&0).unwrap();
            assert_eq!(pass.discovered.len(), learned.len());
            assert_eq!(pass.discovered_keys.len(), learned.len());
            assert!(pass.discovered_url_bytes <= MAX_CLUSTER_PASS_URL_BYTES);
            assert!(!pass.overflowed);
        }

        final_release.notify_one();
        update.await.unwrap();
        join_servers(servers).await;

        let snapshot = nodes.live_nodes.load();
        assert_eq!(snapshot.len(), learned.len());
        assert!(snapshot.iter().all(|node| {
            node.host_str()
                .is_some_and(|host| host.starts_with("node-") && host.ends_with(".test"))
        }));
    }

    #[tokio::test]
    async fn cluster_pass_retries_deadline_cutoff_before_atomic_publication() {
        let first_address = Ipv4Addr::new(127, 10, 0, 1);
        let cutoff_address = Ipv4Addr::new(127, 10, 0, 2);
        let entered = Arc::new(Notify::new());
        let disconnected = Arc::new(Notify::new());
        let (port, servers) = start_address_servers(
            "unused.test",
            vec![
                (
                    first_address,
                    vec![
                        ExpectedReply::json(r#"["node-a.test"]"#)
                            .for_host(first_address.to_string()),
                    ],
                ),
                (
                    cutoff_address,
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes",
                            ServerReply::WaitForDisconnect {
                                entered: entered.clone(),
                                disconnected: disconnected.clone(),
                            },
                        )
                        .for_host(cutoff_address.to_string()),
                        ExpectedReply::json(r#"["node-b.test"]"#)
                            .for_host(cutoff_address.to_string()),
                    ],
                ),
            ],
        )
        .await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts([first_address.to_string(), cutoff_address.to_string()])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        let refresh = nodes.refresh_context();

        assert!(
            tokio::time::timeout(
                Duration::from_millis(80),
                refresh.discover_cluster_live_nodes(0, Duration::from_secs(1)),
            )
            .await
            .is_err()
        );
        tokio::time::timeout(Duration::from_millis(200), entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(200), disconnected.notified())
            .await
            .unwrap();
        assert_eq!(nodes.live_nodes.load().len(), 2);
        assert!(nodes.live_nodes.load().iter().all(|node| {
            node.host_str()
                .is_some_and(|host| host.starts_with("127.10."))
        }));

        let discovered = refresh
            .discover_cluster_live_nodes(0, Duration::from_millis(200))
            .await;
        join_servers(servers).await;

        let DiscoveryOutcome::Found(discovered) = discovered else {
            panic!("cluster pass did not return its complete union");
        };
        assert_eq!(discovered.len(), 2);
        assert_eq!(discovered[0].host_str(), Some("node-a.test"));
        assert_eq!(discovered[1].host_str(), Some("node-b.test"));
    }

    #[tokio::test]
    async fn explicit_candidate_deadline_finishes_cluster_pass_with_reachable_union() {
        let stalled_address = Ipv4Addr::new(127, 11, 0, 1);
        let reachable_address = Ipv4Addr::new(127, 11, 0, 2);
        let entered = Arc::new(Notify::new());
        let disconnected = Arc::new(Notify::new());
        let second_entered = Arc::new(Notify::new());
        let (port, mut servers) = start_address_servers(
            "unused.test",
            vec![
                (
                    stalled_address,
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes",
                            ServerReply::WaitForDisconnect {
                                entered: entered.clone(),
                                disconnected: disconnected.clone(),
                            },
                        )
                        .for_host(stalled_address.to_string()),
                        ExpectedReply::for_path(
                            "/localnodes",
                            ServerReply::WaitForDisconnect {
                                entered: second_entered.clone(),
                                disconnected: Arc::new(Notify::new()),
                            },
                        )
                        .for_host(stalled_address.to_string()),
                    ],
                ),
                (
                    reachable_address,
                    vec![
                        ExpectedReply::json(r#"["reachable.test"]"#)
                            .for_host(reachable_address.to_string()),
                    ],
                ),
            ],
        )
        .await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts([stalled_address.to_string(), reachable_address.to_string()])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        let refresh = nodes.refresh_context();

        refresh
            .update_live_nodes_with_timeout(Duration::from_millis(150))
            .await;
        tokio::time::timeout(Duration::from_millis(200), entered.notified())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_millis(200), disconnected.notified())
            .await
            .unwrap();
        refresh
            .update_live_nodes_with_timeout(Duration::from_millis(500))
            .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), second_entered.notified())
                .await
                .is_err(),
            "terminal full-window timeout retried a repeat-accepting blackhole"
        );
        let stalled_server = servers.remove(0);
        stalled_server.abort();
        let _ = stalled_server.await;
        join_servers(servers).await;

        assert_eq!(nodes.live_nodes.load().len(), 3);
        assert!(
            nodes
                .live_nodes
                .load()
                .iter()
                .any(|node| node.host_str() == Some("reachable.test")),
            "the reachable candidate was not merged into the retained snapshot"
        );
    }

    #[tokio::test]
    async fn residual_scope_cutoff_keeps_tail_candidate_for_a_full_window() {
        let slow_failure = Ipv4Addr::new(127, 22, 0, 1);
        let tail_success = Ipv4Addr::new(127, 22, 0, 2);
        let (port, servers) = start_address_servers(
            "unused.test",
            vec![
                (
                    slow_failure,
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes",
                            ServerReply::Sleep {
                                body: "not-json".to_string(),
                                delay: Duration::from_millis(1500),
                            },
                        )
                        .for_host(slow_failure.to_string()),
                    ],
                ),
                (
                    tail_success,
                    vec![
                        ExpectedReply::for_path(
                            "/localnodes",
                            ServerReply::Sleep {
                                body: r#"["tail-success.test"]"#.to_string(),
                                delay: Duration::from_millis(400),
                            },
                        )
                        .for_host(tail_success.to_string()),
                        ExpectedReply::for_path(
                            "/localnodes",
                            ServerReply::Sleep {
                                body: r#"["tail-success.test"]"#.to_string(),
                                delay: Duration::from_millis(400),
                            },
                        )
                        .for_host(tail_success.to_string()),
                    ],
                ),
            ],
        )
        .await;
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(port)
            .seed_hosts([slow_failure.to_string(), tail_success.to_string()])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        let refresh = nodes.refresh_context();

        refresh
            .update_live_nodes_with_timeout(Duration::from_secs(2))
            .await;
        assert_eq!(nodes.live_nodes.load().len(), 2);
        assert!(
            nodes
                .progress
                .lock()
                .unwrap()
                .cluster_passes
                .contains_key(&0)
        );

        refresh
            .update_live_nodes_with_timeout(Duration::from_secs(2))
            .await;
        join_servers(servers).await;

        assert_eq!(nodes.live_nodes.load().len(), 3);
        assert!(
            nodes
                .live_nodes
                .load()
                .iter()
                .any(|node| node.host_str() == Some("tail-success.test")),
            "the tail candidate was not merged into the retained snapshot"
        );
    }

    #[tokio::test]
    async fn large_already_discovered_cluster_pass_is_linear_and_cooperative() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(["seed.test"])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        let refresh = nodes.refresh_context();
        let candidates = (0..20_000)
            .map(|index| {
                Arc::new(Url::parse(&format!("http://node-{index:05}.test:8000/")).unwrap())
            })
            .collect::<Vec<_>>();
        let mut pass = ClusterDiscoveryPass {
            candidates: DiscoveryCandidatePass::new(candidates.clone().into(), VecDeque::new()),
            ..ClusterDiscoveryPass::default()
        };
        pass.merge_response(candidates);
        refresh
            .progress
            .lock()
            .unwrap()
            .cluster_passes
            .insert(0, pass);
        let scheduler_ran = Arc::new(AtomicBool::new(false));
        let task_scheduler_ran = scheduler_ran.clone();
        let scheduler = tokio::spawn(async move {
            tokio::task::yield_now().await;
            task_scheduler_ran.store(true, Ordering::SeqCst);
        });

        let discovered = tokio::time::timeout(
            Duration::from_secs(1),
            refresh.discover_cluster_live_nodes(0, Duration::from_millis(100)),
        )
        .await
        .expect("large skip-heavy pass exceeded its bounded cooperative work");
        let DiscoveryOutcome::Found(discovered) = discovered else {
            panic!("cluster pass did not return its complete union");
        };
        scheduler.await.unwrap();

        assert_eq!(discovered.len(), 20_000);
        assert!(scheduler_ran.load(Ordering::SeqCst));
    }

    #[test]
    fn partial_cluster_merge_keeps_fresh_nodes_and_only_fitting_old_prefix() {
        use std::cell::Cell;

        let first = Arc::new(Url::parse("http://first.test:8000/").unwrap());
        let second = Arc::new(Url::parse("http://second.test:8000/").unwrap());
        let third = Arc::new(Url::parse("http://third.test:8000/").unwrap());

        let merged = DiscoveryRefresh::merge_cluster_snapshots_with_limits(
            vec![second.clone()],
            vec![first.clone(), second.clone()],
            2,
            1_000,
        )
        .unwrap();
        assert_eq!(merged, vec![first.clone(), second.clone()]);
        let node_limited = DiscoveryRefresh::merge_cluster_snapshots_with_limits(
            vec![third.clone()],
            merged.clone(),
            2,
            1_000,
        )
        .unwrap();
        assert_eq!(node_limited, vec![first.clone(), third]);

        let byte_limited = DiscoveryRefresh::merge_cluster_snapshots_with_limits(
            vec![second.clone()],
            vec![first.clone()],
            2,
            merged.iter().map(|node| node.as_str().len()).sum::<usize>() - 1,
        )
        .unwrap();
        assert_eq!(byte_limited, vec![second]);

        let consumed = Cell::new(0usize);
        let current_first = first.clone();
        let consumed_ref = &consumed;
        let oversized_current = (0..100).map(move |index| {
            consumed_ref.set(consumed_ref.get() + 1);
            if index == 0 {
                current_first.clone()
            } else {
                Arc::new(Url::parse(&format!("http://overflow-{index}.test:8000/")).unwrap())
            }
        });
        let prefix_only = DiscoveryRefresh::merge_cluster_snapshots_with_limits(
            vec![first.clone()],
            oversized_current,
            1,
            1_000,
        )
        .unwrap();
        assert_eq!(prefix_only, vec![first]);
        assert_eq!(
            consumed.get(),
            2,
            "bounded merge consumed nodes after the first proven overflow"
        );
    }

    #[test]
    fn cluster_partial_publication_repeats_when_new_nodes_are_validated() {
        let first = Arc::new(Url::parse("http://first.test:8000/").unwrap());
        let second = Arc::new(Url::parse("http://second.test:8000/").unwrap());
        let mut pass = ClusterDiscoveryPass::default();

        pass.merge_response(vec![first.clone()]);
        match pass.incomplete_outcome() {
            DiscoveryOutcome::PartialFound(nodes) => assert_eq!(nodes, vec![first.clone()]),
            outcome => panic!("expected first partial publication, got {outcome:?}"),
        }
        assert!(matches!(
            pass.incomplete_outcome(),
            DiscoveryOutcome::Incomplete
        ));

        pass.merge_response(vec![second.clone()]);
        match pass.incomplete_outcome() {
            DiscoveryOutcome::PartialFound(nodes) => assert_eq!(nodes, vec![first, second]),
            outcome => panic!("expected updated partial publication, got {outcome:?}"),
        }
    }

    #[tokio::test]
    async fn cluster_union_overflow_fails_closed_without_replacing_snapshot() {
        let config = AlternatorConfig::builder()
            .behavior_version_latest()
            .scheme("http")
            .port(8000)
            .seed_hosts(["seed.test"])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        let refresh = nodes.refresh_context();
        let retained = nodes.live_nodes.load().as_ref().clone();
        let repeated = Arc::new(Url::parse("http://repeated.test:8000/").unwrap());
        let mut pass = ClusterDiscoveryPass {
            discovered: vec![repeated; MAX_CLUSTER_PASS_NODES],
            ..ClusterDiscoveryPass::default()
        };
        pass.merge_response(vec![Arc::new(
            Url::parse("http://one-too-many.test:8000/").unwrap(),
        )]);
        assert!(pass.overflowed);
        let mut byte_overflow = ClusterDiscoveryPass {
            discovered_url_bytes: MAX_CLUSTER_PASS_URL_BYTES,
            ..ClusterDiscoveryPass::default()
        };
        byte_overflow.merge_response(vec![Arc::new(
            Url::parse("http://one-byte-too-many.test:8000/").unwrap(),
        )]);
        assert!(byte_overflow.overflowed);
        refresh
            .progress
            .lock()
            .unwrap()
            .cluster_passes
            .insert(0, pass);

        refresh
            .update_live_nodes_with_timeout(Duration::from_millis(100))
            .await;

        assert_eq!(nodes.live_nodes.load().as_slice(), retained.as_slice());
        assert!(refresh.progress.lock().unwrap().cluster_passes.is_empty());
    }

    #[test]
    fn address_pass_state_is_bounded_to_the_current_dns_answer() {
        let stable = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000);
        let mut pass = AddressDiscoveryPass::default();
        for index in 0..1000 {
            let churn = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 3, (index / 250) as u8, index as u8)),
                8000,
            );
            pass.reconcile(&[stable, churn]);
            assert_eq!(pass.addresses.len(), 2);
            assert!(pass.addresses.contains_key(&stable));
            assert!(pass.addresses.contains_key(&churn));
        }
    }

    #[test]
    fn address_pass_prioritizes_a_stable_record_across_alternating_subsets() {
        let socket = |last| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 13, 0, last)), 8000);
        let (a, b, c, d, e) = (socket(1), socket(2), socket(3), socket(4), socket(5));
        let mut pass = AddressDiscoveryPass::default();

        pass.reconcile(&[a, b, c]);
        assert_eq!(pass.begin_next(), Some(a));
        // Reconciliation models an enclosing scope cancellation: A remains
        // incomplete, while stable C retains an older first-seen rank than the
        // newly introduced D and E.
        pass.reconcile(&[d, e, c]);
        assert_eq!(pass.begin_next(), Some(c));
    }

    #[test]
    fn address_pass_persists_empty_evidence_until_all_records_finish() {
        let addresses = (1..=40)
            .map(|last| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 14, 0, last)), 8000))
            .collect::<Vec<_>>();
        let mut pass = AddressDiscoveryPass::default();
        pass.reconcile(&addresses);

        assert_eq!(pass.begin_next(), Some(addresses[0]));
        pass.complete_active(true);
        for _ in 1..addresses.len() {
            assert!(pass.begin_next().is_some());
            pass.complete_active(false);
        }

        assert!(matches!(
            pass.completed_outcome(),
            Some(DiscoveryOutcome::AuthoritativeEmpty)
        ));
    }

    #[test]
    fn address_pass_queue_exhaustion_is_linear() {
        let addresses = (0..20_000)
            .map(|index| {
                SocketAddr::new(
                    IpAddr::V6(Ipv6Addr::new(
                        0x2001,
                        0xdb8,
                        (index >> 16) as u16,
                        index as u16,
                        0,
                        0,
                        0,
                        1,
                    )),
                    8000,
                )
            })
            .collect::<Vec<_>>();
        let mut pass = AddressDiscoveryPass::default();
        pass.reconcile(&addresses);

        for _ in 0..addresses.len() {
            assert!(pass.begin_next().is_some());
            pass.complete_active(false);
        }
        assert!(pass.begin_next().is_none());

        assert!(pass.queue_probes <= addresses.len() * 2);
        assert!(matches!(
            pass.completed_outcome(),
            Some(DiscoveryOutcome::Unavailable)
        ));
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
        assert!(
            nodes
                .live_nodes
                .load()
                .iter()
                .any(|node| node.as_str() == format!("http://[::1]:{port}/")),
            "raw IPv6 seed recovery did not publish the recovered node"
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
