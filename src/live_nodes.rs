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

//! Maintains and updates a list of known Alternator nodes using Scylla system tables.
//!
//! # Overview
//!
//! [`LiveNodes`] is constructed from an [`AlternatorConfig`] and seeded with a list of hosts.
//! When [`ensure_discovery_started`] finds an active Tokio runtime, it launches a
//! background task which periodically calls [`update_live_nodes`] to query the
//! `.scylla.alternator.system.local` and `.scylla.alternator.system.peers`
//! virtual tables through a known node. After a successful refresh, the list is
//! updated to nodes from the highest available scope in the fallback chain
//! provided by the user. Discovery uses an injected [`aws_sdk_dynamodb::Client`]
//! so it shares configured transport, authentication, and TLS settings.
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
//! Each refresh shuffles the current node list and walks it as a candidate
//! queue. A pair of scans is pinned to each candidate endpoint and reads
//! `rpc_address`, `data_center`, and `rack` from both virtual tables:
//! - Both scans, including all pagination, must succeed before the snapshot is
//!   considered complete.
//! - The client filters that snapshot through the preferred routing scope and
//!   each configured fallback, publishing the first non-empty node set.
//! - A scan or network error drops that candidate and tries the next one.
//! - If the current candidates are exhausted, the original seed nodes get one
//!   recovery pass. Exhausting them, or finding no matching scope, leaves the
//!   last published node set unchanged.
//!
//! System-table membership describes topology rather than independently probed
//! liveness. Once discovery gets a non-empty topology, it atomically updates
//! the [`live_nodes`] list using [`ArcSwap`].
//!
//!  # Lifetime
//!
//! The background task holds a [`Weak`] reference to its [`LiveNodes`], so it
//! terminates on its own once the last owning [`Arc`] is dropped. [`Drop`]
//! additionally aborts the task to avoid waiting out the current sleep. The
//! client lifecycle owns all discovery tasks and
//! explicitly stops and joins them when the client is shut down.
//!
//! # Start-up
//!
//! The task is launched on the current Tokio runtime, which requires an active
//! runtime on the calling thread.
//! The client's [`from_conf`] constructor, however, is synchronous and can be called from anywhere.
//! It is handled by funneling start-up through a single idempotent entry point:
//! [`ensure_discovery_started`]. It does three things, in order:
//!
//! 1. If discovery is already running on the caller's runtime, return through
//!    a lock-free fast path. If another runtime owns it, retain that owner while
//!    the discovery task acknowledges a liveness request.
//! 2. If no Tokio runtime is available on the current thread, return without spawning.
//!    The task will be started lazily on the first [`get_next_node_round_robin`] or [`get_live_nodes`] call,
//!    which is typically invoked from within the request pipeline and therefore from within a runtime.
//! 3. An atomic registration plus a mutex on the cold-start or handoff path
//!    ensures that exactly one caller starts the task. A
//!    task-owned guard clears only its registration when it exits.
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
//! [`update_live_nodes`]: LiveNodes::update_live_nodes
//! [`get_next_node_round_robin`]: LiveNodes::get_next_node_round_robin
//! [`get_live_nodes`]: LiveNodes::get_live_nodes
//! [`live_nodes`]: LiveNodes::live_nodes
//! [`from_conf`]: crate::client::AlternatorClient::from_conf

use crate::routing_scope::RoutingScope;
use arc_swap::{ArcSwap, ArcSwapOption};
use aws_sdk_dynamodb::types::AttributeValue;
use aws_smithy_types::{retry::RetryConfig, timeout::TimeoutConfig};
use rand::seq::SliceRandom;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use url::Url;

const DEFAULT_ACTIVE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_IDLE_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const DISCOVERY_HTTP_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
const DISCOVERY_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const INITIAL_DISCOVERY_WAIT_SLACK: Duration = Duration::from_secs(1);
const SYSTEM_LOCAL_TABLE: &str = ".scylla.alternator.system.local";
const SYSTEM_PEERS_TABLE: &str = ".scylla.alternator.system.peers";
const TOPOLOGY_PROJECTION: &str = "rpc_address,data_center,rack";

/// An error encountered while constructing topology-discovery state.
#[derive(Debug)]
pub(crate) enum LiveNodesBuildError {
    MissingRoutingTarget,
    InvalidSeedHost {
        seed_host: String,
        source: url::ParseError,
    },
    InvalidScheme(String),
}

impl std::fmt::Display for LiveNodesBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingRoutingTarget => formatter
                .write_str("no Alternator routing target configured; set non-empty seed_hosts"),
            Self::InvalidSeedHost { seed_host, source } => {
                write!(formatter, "invalid seed host {seed_host:?}: {source}")
            }
            Self::InvalidScheme(scheme) => write!(
                formatter,
                "invalid Alternator transport scheme {scheme:?}: expected http or https, or a valid custom URI scheme for direct routing with a custom HTTP client"
            ),
        }
    }
}

impl std::error::Error for LiveNodesBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MissingRoutingTarget | Self::InvalidScheme(_) => None,
            Self::InvalidSeedHost { source, .. } => Some(source),
        }
    }
}

fn load_native_root_store() -> (rustls::RootCertStore, Vec<String>) {
    let load_results = rustls_native_certs::load_native_certs();
    let mut roots = rustls::RootCertStore::empty();
    let mut errors = load_results
        .errors
        .into_iter()
        .map(|error| error.to_string())
        .collect::<Vec<_>>();

    for certificate in load_results.certs {
        if let Err(error) = roots.add(certificate) {
            errors.push(error.to_string());
        }
    }

    (roots, errors)
}

pub(crate) fn native_roots_are_usable() -> bool {
    let (roots, _) = load_native_root_store();
    !roots.is_empty()
}

pub(crate) fn ensure_native_roots_are_usable() -> Result<(), String> {
    let (roots, errors) = load_native_root_store();
    if roots.is_empty() {
        Err(unusable_native_roots_message(errors))
    } else {
        Ok(())
    }
}

fn unusable_native_roots_message(errors: Vec<String>) -> String {
    if errors.is_empty() {
        "no usable native CA certificates were found".to_string()
    } else {
        errors.join("; ")
    }
}

#[derive(Debug)]
pub(crate) struct LiveNodes {
    pub(crate) tasks: Arc<crate::discovery::DiscoveryTasks>,
    routing_scope: RoutingScope,
    active_interval: Duration,
    idle_interval: Duration,
    discovery_timeout: Option<Duration>,
    counter: Arc<AtomicUsize>,
    live_nodes: ArcSwap<Vec<Arc<Url>>>,
    refresh_state: Mutex<RefreshState>,
    seed_urls: Vec<Arc<Url>>,
    alternator_scheme: String,
    port: Option<u16>,
    client: aws_sdk_dynamodb::Client,
    last_activity: Arc<Mutex<Instant>>,
    notify: Arc<tokio::sync::Notify>,
    initial_discovery_complete: AtomicBool,
    initial_discovery_notify: tokio::sync::Notify,
    discovery_task: Mutex<Option<tokio::task::AbortHandle>>,
    discovery_runtime: ArcSwapOption<DiscoveryRuntime>,
}

/// How long another runtime waits for the discovery task to acknowledge a
/// liveness request. No background polling is added while the client is idle.
const SHUTDOWN_PROBE_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug)]
struct DiscoveryRuntime {
    id: tokio::runtime::Id,
    stopped: AtomicBool,
    handoff_required: AtomicBool,
    shutdown_probe: Mutex<ShutdownProbe>,
    probe_requested: tokio::sync::Notify,
}

#[derive(Debug, Default)]
struct ShutdownProbe {
    last_started: Option<Instant>,
    pending: bool,
}

impl ShutdownProbe {
    fn is_due(&self, now: Instant) -> bool {
        self.last_started
            .is_none_or(|started| now.saturating_duration_since(started) >= SHUTDOWN_PROBE_INTERVAL)
    }
}

#[derive(Debug, Default)]
struct RefreshState {
    next_generation: u64,
    latest_published_generation: u64,
}

#[derive(Debug)]
struct TopologyRow {
    rpc_address: String,
    data_center: String,
    rack: String,
}

impl TopologyRow {
    fn from_item(
        item: &HashMap<String, AttributeValue>,
        rpc_address_fallback: Option<&str>,
    ) -> Option<Self> {
        let rpc_address = item
            .get("rpc_address")
            .and_then(|value| value.as_s().ok())
            .and_then(|address| address.parse::<std::net::IpAddr>().ok())
            .filter(|address| !address.is_unspecified())
            .map(|address| address.to_string())
            .or_else(|| rpc_address_fallback.map(str::to_owned))?;

        Some(Self {
            rpc_address,
            data_center: item.get("data_center")?.as_s().ok()?.clone(),
            rack: item.get("rack")?.as_s().ok()?.clone(),
        })
    }
}

impl DiscoveryRuntime {
    fn new(id: tokio::runtime::Id) -> Self {
        Self {
            id,
            stopped: AtomicBool::new(false),
            handoff_required: AtomicBool::new(false),
            shutdown_probe: Mutex::new(ShutdownProbe::default()),
            probe_requested: tokio::sync::Notify::new(),
        }
    }

    fn should_handoff(&self) -> bool {
        self.should_handoff_at(Instant::now())
    }

    fn should_handoff_at(&self, now: Instant) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return true;
        }
        let mut probe = self
            .shutdown_probe
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if probe.pending {
            if probe.is_due(now) {
                self.handoff_required.store(true, Ordering::Release);
            }
            return self.handoff_required.load(Ordering::Acquire);
        }
        if probe.is_due(now) {
            probe.last_started = Some(now);
            probe.pending = true;
            self.probe_requested.notify_one();
        }
        false
    }

    fn acknowledge_probe(&self) {
        let mut probe = self
            .shutdown_probe
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        probe.pending = false;
        self.handoff_required.store(false, Ordering::Release);
    }

    /// Keep the same I/O or sleep future pinned while acknowledging demand
    /// from another runtime. The discovery task itself proves that its owner
    /// is being driven; no probe is queued in either Tokio task pool.
    async fn with_probes<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        tokio::pin!(future);
        loop {
            tokio::select! {
                biased;
                _ = self.probe_requested.notified() => self.acknowledge_probe(),
                result = &mut future => return result,
            }
        }
    }
}

struct DiscoveryTaskGuard {
    live_nodes: Weak<LiveNodes>,
    runtime: Arc<DiscoveryRuntime>,
}

impl Drop for DiscoveryTaskGuard {
    fn drop(&mut self) {
        self.runtime.stopped.store(true, Ordering::Release);
        if let Some(live_nodes) = self.live_nodes.upgrade() {
            // Clear only this task's exact registration so cleanup can never
            // erase a newer registration.
            let previous = live_nodes
                .discovery_runtime
                .compare_and_swap(&self.runtime, None);
            if previous
                .as_ref()
                .is_some_and(|runtime| Arc::ptr_eq(runtime, &self.runtime))
            {
                // A request waiting for initial discovery may be running on a
                // different runtime. Wake it so that runtime can take over
                // discovery without waiting for the readiness deadline.
                live_nodes.initial_discovery_notify.notify_waiters();
            }
        }
    }
}

impl LiveNodes {
    /// Creates discovery state from the configured seed hosts.
    ///
    /// Returns [`None`] when discovery is turned off with
    /// [`AlternatorBuilder::without_discovery`](crate::config::AlternatorBuilder::without_discovery),
    /// in which case requests go straight to the first seed host.
    ///
    /// # Panics
    ///
    /// Panics if routing configuration is missing or invalid. Invalid routing
    /// configuration fails closed instead of falling back to an unrelated SDK
    /// endpoint.
    #[cfg(test)]
    pub(crate) fn new(config: &crate::config::AlternatorConfig) -> Option<Arc<Self>> {
        Self::try_new_for_test(config)
            .unwrap_or_else(|error| panic!("failed to construct LiveNodes: {error}"))
    }

    #[cfg(test)]
    pub(crate) fn try_new_for_test(
        config: &crate::config::AlternatorConfig,
    ) -> Result<Option<Arc<Self>>, LiveNodesBuildError> {
        let mut builder = config.dynamodb_config.to_builder().allow_no_auth();
        if config.region().is_none() {
            builder = builder.region(aws_sdk_dynamodb::config::Region::from_static("us-east-1"));
        }
        let client = aws_sdk_dynamodb::Client::from_conf(builder.build());
        Self::try_new(config, client)
    }

    pub(crate) fn try_new(
        config: &crate::config::AlternatorConfig,
        client: aws_sdk_dynamodb::Client,
    ) -> Result<Option<Arc<Self>>, LiveNodesBuildError> {
        Self::try_new_with_scope(config, None, client)
    }

    /// Creates discovery state using `routing_scope` instead of the scope in
    /// `config` while retaining all other transport and refresh settings.
    pub(crate) fn try_new_for_scope(
        config: &crate::config::AlternatorConfig,
        routing_scope: RoutingScope,
        client: aws_sdk_dynamodb::Client,
    ) -> Result<Option<Arc<Self>>, LiveNodesBuildError> {
        Self::try_new_with_scope(config, Some(routing_scope), client)
    }

    fn try_new_with_scope(
        config: &crate::config::AlternatorConfig,
        routing_scope: Option<RoutingScope>,
        client: aws_sdk_dynamodb::Client,
    ) -> Result<Option<Arc<Self>>, LiveNodesBuildError> {
        let active_interval = config
            .active_interval()
            .unwrap_or(DEFAULT_ACTIVE_REFRESH_INTERVAL);
        let idle_interval = config
            .idle_interval()
            .unwrap_or(DEFAULT_IDLE_REFRESH_INTERVAL);
        let routing_scope = routing_scope
            .or_else(|| config.routing_scope())
            .unwrap_or(RoutingScope::from_cluster());
        let alternator_scheme = config.scheme().unwrap_or("http".to_string());
        let port = config.port();
        let Some(seed_nodes) = config.seed_hosts() else {
            return Err(LiveNodesBuildError::MissingRoutingTarget);
        };
        let Some(first_seed) = seed_nodes.first() else {
            return Err(LiveNodesBuildError::MissingRoutingTarget);
        };

        if config.without_discovery() {
            // Requests go to the seed host itself, so it has to be a usable
            // target even though nothing is discovered through it. A custom
            // HTTP client may speak a scheme this driver does not know.
            if !is_valid_uri_scheme(&alternator_scheme)
                || (config.http_client().is_none()
                    && !alternator_scheme.eq_ignore_ascii_case("http")
                    && !alternator_scheme.eq_ignore_ascii_case("https"))
            {
                return Err(LiveNodesBuildError::InvalidScheme(alternator_scheme));
            }
            build_seed_url(&alternator_scheme, first_seed, port).map_err(|source| {
                LiveNodesBuildError::InvalidSeedHost {
                    seed_host: first_seed.clone(),
                    source,
                }
            })?;
            return Ok(None);
        }

        if !alternator_scheme.eq_ignore_ascii_case("http")
            && !alternator_scheme.eq_ignore_ascii_case("https")
        {
            return Err(LiveNodesBuildError::InvalidScheme(alternator_scheme));
        }

        let mut seed_urls = seed_nodes
            .iter()
            .map(|seed_host| {
                build_seed_url(&alternator_scheme, seed_host, port)
                    .map(Arc::new)
                    .map_err(|source| LiveNodesBuildError::InvalidSeedHost {
                        seed_host: seed_host.clone(),
                        source,
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        seed_urls.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));

        Ok(Some(Arc::new(Self {
            tasks: Arc::default(),
            routing_scope,
            active_interval,
            idle_interval,
            discovery_timeout: config.discovery_timeout(),
            counter: Arc::new(AtomicUsize::new(0)),
            live_nodes: ArcSwap::from_pointee(seed_urls.clone()),
            refresh_state: Mutex::new(RefreshState::default()),
            seed_urls,
            alternator_scheme,
            port,
            client,
            last_activity: Arc::new(Mutex::new(Instant::now())),
            notify: Arc::new(tokio::sync::Notify::new()),
            initial_discovery_complete: AtomicBool::new(false),
            initial_discovery_notify: tokio::sync::Notify::new(),
            discovery_task: Mutex::new(None),
            discovery_runtime: ArcSwapOption::empty(),
        })))
    }

    fn host_to_uri(&self, addr: &str) -> Result<Url, url::ParseError> {
        build_node_url(&self.alternator_scheme, addr, self.port)
    }

    async fn scan_topology_table(
        &self,
        table_name: &str,
        node_addr: &Url,
        rpc_address_fallback: Option<&str>,
    ) -> Option<Vec<TopologyRow>> {
        let endpoint_url = node_addr.origin().ascii_serialization();
        let request_timeout = self
            .discovery_timeout
            .unwrap_or(DISCOVERY_HTTP_OPERATION_TIMEOUT);
        let deadline = tokio::time::Instant::now().checked_add(request_timeout)?;

        tokio::time::timeout_at(deadline, async {
            let mut rows = Vec::new();
            let mut exclusive_start_key = None;

            loop {
                let timeouts = TimeoutConfig::builder()
                    .connect_timeout(DISCOVERY_HTTP_CONNECT_TIMEOUT.min(request_timeout))
                    .operation_timeout(request_timeout)
                    .operation_attempt_timeout(request_timeout);
                let config_override = aws_sdk_dynamodb::Config::builder()
                    .endpoint_url(&endpoint_url)
                    .retry_config(RetryConfig::disabled())
                    .timeout_config(timeouts.build());
                let output = self
                    .client
                    .scan()
                    .table_name(table_name)
                    .projection_expression(TOPOLOGY_PROJECTION)
                    .set_exclusive_start_key(exclusive_start_key)
                    .customize()
                    .config_override(config_override)
                    .send()
                    .await
                    .ok()?;

                rows.extend(
                    output
                        .items()
                        .iter()
                        .filter_map(|item| TopologyRow::from_item(item, rpc_address_fallback)),
                );
                exclusive_start_key = output
                    .last_evaluated_key()
                    .filter(|key| !key.is_empty())
                    .cloned();
                if exclusive_start_key.is_none() {
                    return Some(rows);
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    async fn fetch_topology(&self, node_addr: &Url) -> Option<Vec<TopologyRow>> {
        // Treat the two complete scans as one snapshot: publishing rows from
        // only one table could silently drop the local node or every peer.
        let local_address = node_addr.host_str()?;
        let mut rows = self
            .scan_topology_table(SYSTEM_LOCAL_TABLE, node_addr, Some(local_address))
            .await?;
        if rows.is_empty() {
            return None;
        }
        rows.extend(
            self.scan_topology_table(SYSTEM_PEERS_TABLE, node_addr, None)
                .await?,
        );
        Some(rows)
    }

    fn topology_for_scope(&self, topology: &[TopologyRow], scope: &RoutingScope) -> Vec<Arc<Url>> {
        let mut nodes = topology
            .iter()
            .filter(|row| scope.contains(&row.data_center, &row.rack))
            .filter_map(|row| self.host_to_uri(&row.rpc_address).ok().map(Arc::new))
            .collect::<Vec<_>>();

        nodes.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        nodes.dedup_by(|left, right| left.as_str() == right.as_str());
        nodes
    }

    fn begin_refresh(&self) -> u64 {
        let mut state = self
            .refresh_state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.next_generation = state
            .next_generation
            .checked_add(1)
            .expect("live-node refresh generation overflowed");
        state.next_generation
    }

    /// Ensures the background discovery task is running.
    ///
    /// Idempotent and safe to call from any context: returns immediately if
    /// discovery is already running on a live Tokio runtime, or if no runtime
    /// is available. A caller on another runtime keeps a healthy owner stable,
    /// but takes ownership when its discovery task exits or cannot acknowledge
    /// a liveness request within the bounded interval.
    pub(crate) fn ensure_discovery_started(self: &Arc<Self>) {
        if self.tasks.is_stopped() {
            return;
        }
        let Ok(handle) = Handle::try_current() else {
            return;
        };
        let runtime_id = handle.id();
        // Requests on the owning runtime never take the start/transfer mutex.
        // A caller on another runtime transfers only after the owner exits or
        // leaves a liveness request pending beyond the bounded interval.
        if self
            .discovery_runtime
            .load()
            .as_ref()
            .is_some_and(|active| active.id == runtime_id || !active.should_handoff())
        {
            return;
        }

        let mut discovery_task = self
            .discovery_task
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        // Another caller may have completed the cold start or transfer while
        // this caller waited for the mutex.
        let active_runtime = self.discovery_runtime.load_full();
        if active_runtime
            .as_ref()
            .is_some_and(|active| active.id == runtime_id || !active.should_handoff())
        {
            return;
        }

        if let Some(old_task) = discovery_task.take() {
            old_task.abort();
        }
        let runtime = Arc::new(DiscoveryRuntime::new(runtime_id));
        self.discovery_runtime.store(Some(runtime.clone()));
        let weak_self = Arc::downgrade(self);
        let notify = self.notify.clone();
        let task_guard = DiscoveryTaskGuard {
            live_nodes: weak_self.clone(),
            runtime: runtime.clone(),
        };

        self.mark_activity();
        let task = self.tasks.spawn(&handle, async move {
            let _task_guard = task_guard;
            loop {
                let (idle_interval, active_interval, is_idle) = {
                    let Some(strong_self) = weak_self.upgrade() else {
                        break;
                    };

                    runtime.with_probes(strong_self.update_live_nodes()).await;

                    let last = *strong_self.last_activity.lock().unwrap();
                    (
                        strong_self.idle_interval,
                        strong_self.active_interval,
                        last.elapsed() >= strong_self.idle_interval,
                    )
                };

                let interval = if is_idle {
                    idle_interval
                } else {
                    active_interval
                };
                runtime
                    .with_probes(async {
                        tokio::select! {
                            _ = tokio::time::sleep(interval) => {}
                            _ = notify.notified() => {}
                        }
                    })
                    .await;
            }
        });
        *discovery_task = task;
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

    fn mark_initial_discovery_complete(&self) {
        if !self.initial_discovery_complete.swap(true, Ordering::AcqRel) {
            self.initial_discovery_notify.notify_waiters();
        }
    }

    /// Waits until discovery has published its first non-empty topology.
    ///
    /// Rack-scoped clients use this before their first affinity-routed request
    /// so the deterministic coordinator is derived from every rack rather
    /// than from whichever bootstrap seeds one client happened to receive.
    pub(crate) async fn wait_for_initial_discovery(self: &Arc<Self>) -> bool {
        self.wait_for_initial_discovery_with_timeouts(
            self.discovery_timeout
                .unwrap_or(DISCOVERY_HTTP_OPERATION_TIMEOUT),
            INITIAL_DISCOVERY_WAIT_SLACK,
        )
        .await
    }

    async fn wait_for_initial_discovery_with_timeouts(
        self: &Arc<Self>,
        request_timeout: Duration,
        scheduling_slack: Duration,
    ) -> bool {
        if self.initial_discovery_complete.load(Ordering::Acquire) {
            return true;
        }
        if Handle::try_current().is_err() {
            return false;
        }
        let started = tokio::time::Instant::now();
        let deadline = started.checked_add(self.initial_discovery_wait_timeout_for_live_count(
            self.live_nodes.load().len(),
            request_timeout,
            scheduling_slack,
        ));
        let Some(deadline) = deadline else {
            return false;
        };

        loop {
            let notified = self.initial_discovery_notify.notified();
            // Register for owner-exit notifications before checking ownership,
            // then request an immediate refresh. Retry on every wake so this
            // runtime can also take over from an owner that stopped during the
            // wait.
            self.ensure_discovery_started();
            if self.initial_discovery_complete.load(Ordering::Acquire) {
                return true;
            }
            if self.tasks.is_stopped() {
                return false;
            }
            self.notify.notify_one();

            let timed_out = tokio::select! {
                _ = self.tasks.cancelled() => return self.initial_discovery_complete.load(Ordering::Acquire),
                _ = tokio::time::sleep(SHUTDOWN_PROBE_INTERVAL) => false,
                result = tokio::time::timeout_at(deadline, notified) => result.is_err(),
            };
            if self.initial_discovery_complete.load(Ordering::Acquire) {
                return true;
            }
            if timed_out || tokio::time::Instant::now() >= deadline {
                return false;
            }
        }
    }

    #[cfg(test)]
    fn initial_discovery_wait_timeout(&self) -> Duration {
        self.initial_discovery_wait_timeout_for_live_count(
            self.live_nodes.load().len(),
            self.discovery_timeout
                .unwrap_or(DISCOVERY_HTTP_OPERATION_TIMEOUT),
            INITIAL_DISCOVERY_WAIT_SLACK,
        )
    }

    fn initial_discovery_wait_timeout_for_live_count(
        &self,
        live_count: usize,
        request_timeout: Duration,
        scheduling_slack: Duration,
    ) -> Duration {
        let seed_count = u32::try_from(self.seed_urls.len()).unwrap_or(u32::MAX);
        let live_count = u32::try_from(live_count).unwrap_or(u32::MAX);
        // A failed pass can visit the current candidates and then all original
        // seeds. Each candidate needs one bounded system.local scan and one
        // bounded system.peers scan. Scope fallback filters the same snapshot
        // locally, so it adds no network allowance.
        let candidate_budget = seed_count.saturating_add(live_count);
        let request_budget = candidate_budget.saturating_mul(2);
        request_timeout
            .saturating_mul(request_budget)
            .saturating_add(scheduling_slack)
    }

    /// Returns all currently discovered topology nodes and updates activity.
    pub(crate) fn get_live_nodes(self: &Arc<Self>) -> Vec<Arc<Url>> {
        self.ensure_discovery_started();
        self.mark_activity();
        self.live_nodes.load().as_ref().clone()
    }

    /// Returns the first discovered node not in `used_nodes`, starting at the
    /// next round-robin position.
    /// Used by [`crate::QueryPlan`] round-robin strategy.
    pub(crate) fn get_next_node_round_robin(
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

        // Checking an exhausted query plan must not advance the shared
        // round-robin position. The caller may clear `used_nodes` and retry,
        // and that retry should consume the next position itself.
        if live_nodes.iter().all(|node| used_nodes.contains(node)) {
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

    async fn update_live_nodes(&self) {
        let generation = self.begin_refresh();
        // Try discovered topology nodes in a random order.
        let mut nodes = self.live_nodes.load().as_ref().clone();
        nodes.shuffle(&mut rand::rng());
        let mut candidates: VecDeque<Arc<Url>> = nodes.into();
        let mut using_seeds = false;

        while let Some(node_addr) = candidates.pop_front() {
            // Request failed: try the next candidate, or fall back to seeds.
            let Some(topology) = self.fetch_topology(&node_addr).await else {
                if candidates.is_empty() && !using_seeds {
                    using_seeds = true;
                    candidates = self.seed_urls.clone().into();
                }
                continue;
            };

            // One system-table snapshot covers every routing scope. Walk the
            // fallback chain locally instead of issuing another pair of scans.
            let mut scope = &self.routing_scope;
            loop {
                let new_nodes = self.topology_for_scope(&topology, scope);
                if !new_nodes.is_empty() {
                    self.publish_live_nodes(generation, new_nodes);
                    return;
                }

                let Some(fallback) = scope.fallback() else {
                    return;
                };
                scope = fallback;
            }
        }
    }

    fn publish_live_nodes(&self, generation: u64, new_nodes: Vec<Arc<Url>>) {
        let mut state = self
            .refresh_state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if generation < state.latest_published_generation {
            return;
        }
        if **self.live_nodes.load() != new_nodes {
            self.live_nodes.store(Arc::new(new_nodes));
        }
        state.latest_published_generation = generation;
        self.mark_initial_discovery_complete();
    }
}

pub(crate) fn build_seed_url(
    scheme: &str,
    addr: &str,
    port: Option<u16>,
) -> Result<Url, url::ParseError> {
    let unbracketed = addr
        .strip_prefix('[')
        .and_then(|addr| addr.strip_suffix(']'))
        .unwrap_or(addr);
    if unbracketed.parse::<std::net::Ipv6Addr>().is_err() {
        url::Host::parse(addr)?;
    }
    build_node_url(scheme, unbracketed, port)
}

/// Whether `scheme` is a syntactically valid bare URI scheme.
///
/// Validate before calling [`Url::parse`]. WHATWG URL parsing strips ASCII
/// tabs, newlines, carriage returns, and surrounding control characters, so
/// relying on it alone could silently turn malformed input into another valid
/// transport scheme.
fn is_valid_uri_scheme(scheme: &str) -> bool {
    let mut bytes = scheme.bytes();

    matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
}

fn build_node_url(scheme: &str, addr: &str, port: Option<u16>) -> Result<Url, url::ParseError> {
    if !is_valid_uri_scheme(scheme) {
        return Err(url::ParseError::RelativeUrlWithoutBase);
    }
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

impl Drop for LiveNodes {
    fn drop(&mut self) {
        if let Ok(mut state) = self.discovery_task.lock()
            && let Some(task) = state.take()
        {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AlternatorConfig;
    use aws_smithy_runtime_api::client::http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;
    use futures_util::FutureExt;
    use std::collections::VecDeque;

    #[tokio::test]
    async fn initial_discovery_waits_for_a_published_topology() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(1)
            .active_interval(Duration::from_secs(60))
            .build();
        let nodes = LiveNodes::new(&config).expect("live nodes");
        let waiter = tokio::spawn({
            let nodes = nodes.clone();
            async move { nodes.wait_for_initial_discovery().await }
        });

        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        let generation = nodes.begin_refresh();
        nodes.publish_live_nodes(
            generation,
            vec![Arc::new(Url::parse("http://127.0.0.2:1").unwrap())],
        );

        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .expect("waiter should observe the topology")
                .expect("waiter task should complete")
        );
    }

    #[tokio::test]
    async fn initial_discovery_wait_deadline_does_not_roll_on_notifications() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(1)
            .active_interval(Duration::from_secs(60))
            .build();
        let nodes = LiveNodes::new(&config).expect("live nodes");

        let notifier = tokio::spawn({
            let nodes = nodes.clone();
            async move {
                for _ in 0..200 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    nodes.initial_discovery_notify.notify_waiters();
                }
            }
        });

        let result = tokio::time::timeout(
            Duration::from_millis(100),
            nodes.wait_for_initial_discovery_with_timeouts(
                Duration::from_millis(1),
                Duration::from_millis(1),
            ),
        )
        .await
        .expect("duplicate notifications must not roll the readiness deadline");
        notifier.abort();

        assert!(!result);
    }

    #[test]
    fn initial_discovery_timeout_accounts_for_candidates_and_both_scans() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1", "127.0.0.2"])
            .port(1)
            .routing_scope(
                RoutingScope::from_rack("dc1".to_string(), "rack1".to_string())
                    .with_fallback(RoutingScope::from_datacenter("dc1".to_string()))
                    .with_fallback(RoutingScope::from_cluster()),
            )
            .build();
        let nodes = LiveNodes::new(&config).expect("live nodes");

        assert_eq!(
            nodes.initial_discovery_wait_timeout(),
            Duration::from_secs(41)
        );

        nodes.live_nodes.store(Arc::new(
            (1..=5)
                .map(|index| Arc::new(Url::parse(&format!("http://node{index}.test:1")).unwrap()))
                .collect(),
        ));
        assert_eq!(
            nodes.initial_discovery_wait_timeout(),
            Duration::from_secs(71)
        );
    }

    #[tokio::test]
    async fn shutdown_releases_initial_discovery_waiters() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        let mut waiter = Box::pin(nodes.wait_for_initial_discovery());
        assert!(waiter.as_mut().now_or_never().is_none());
        tokio::time::timeout(Duration::from_secs(1), nodes.tasks.shutdown())
            .await
            .expect("shutdown must join topology discovery");
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .expect("shutdown must release initial discovery waiters")
        );
        nodes.ensure_discovery_started();
        assert!(!discovery_is_running(&nodes));
    }

    fn discovery_is_running(nodes: &LiveNodes) -> bool {
        nodes.discovery_runtime.load().is_some()
    }

    fn test_config() -> AlternatorConfig {
        AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(1)
            .build()
    }

    #[derive(Clone, Debug)]
    struct DiscoveryResponseHttpClient {
        requests: Arc<Mutex<Vec<String>>>,
        responses: Arc<Mutex<VecDeque<(u16, String)>>>,
    }

    impl DiscoveryResponseHttpClient {
        fn new(responses: impl IntoIterator<Item = impl Into<String>>) -> Self {
            Self::with_statuses(responses.into_iter().map(|response| (200, response.into())))
        }

        fn with_statuses(responses: impl IntoIterator<Item = (u16, String)>) -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
            }
        }
    }

    impl HttpClient for DiscoveryResponseHttpClient {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    impl HttpConnector for DiscoveryResponseHttpClient {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let body = request
                .body()
                .bytes()
                .expect("DynamoDB request body should be buffered");
            self.requests.lock().unwrap().push(format!(
                "{} {} {}",
                request.method(),
                request.uri(),
                String::from_utf8_lossy(body)
            ));
            let (status, response) = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected topology request");
            HttpConnectorFuture::ready(Ok(HttpResponse::new(
                StatusCode::try_from(status).unwrap(),
                SdkBody::from(response),
            )))
        }
    }

    #[tokio::test]
    async fn topology_scans_are_pinned_paginated_filtered_and_deduplicated() {
        let http_client = DiscoveryResponseHttpClient::new([
            r#"{"Items":[{"rpc_address":{"S":"10.0.0.2"},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}}],"Count":1,"ScannedCount":1,"LastEvaluatedKey":{"rpc_address":{"S":"10.0.0.2"}}}"#,
            r#"{"Items":[{"rpc_address":{"S":"10.0.0.1"},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}}],"Count":1,"ScannedCount":1}"#,
            r#"{"Items":[{"rpc_address":{"S":"10.0.0.2"},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}},{"rpc_address":{"S":"10.0.0.3"},"data_center":{"S":"dc1"},"rack":{"S":"rack2"}},{"data_center":{"S":"dc1"},"rack":{"S":"rack1"}},{"rpc_address":{"N":"4"},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}},{"rpc_address":{"S":""},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}}],"Count":5,"ScannedCount":5}"#,
        ]);
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .routing_scope(RoutingScope::from_rack("dc1".into(), "rack1".into()))
            .http_client(http_client.clone())
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        nodes.update_live_nodes().await;

        let urls = nodes
            .live_nodes
            .load()
            .iter()
            .map(|url| url.as_str().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(urls, ["http://10.0.0.1:8000/", "http://10.0.0.2:8000/"]);

        let requests = http_client.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        for request in requests.iter() {
            assert!(request.starts_with("POST http://127.0.0.1:8000/ "));
            let body = request.split_once("/ ").unwrap().1;
            let body: serde_json::Value = serde_json::from_str(body).unwrap();
            assert_eq!(
                body["ProjectionExpression"],
                serde_json::Value::String(TOPOLOGY_PROJECTION.to_owned())
            );
        }
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(requests[0].split_once("/ ").unwrap().1)
                .unwrap()["TableName"],
            SYSTEM_LOCAL_TABLE
        );
        let second =
            serde_json::from_str::<serde_json::Value>(requests[1].split_once("/ ").unwrap().1)
                .unwrap();
        assert_eq!(second["TableName"], SYSTEM_LOCAL_TABLE);
        assert_eq!(second["ExclusiveStartKey"]["rpc_address"]["S"], "10.0.0.2");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(requests[2].split_once("/ ").unwrap().1)
                .unwrap()["TableName"],
            SYSTEM_PEERS_TABLE
        );
    }

    #[tokio::test]
    async fn failed_peers_scan_discards_local_rows_without_sdk_retry() {
        let http_client = DiscoveryResponseHttpClient::with_statuses([
            (
                200,
                r#"{"Items":[{"rpc_address":{"S":"10.0.0.1"},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}}],"Count":1,"ScannedCount":1}"#.to_owned(),
            ),
            (
                500,
                r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"failed"}"#.to_owned(),
            ),
        ]);
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .http_client(http_client.clone())
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        assert!(nodes.fetch_topology(&nodes.seed_urls[0]).await.is_none());
        assert_eq!(
            http_client.requests.lock().unwrap().len(),
            2,
            "operation override must disable SDK retries"
        );
    }

    #[tokio::test]
    async fn local_row_uses_candidate_for_an_unusable_rpc_address() {
        for local_item in [
            r#"{"rpc_address":{"S":"0.0.0.0"},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}}"#,
            r#"{"rpc_address":{"N":"1"},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}}"#,
            r#"{"data_center":{"S":"dc1"},"rack":{"S":"rack1"}}"#,
        ] {
            let http_client = DiscoveryResponseHttpClient::new([
                format!(r#"{{"Items":[{local_item}],"Count":1,"ScannedCount":1}}"#),
                r#"{"Items":[],"Count":0,"ScannedCount":0}"#.to_owned(),
            ]);
            let config = AlternatorConfig::builder()
                .seed_hosts(["seed.example.com"])
                .port(8000)
                .http_client(http_client)
                .build();
            let nodes = LiveNodes::new(&config).unwrap();

            let topology = nodes
                .fetch_topology(&nodes.seed_urls[0])
                .await
                .expect("the candidate address should repair the local row");

            assert_eq!(topology.len(), 1);
            assert_eq!(topology[0].rpc_address, "seed.example.com");
            assert_eq!(topology[0].data_center, "dc1");
            assert_eq!(topology[0].rack, "rack1");
        }
    }

    #[tokio::test]
    async fn missing_local_row_rejects_an_incomplete_topology() {
        let http_client =
            DiscoveryResponseHttpClient::new([r#"{"Items":[],"Count":0,"ScannedCount":0}"#]);
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .http_client(http_client.clone())
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        assert!(nodes.fetch_topology(&nodes.seed_urls[0]).await.is_none());
        assert_eq!(
            http_client.requests.lock().unwrap().len(),
            1,
            "system.peers must not be published without a valid local row"
        );
    }

    #[tokio::test]
    async fn initial_wait_wakes_discovery_after_a_failed_refresh() {
        let failure = r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"failed"}"#;
        let http_client = DiscoveryResponseHttpClient::with_statuses([
            (500, failure.to_owned()),
            (500, failure.to_owned()),
            (
                200,
                r#"{"Items":[{"rpc_address":{"S":"127.0.0.1"},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}}],"Count":1,"ScannedCount":1}"#.to_owned(),
            ),
            (200, r#"{"Items":[],"Count":0,"ScannedCount":0}"#.to_owned()),
        ]);
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .active_interval(Duration::from_secs(60 * 60))
            .idle_interval(Duration::from_secs(60 * 60))
            .http_client(http_client.clone())
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        nodes.ensure_discovery_started();

        tokio::time::timeout(Duration::from_secs(1), async {
            while http_client.requests.lock().unwrap().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first refresh should exhaust the candidate and seed");
        assert!(!nodes.initial_discovery_complete.load(Ordering::Acquire));

        let ready = tokio::time::timeout(
            Duration::from_secs(1),
            nodes.wait_for_initial_discovery_with_timeouts(
                Duration::from_millis(100),
                Duration::from_millis(100),
            ),
        )
        .await
        .expect("readiness wait should wake sleeping discovery");

        assert!(ready);
        assert_eq!(http_client.requests.lock().unwrap().len(), 4);
    }

    #[test]
    fn start_without_runtime_does_not_panic() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!discovery_is_running(&nodes));
    }

    #[tokio::test]
    async fn start_with_runtime_starts_correctly() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(discovery_is_running(&nodes));
    }

    #[test]
    fn start_on_first_access_round_robin() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!discovery_is_running(&nodes));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _ = nodes.get_next_node_round_robin(&std::collections::HashSet::new());
        });
        assert!(discovery_is_running(&nodes));
    }

    #[test]
    fn start_on_first_access() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        LiveNodes::ensure_discovery_started(&nodes);
        assert!(!discovery_is_running(&nodes));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _ = nodes.get_live_nodes();
        });
        assert!(discovery_is_running(&nodes));
    }

    #[test]
    fn initial_discovery_waiter_takes_over_after_owner_runtime_stops() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(1)
            .active_interval(Duration::from_secs(60 * 60))
            .idle_interval(Duration::from_secs(60 * 60))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        let first_runtime = tokio::runtime::Runtime::new().unwrap();
        first_runtime.block_on(async {
            nodes.ensure_discovery_started();
            tokio::task::yield_now().await;
        });
        let first_owner = nodes.discovery_runtime.load_full().unwrap();

        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::sync_channel(0);
        let shutdown = std::thread::spawn(move || {
            shutdown_rx.recv().unwrap();
            first_runtime.shutdown_background();
        });

        let replacement_runtime = tokio::runtime::Runtime::new().unwrap();
        let replacement_id = replacement_runtime.handle().id();
        replacement_runtime.block_on(async {
            let waiter = tokio::spawn({
                let nodes = nodes.clone();
                async move {
                    nodes
                        .wait_for_initial_discovery_with_timeouts(
                            Duration::from_secs(1),
                            Duration::ZERO,
                        )
                        .await
                }
            });

            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if first_owner
                        .shutdown_probe
                        .lock()
                        .unwrap()
                        .last_started
                        .is_some()
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("waiter did not check the original discovery owner");
            shutdown_tx.send(()).unwrap();

            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if nodes
                        .discovery_runtime
                        .load()
                        .as_ref()
                        .is_some_and(|runtime| runtime.id == replacement_id)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("waiting runtime did not take over discovery after owner exit");

            waiter.abort();
            let _ = waiter.await;
        });
        shutdown.join().unwrap();
    }

    #[test]
    fn healthy_discovery_owner_is_stable_across_runtimes() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        let first_runtime = tokio::runtime::Runtime::new().unwrap();
        let second_runtime = tokio::runtime::Runtime::new().unwrap();
        first_runtime.block_on(async {
            nodes.ensure_discovery_started();
        });
        let original = nodes.discovery_runtime.load_full().unwrap();
        second_runtime.block_on(async {
            for _ in 0..10 {
                original.shutdown_probe.lock().unwrap().last_started = None;
                nodes.ensure_discovery_started();
                tokio::time::timeout(Duration::from_secs(1), async {
                    while original.shutdown_probe.lock().unwrap().pending {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("healthy owner must acknowledge without a blocking-pool probe");
                assert!(Arc::ptr_eq(
                    &nodes.discovery_runtime.load_full().unwrap(),
                    &original
                ));
                assert_eq!(nodes.tasks.active_task_count(), 1);
            }
            tokio::time::timeout(Duration::from_secs(1), nodes.tasks.shutdown())
                .await
                .expect("shutdown must join discovery on its healthy owner");
        });
    }

    #[test]
    fn shutdown_probe_interval_is_rate_limited() {
        let started = Instant::now();
        let probe = ShutdownProbe {
            last_started: Some(started),
            ..Default::default()
        };

        assert!(!probe.is_due(started));
        assert!(!probe.is_due(started + SHUTDOWN_PROBE_INTERVAL - Duration::from_nanos(1)));
        assert!(probe.is_due(started + SHUTDOWN_PROBE_INTERVAL));
    }

    #[derive(Clone, Debug)]
    struct HeldFirstTopology {
        requests: Arc<AtomicUsize>,
        entered: Arc<tokio::sync::Semaphore>,
        dropped: Arc<tokio::sync::Semaphore>,
        following: DiscoveryResponseHttpClient,
        connect_timeouts: Arc<Mutex<Vec<Option<Duration>>>>,
    }

    struct DropPermit(Arc<tokio::sync::Semaphore>);
    impl Drop for DropPermit {
        fn drop(&mut self) {
            self.0.add_permits(1);
        }
    }

    impl HttpClient for HeldFirstTopology {
        fn http_connector(
            &self,
            settings: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            self.connect_timeouts
                .lock()
                .unwrap()
                .push(settings.connect_timeout());
            SharedHttpConnector::new(self.clone())
        }
    }

    impl HttpConnector for HeldFirstTopology {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            if self.requests.fetch_add(1, Ordering::SeqCst) == 0 {
                let entered = self.entered.clone();
                let dropped = self.dropped.clone();
                return HttpConnectorFuture::new(async move {
                    let _guard = DropPermit(dropped);
                    entered.add_permits(1);
                    std::future::pending().await
                });
            }
            self.following.call(request)
        }
    }

    #[test]
    fn demand_probes_preserve_io_and_stalled_owner_cannot_publish_after_handoff() {
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
            .enable_all()
            .build()
            .unwrap();
        let observer = tokio::runtime::Runtime::new().unwrap();
        let http = HeldFirstTopology {
            requests: Arc::default(),
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
            dropped: Arc::new(tokio::sync::Semaphore::new(0)),
            connect_timeouts: Arc::default(),
            following: DiscoveryResponseHttpClient::new([
                r#"{"Items":[{"rpc_address":{"S":"127.0.0.2"},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}}]}"#,
                r#"{"Items":[]}"#,
            ]),
        };
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .http_client(http.clone())
            .active_interval(Duration::from_secs(3600))
            .idle_interval(Duration::from_secs(3600))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        owner.block_on(async {
            nodes.ensure_discovery_started();
        });
        let original = nodes.discovery_runtime.load_full().unwrap();
        observer.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), http.entered.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            for _ in 0..5 {
                original.shutdown_probe.lock().unwrap().last_started = None;
                nodes.ensure_discovery_started();
                tokio::time::timeout(Duration::from_secs(1), async {
                    while original.shutdown_probe.lock().unwrap().pending {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            assert_eq!(http.requests.load(Ordering::SeqCst), 1);
            assert_eq!(
                http.dropped.available_permits(),
                0,
                "liveness demand must not restart pending SDK I/O"
            );
        });
        let (started, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release, release_rx) = std::sync::mpsc::channel();
        let release = ReleaseOnDrop(Some(release));
        let worker = owner.spawn(async move {
            started.send(()).unwrap();
            let _ = release_rx.recv();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        owner.shutdown_background();
        // A stopped runtime whose async worker is blocked cannot acknowledge.
        // Its existing I/O is still owned and must be dropped once it resumes.
        original.shutdown_probe.lock().unwrap().last_started = None;
        let started = Instant::now();
        assert!(!original.should_handoff_at(started));
        assert!(original.should_handoff_at(started + SHUTDOWN_PROBE_INTERVAL));
        observer.block_on(async {
            nodes.ensure_discovery_started();
            tokio::time::timeout(Duration::from_secs(1), async {
                while !nodes.initial_discovery_complete.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        });
        let replacement = nodes.discovery_runtime.load_full().unwrap();
        assert_eq!(replacement.id, observer.handle().id());
        drop(release);
        observer.block_on(async {
            let _ = tokio::time::timeout(Duration::from_secs(1), worker)
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(1), http.dropped.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            tokio::time::timeout(Duration::from_secs(1), async {
                while !original.stopped.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(Arc::ptr_eq(
                &nodes.discovery_runtime.load_full().unwrap(),
                &replacement
            ));
            assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("127.0.0.2"));
            assert_eq!(
                http.requests.load(Ordering::SeqCst),
                3,
                "cancelled old refresh must not resume scanning or publish"
            );
            tokio::time::timeout(Duration::from_secs(1), nodes.tasks.shutdown())
                .await
                .unwrap();
        });
        assert_eq!(nodes.tasks.active_task_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn topology_deadlines_apply_short_long_and_default_discovery_timeouts() {
        for configured in [
            Some(Duration::from_millis(250)),
            Some(Duration::from_secs(10)),
            None,
        ] {
            let http = HeldFirstTopology {
                requests: Arc::default(),
                entered: Arc::new(tokio::sync::Semaphore::new(0)),
                dropped: Arc::new(tokio::sync::Semaphore::new(0)),
                connect_timeouts: Arc::default(),
                following: DiscoveryResponseHttpClient::new(std::iter::empty::<String>()),
            };
            let mut builder = AlternatorConfig::builder()
                .seed_hosts(["127.0.0.1"])
                .port(8000)
                .http_client(http.clone())
                .timeout_config(
                    TimeoutConfig::builder()
                        .operation_timeout(Duration::from_millis(100))
                        .operation_attempt_timeout(Duration::from_millis(100))
                        .build(),
                );
            builder.set_discovery_timeout(configured);
            let nodes = LiveNodes::new(&builder.build()).unwrap();
            let started = tokio::time::Instant::now();
            let scan = tokio::spawn(async move {
                nodes
                    .scan_topology_table(SYSTEM_LOCAL_TABLE, &nodes.seed_urls[0], None)
                    .await
            });
            tokio::time::timeout(Duration::from_secs(1), http.entered.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            let effective = configured.unwrap_or(Duration::from_secs(5));
            let before_deadline = if effective > Duration::from_secs(5) {
                Duration::from_secs(6)
            } else {
                effective / 2
            };
            tokio::time::advance(before_deadline).await;
            assert!(
                !scan.is_finished(),
                "discovery deadline must not be silently capped"
            );
            assert_eq!(http.dropped.available_permits(), 0);
            tokio::time::advance(effective - before_deadline).await;
            assert!(
                tokio::time::timeout(Duration::from_secs(1), scan)
                    .await
                    .expect("topology scan must complete at its discovery deadline")
                    .unwrap()
                    .is_none()
            );
            let elapsed = started.elapsed();
            // Awaiting the task may auto-advance paused time. Permit only the
            // millisecond granularity of Tokio's timers beyond the deadline.
            assert!(
                elapsed <= effective + Duration::from_millis(1),
                "topology scan exceeded discovery deadline: {elapsed:?} > {effective:?}"
            );
            assert_eq!(http.dropped.available_permits(), 1);
            let settings = http.connect_timeouts.lock().unwrap();
            assert!(!settings.is_empty());
            assert!(
                settings
                    .iter()
                    .all(|timeout| *timeout == Some(Duration::from_secs(2).min(effective)))
            );
        }
    }

    #[derive(Clone, Debug)]
    struct DelayedTopology {
        entered: Arc<tokio::sync::Semaphore>,
        following: DiscoveryResponseHttpClient,
    }

    impl HttpClient for DelayedTopology {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            SharedHttpConnector::new(self.clone())
        }
    }

    impl HttpConnector for DelayedTopology {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let entered = self.entered.clone();
            let response = self.following.call(request);
            HttpConnectorFuture::new(async move {
                entered.add_permits(1);
                tokio::time::sleep(Duration::from_secs(1)).await;
                response.await
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn default_topology_deadline_ignores_short_data_attempt_timeout() {
        let http = DelayedTopology {
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
            following: DiscoveryResponseHttpClient::new([
                r#"{"Items":[{"rpc_address":{"S":"127.0.0.2"},"data_center":{"S":"dc1"},"rack":{"S":"rack1"}}]}"#,
                r#"{"Items":[]}"#,
            ]),
        };
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .http_client(http.clone())
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(Duration::from_millis(250))
                    .operation_attempt_timeout(Duration::from_millis(250))
                    .build(),
            )
            .active_interval(Duration::from_secs(3600))
            .idle_interval(Duration::from_secs(3600))
            .build();
        let nodes = LiveNodes::new(&config).unwrap();
        assert_eq!(nodes.discovery_timeout, None);
        let update = tokio::spawn({
            let nodes = nodes.clone();
            async move { nodes.update_live_nodes().await }
        });
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(1), http.entered.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            tokio::time::advance(Duration::from_millis(500)).await;
            assert!(!nodes.initial_discovery_complete.load(Ordering::Acquire));
            tokio::time::advance(Duration::from_millis(500)).await;
            tokio::task::yield_now().await;
        }
        update.await.unwrap();
        assert!(nodes.initial_discovery_complete.load(Ordering::Acquire));
        assert!(nodes.wait_for_initial_discovery().await);
        assert_eq!(nodes.live_nodes.load()[0].host_str(), Some("127.0.0.2"));
        assert_eq!(http.following.requests.lock().unwrap().len(), 2);
        nodes.tasks.shutdown().await;
    }

    #[tokio::test]
    async fn overflowing_readiness_budget_fails_without_starting_discovery() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        assert!(
            !nodes
                .wait_for_initial_discovery_with_timeouts(Duration::MAX, Duration::MAX)
                .await
        );
        assert!(nodes.discovery_runtime.load().is_none());
    }

    #[test]
    fn readiness_budget_uses_the_configured_discovery_timeout() {
        for (configured, expected) in [
            (Some(Duration::from_millis(250)), Duration::from_secs(2)),
            (Some(Duration::from_secs(10)), Duration::from_secs(41)),
            (None, Duration::from_secs(21)),
        ] {
            let mut config = test_config().to_builder();
            config.set_discovery_timeout(configured);
            let nodes = LiveNodes::new(&config.build()).unwrap();
            assert_eq!(nodes.initial_discovery_wait_timeout(), expected);
        }
    }

    #[test]
    fn shutdown_probe_handoff_is_latched_until_owner_acknowledges() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let owner = DiscoveryRuntime::new(runtime.handle().id());
        let started = Instant::now();
        assert!(!owner.should_handoff_at(started));
        assert!(owner.should_handoff_at(started + SHUTDOWN_PROBE_INTERVAL));
        assert!(owner.should_handoff_at(started));
        owner.acknowledge_probe();
        assert!(!owner.should_handoff_at(started));
        owner.stopped.store(true, Ordering::Release);
        assert!(owner.should_handoff_at(started));
    }

    #[test]
    fn parked_owner_hands_off_and_shutdown_joins_when_owner_is_driven_again() {
        let owner = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let observer = tokio::runtime::Runtime::new().unwrap();
        let nodes = LiveNodes::new(&test_config()).unwrap();
        owner.block_on(async {
            nodes.ensure_discovery_started();
            tokio::task::yield_now().await;
        });
        let registration = nodes.discovery_runtime.load_full().unwrap();
        let started = Instant::now();
        assert!(!registration.should_handoff_at(started));
        assert!(registration.should_handoff_at(started + SHUTDOWN_PROBE_INTERVAL));
        observer.block_on(async {
            nodes.ensure_discovery_started();
            assert_eq!(
                nodes.discovery_runtime.load_full().unwrap().id,
                observer.handle().id()
            );
            // Tokio cannot destroy an aborted task on a current-thread runtime
            // which is not being driven. Its join remains owned and pending.
            assert!(nodes.tasks.shutdown().now_or_never().is_none());
        });
        owner.block_on(tokio::task::yield_now());
        observer.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), nodes.tasks.shutdown())
                .await
                .unwrap();
        });
        assert_eq!(nodes.tasks.active_task_count(), 0);
    }

    #[test]
    fn repeated_handoffs_do_not_accumulate_discovery_tasks() {
        let first = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let second = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let nodes = LiveNodes::new(&test_config()).unwrap();
        first.block_on(async {
            nodes.ensure_discovery_started();
            tokio::task::yield_now().await;
        });
        for index in 0..8 {
            let previous = nodes.discovery_runtime.load_full().unwrap();
            let started = Instant::now();
            assert!(!previous.should_handoff_at(started));
            assert!(previous.should_handoff_at(started + SHUTDOWN_PROBE_INTERVAL));
            let (next, old) = if index % 2 == 0 {
                (&second, &first)
            } else {
                (&first, &second)
            };
            next.block_on(async {
                nodes.ensure_discovery_started();
                tokio::task::yield_now().await;
            });
            old.block_on(tokio::task::yield_now());
            assert_eq!(
                nodes.discovery_runtime.load_full().unwrap().id,
                next.handle().id()
            );
            assert!(previous.stopped.load(Ordering::Acquire));
            assert_eq!(nodes.tasks.active_task_count(), 1);
            assert!(nodes.tasks.registered_task_count() <= 2);
        }
        first.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), nodes.tasks.shutdown())
                .await
                .unwrap();
        });
        assert_eq!(nodes.tasks.active_task_count(), 0);
    }

    #[test]
    fn readiness_without_tokio_returns_failure_or_uses_cached_topology() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        assert!(!futures::executor::block_on(
            nodes.wait_for_initial_discovery()
        ));
        nodes.mark_initial_discovery_complete();
        assert!(futures::executor::block_on(
            nodes.wait_for_initial_discovery()
        ));
        nodes.tasks.stop();
        assert!(futures::executor::block_on(
            nodes.wait_for_initial_discovery()
        ));
    }

    #[test]
    fn stale_task_guard_does_not_clear_replacement_registration() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        let first_runtime = tokio::runtime::Runtime::new().unwrap();
        let second_runtime = tokio::runtime::Runtime::new().unwrap();
        let first = Arc::new(DiscoveryRuntime::new(first_runtime.handle().id()));
        let second = Arc::new(DiscoveryRuntime::new(second_runtime.handle().id()));

        nodes.discovery_runtime.store(Some(first.clone()));
        let stale_guard = DiscoveryTaskGuard {
            live_nodes: Arc::downgrade(&nodes),
            runtime: first,
        };
        nodes.discovery_runtime.store(Some(second.clone()));

        drop(stale_guard);

        let active = nodes.discovery_runtime.load_full().unwrap();
        assert!(Arc::ptr_eq(&active, &second));

        drop(DiscoveryTaskGuard {
            live_nodes: Arc::downgrade(&nodes),
            runtime: second,
        });
        assert!(nodes.discovery_runtime.load().is_none());
    }

    #[test]
    fn without_discovery_disables_discovery() {
        let config = AlternatorConfig::builder()
            .seed_hosts(["127.0.0.1"])
            .port(8000)
            .without_discovery()
            .build();

        assert!(LiveNodes::try_new_for_test(&config).unwrap().is_none());
        assert!(crate::AlternatorClient::try_from_conf(config).is_ok());
    }

    #[test]
    fn direct_routing_validates_the_configured_scheme() {
        for unsupported_scheme in ["ftp", "ws", "https://dynamodb.us-east-1.amazonaws.com/x"] {
            let config = AlternatorConfig::builder()
                .seed_hosts(["host"])
                .without_discovery()
                .scheme(unsupported_scheme)
                .build();

            assert!(matches!(
                LiveNodes::try_new_for_test(&config),
                Err(LiveNodesBuildError::InvalidScheme(scheme))
                    if scheme == unsupported_scheme
            ));
            assert!(crate::AlternatorClient::try_from_conf(config).is_err());
        }

        let direct_http = AlternatorConfig::builder()
            .seed_hosts(["host"])
            .without_discovery()
            .scheme("https")
            .build();
        assert!(LiveNodes::try_new_for_test(&direct_http).unwrap().is_none());
        assert!(crate::AlternatorClient::try_from_conf(direct_http).is_ok());
    }

    #[test]
    fn without_discovery_needs_a_seed_host() {
        for config in [
            AlternatorConfig::builder().without_discovery().build(),
            AlternatorConfig::builder()
                .seed_hosts(Vec::<String>::new())
                .without_discovery()
                .build(),
        ] {
            assert!(matches!(
                LiveNodes::try_new_for_test(&config),
                Err(LiveNodesBuildError::MissingRoutingTarget)
            ));
        }
    }

    #[test]
    fn custom_http_client_supports_valid_direct_schemes() {
        for (configured, stored, endpoint) in [
            ("custom", "custom", "custom://host"),
            ("CUSTOM", "CUSTOM", "custom://host"),
            ("a+b.c-1", "a+b.c-1", "a+b.c-1://host"),
            ("custom:", "custom", "custom://host"),
            ("custom://", "custom", "custom://host"),
        ] {
            let config = AlternatorConfig::builder()
                .scheme(configured)
                .seed_hosts(["host"])
                .without_discovery()
                .http_client(aws_smithy_http_client::Builder::new().build_http())
                .build();

            assert_eq!(config.scheme().as_deref(), Some(stored));
            assert_eq!(config.endpoint_url().as_deref(), Some(endpoint));
            assert!(LiveNodes::try_new_for_test(&config).unwrap().is_none());
            assert!(crate::AlternatorClient::try_from_conf(config).is_ok());
        }
    }

    #[test]
    fn custom_http_client_does_not_allow_malformed_direct_schemes() {
        for malformed in [
            "\nhttp",
            "ht\ntp",
            "\0http",
            "http\t",
            "1custom",
            "+custom",
            "éhttp",
            "http::",
            "http:/",
            "http///",
            "http:://",
            "https://dynamodb.us-east-1.amazonaws.com/x",
        ] {
            let config = AlternatorConfig::builder()
                .scheme(malformed)
                .seed_hosts(["host"])
                .without_discovery()
                .http_client(aws_smithy_http_client::Builder::new().build_http())
                .build();

            assert_eq!(config.endpoint_url(), None, "accepted {malformed:?}");
            assert!(matches!(
                LiveNodes::try_new_for_test(&config),
                Err(LiveNodesBuildError::InvalidScheme(_))
            ));
            assert!(crate::AlternatorClient::try_from_conf(config).is_err());
        }
    }

    #[test]
    fn discovery_rejects_custom_schemes_even_with_a_custom_http_client() {
        let config = AlternatorConfig::builder()
            .scheme("custom")
            .seed_hosts(["host"])
            .http_client(aws_smithy_http_client::Builder::new().build_http())
            .build();

        assert!(matches!(
            LiveNodes::try_new_for_test(&config),
            Err(LiveNodesBuildError::InvalidScheme(scheme)) if scheme == "custom"
        ));
        assert!(crate::AlternatorClient::try_from_conf(config).is_err());
    }

    #[test]
    fn missing_seed_hosts_are_rejected() {
        let config = AlternatorConfig::builder().build();

        assert!(matches!(
            LiveNodes::try_new_for_test(&config),
            Err(LiveNodesBuildError::MissingRoutingTarget)
        ));
    }

    #[test]
    #[should_panic(expected = "invalid seed host")]
    fn malformed_seed_host_fails_closed_even_when_another_seed_is_valid() {
        let config = AlternatorConfig::builder()
            .scheme("http")
            .seed_hosts(["127.0.0.1", "127.0.0.1:invalid"])
            .build();

        let _ = LiveNodes::new(&config);
    }

    #[test]
    fn seed_hosts_reject_url_components_and_ports() {
        for seed_host in [
            "127.0.0.1@dynamodb.us-east-1.amazonaws.com",
            "example.com/path",
            "example.com?query",
            "example.com#fragment",
            "example.com:8000",
        ] {
            let config = AlternatorConfig::builder()
                .scheme("http")
                .seed_hosts([seed_host])
                .build();

            assert!(matches!(
                LiveNodes::try_new_for_test(&config),
                Err(LiveNodesBuildError::InvalidSeedHost { .. })
            ));
        }
    }

    #[test]
    fn discovery_rejects_unsupported_and_url_shaped_schemes() {
        for scheme in ["ftp", "https://dynamodb.us-east-1.amazonaws.com/x"] {
            let config = AlternatorConfig::builder()
                .scheme(scheme)
                .seed_hosts(["127.0.0.1"])
                .build();

            assert!(matches!(
                LiveNodes::try_new_for_test(&config),
                Err(LiveNodesBuildError::InvalidScheme(invalid)) if invalid == scheme
            ));
        }
    }

    #[test]
    fn ipv6_address_parsing() {
        let config = AlternatorConfig::builder()
            .scheme("http")
            .port(8000)
            .seed_hosts(["::1"])
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
            .scheme("http")
            .port(8000)
            .seed_hosts(["::1"])
            .build();
        let nodes = LiveNodes::new(&config).unwrap();

        assert_eq!(nodes.seed_urls[0].host_str(), Some("[::1]"));
        assert_eq!(nodes.seed_urls[0].to_string(), "http://[::1]:8000/");
    }

    #[test]
    fn newer_refresh_without_a_result_does_not_suppress_older_result() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        let older_generation = nodes.begin_refresh();
        let _newer_generation = nodes.begin_refresh();
        let older_result = vec![Arc::new(Url::parse("http://127.0.0.2:1/").unwrap())];

        nodes.publish_live_nodes(older_generation, older_result.clone());

        assert_eq!(nodes.live_nodes.load().as_ref(), &older_result);
    }

    #[test]
    fn older_concurrent_refresh_does_not_overwrite_newer_result() {
        let nodes = LiveNodes::new(&test_config()).unwrap();
        let older_generation = nodes.begin_refresh();
        let newer_generation = nodes.begin_refresh();
        let older_result = vec![Arc::new(Url::parse("http://127.0.0.2:1/").unwrap())];
        let newer_result = vec![Arc::new(Url::parse("http://127.0.0.3:1/").unwrap())];

        nodes.publish_live_nodes(newer_generation, newer_result.clone());
        nodes.publish_live_nodes(older_generation, older_result);

        assert_eq!(nodes.live_nodes.load().as_ref(), &newer_result);
    }
}
