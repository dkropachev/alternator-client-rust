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

//! Owns cancellable discovery tasks without owning their Tokio runtime.

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared, join_all};
use std::future::Future;
use std::sync::Mutex;
use tokio::runtime::Handle;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;

type Completion = Shared<BoxFuture<'static, ()>>;

#[derive(Debug, Default)]
pub(crate) struct DiscoveryTasks {
    stopped: CancellationToken,
    tasks: Mutex<Vec<(AbortHandle, Completion)>>,
}

impl DiscoveryTasks {
    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.is_cancelled()
    }

    pub(crate) async fn cancelled(&self) {
        self.stopped.cancelled().await;
    }

    /// Registration and closure share a lock: a racing spawn is either owned
    /// by shutdown or rejected before it can start.
    pub(crate) fn spawn(
        &self,
        runtime: &Handle,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Option<AbortHandle> {
        let mut tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
        if self.is_stopped() {
            return None;
        }
        tasks.retain(|(task, _)| !task.is_finished());
        let task = runtime.spawn(future);
        let abort = task.abort_handle();
        let completion = async move {
            let _ = task.await;
        }
        .boxed()
        .shared();
        tasks.push((abort.clone(), completion));
        Some(abort)
    }

    #[cfg(test)]
    pub(crate) fn active_task_count(&self) -> usize {
        self.tasks
            .lock()
            .unwrap()
            .iter()
            .filter(|(task, _)| !task.is_finished())
            .count()
    }

    #[cfg(test)]
    pub(crate) fn registered_task_count(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }

    pub(crate) fn stop(&self) {
        let tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
        self.stopped.cancel();
        for (task, _) in tasks.iter() {
            task.abort();
        }
    }

    /// Shared completions keep joins available if a caller cancels its wait,
    /// and let concurrent callers all wait for the same task destruction.
    pub(crate) async fn shutdown(&self) {
        self.stop();
        let completions: Vec<_> = self
            .tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|(_, completion)| completion.clone())
            .collect();
        join_all(completions).await;
    }
}

impl Drop for DiscoveryTasks {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::time::timeout;

    struct Dropped(Arc<AtomicUsize>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_owns_tasks_racing_registration() {
        let tasks = Arc::new(DiscoveryTasks::default());
        let dropped = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(9));
        let mut spawners = Vec::new();
        for _ in 0..8 {
            let (tasks, dropped, barrier, runtime) = (
                tasks.clone(),
                dropped.clone(),
                barrier.clone(),
                Handle::current(),
            );
            spawners.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..50 {
                    let guard = Dropped(dropped.clone());
                    tasks.spawn(&runtime, async move {
                        let _guard = guard;
                        std::future::pending::<()>().await;
                    });
                }
            }));
        }
        barrier.wait();
        timeout(Duration::from_secs(1), tasks.shutdown())
            .await
            .expect("shutdown must join tasks racing registration");
        for spawner in spawners {
            spawner.join().unwrap();
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 400);
        assert!(tasks.is_stopped());
    }

    #[tokio::test]
    async fn cancelled_shutdown_wait_can_be_resumed_and_rejects_spawns() {
        let tasks = DiscoveryTasks::default();
        let dropped = Arc::new(AtomicUsize::new(0));
        let guard = Dropped(dropped.clone());
        tasks.spawn(&Handle::current(), async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        // Poll once to close/abort, then discard the wait before the runtime
        // can destroy the task. The next caller must still join its cleanup.
        assert!(tasks.shutdown().now_or_never().is_none());
        let rejected = Dropped(dropped.clone());
        assert!(
            tasks
                .spawn(&Handle::current(), async move {
                    let _guard = rejected;
                    panic!("closed registry must never poll a new discovery task");
                })
                .is_none()
        );
        timeout(Duration::from_secs(1), async {
            tokio::join!(tasks.shutdown(), tasks.shutdown());
        })
        .await
        .expect("concurrent shutdown callers must join task cleanup");
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
        timeout(Duration::from_secs(1), tasks.shutdown())
            .await
            .expect("completed shutdown must remain ready");
    }
}
