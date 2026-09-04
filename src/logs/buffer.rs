use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::error::AppResult;
use crate::logs::flush_watcher::LogFlushWatcher;
use crate::logs::store::LogStore;
use crate::metadata::MetadataStore;
use crate::model::{container_id::ContainerId, logs::LogLine};

/// Owns in-memory buffering, per-container two-level dedup, and per-service debounced flushing to
/// the `LogStore`/`MetadataStore`. Each service flushes independently so a burst in one
/// service never blocks or piles up with another.
pub struct LogBuffer {
    inner: Arc<Inner>,
}

struct Inner {
    logs: Arc<dyn LogStore>,
    metadata: Arc<dyn MetadataStore>,
    flush_watcher: Arc<LogFlushWatcher>,
    debounce: Duration,
    keep_alive: Duration,
    services: Mutex<HashMap<String, ServiceHandle>>,
}

struct ServiceHandle {
    state: Arc<Mutex<ServiceState>>,
    flush_tx: Option<mpsc::Sender<()>>,
}

#[derive(Default)]
struct ServiceState {
    lines: Vec<LogLine>,
    // Last accepted (ts, content hash) per container, seeded from MetadataStore on startup.
    checkpoints: HashMap<ContainerId, (i64, u64)>,
}

impl LogBuffer {
    /// Preloads every known (service, cid) checkpoint so dedup survives restarts.
    pub async fn new(
        logs: Arc<dyn LogStore>,
        metadata: Arc<dyn MetadataStore>,
        flush_watcher: Arc<LogFlushWatcher>,
        debounce: Duration,
        keep_alive: Duration,
    ) -> AppResult<Self> {
        tracing::info!(debounce = ?debounce, keep_alive = ?keep_alive, "initializing log buffer");
        tracing::info!("preloading log buffer checkpoints from metadata store");
        let mut seeded: HashMap<String, ServiceState> = HashMap::new();
        for entry in metadata.list_log_checkpoints().await? {
            seeded
                .entry(entry.service)
                .or_default()
                .checkpoints
                .insert(entry.cid, (entry.checkpoint.ts, entry.checkpoint.line_hash));
        }

        let inner = Arc::new(Inner {
            logs,
            metadata,
            flush_watcher,
            debounce,
            keep_alive,
            services: Mutex::new(HashMap::new()),
        });
        let buffer = Self { inner };
        for (service, state) in seeded {
            buffer.seed_service(service, state);
        }
        Ok(buffer)
    }

    fn seed_service(&self, service: String, state: ServiceState) {
        let state = Arc::new(Mutex::new(state));
        self.inner.services.lock().unwrap().insert(
            service,
            ServiceHandle {
                state,
                flush_tx: None,
            },
        );
    }

    fn service_state(&self, service: &str) -> Arc<Mutex<ServiceState>> {
        let mut services = self.inner.services.lock().unwrap();
        if !services.contains_key(service) {
            services.insert(
                service.to_string(),
                ServiceHandle {
                    state: Arc::new(Mutex::new(ServiceState::default())),
                    flush_tx: None,
                },
            );
        }
        services
            .get(service)
            .expect("service entry must exist")
            .state
            .clone()
    }

    fn schedule_flush(&self, service: &str) {
        let (state, flush_tx) = {
            let mut services = self.inner.services.lock().unwrap();
            let handle = services
                .get_mut(service)
                .expect("service entry must exist before scheduling flush");
            if handle.flush_tx.is_none() {
                let (flush_tx, flush_rx) = mpsc::channel(1);
                tracing::info!(service=%service, "spawning log flush worker");
                spawn_flush_worker(
                    Arc::downgrade(&self.inner),
                    service.to_string(),
                    handle.state.clone(),
                    flush_rx,
                );
                handle.flush_tx = Some(flush_tx);
            }
            (
                handle.state.clone(),
                handle
                    .flush_tx
                    .clone()
                    .expect("flush sender must exist after worker spawn"),
            )
        };

        // Keep state alive until the scheduling call completes.
        drop(state);
        let _ = flush_tx.try_send(());
    }

    /// Buffers `line` unless it's dropped by the two-level dedup check, and schedules a
    /// debounced flush for its service: strictly-older lines are dropped, exact repeats at the
    /// boundary timestamp are dropped, anything else (including a distinct line sharing the
    /// same timestamp) is accepted.
    pub fn push(&self, line: LogLine) {
        let service = line.service.clone();
        let state = self.service_state(&service);
        {
            let mut guard = state.lock().unwrap();
            let hash = hash_line(&line);
            if let Some(&(ts_last, hash_last)) = guard.checkpoints.get(&line.cid) {
                if line.ts < ts_last || (line.ts == ts_last && hash == hash_last) {
                    return;
                }
            }
            guard.checkpoints.insert(line.cid, (line.ts, hash));
            guard.lines.push(line);
        }
        self.schedule_flush(&service);
    }

    /// Flushes every service immediately, bypassing the debounce. Used on shutdown.
    pub async fn flush_all(&self) {
        let handles: Vec<(String, Arc<Mutex<ServiceState>>)> = self
            .inner
            .services
            .lock()
            .unwrap()
            .iter()
            .map(|(service, handle)| (service.clone(), handle.state.clone()))
            .collect();
        for (service, state) in handles {
            self.inner.flush_service(&service, &state).await;
        }
    }
}

impl Inner {
    async fn flush_service(&self, service: &str, state: &Arc<Mutex<ServiceState>>) {
        let (lines, checkpoints) = {
            let mut guard = state.lock().unwrap();
            if guard.lines.is_empty() {
                return;
            }
            let lines = std::mem::take(&mut guard.lines);
            let mut cids: Vec<ContainerId> = lines.iter().map(|line| line.cid).collect();
            cids.sort_unstable();
            cids.dedup();
            let checkpoints: Vec<(ContainerId, i64, u64)> = cids
                .into_iter()
                .filter_map(|cid| {
                    guard
                        .checkpoints
                        .get(&cid)
                        .map(|&(ts, hash)| (cid, ts, hash))
                })
                .collect();
            (lines, checkpoints)
        };

        let retry_lines = lines.clone();
        if let Err(err) = self.logs.append(service, lines).await {
            tracing::warn!(service = %service, error = %err, "failed to persist container logs; scheduling retry");

            // Preserve failed lines and keep their relative order ahead of any newer lines.
            {
                let mut guard = state.lock().unwrap();
                if !guard.lines.is_empty() {
                    let mut buffered = std::mem::take(&mut guard.lines);
                    let mut retry = retry_lines;
                    retry.append(&mut buffered);
                    guard.lines = retry;
                } else {
                    guard.lines = retry_lines;
                }
            }

            self.request_service_flush(service);
            return; // don't advance checkpoints for data that didn't land
        }
        for (cid, ts, line_hash) in checkpoints {
            if let Err(err) = self
                .metadata
                .advance_log_checkpoint(
                    service,
                    cid,
                    crate::metadata::LogCheckpoint { ts, line_hash },
                )
                .await
            {
                tracing::error!(service = %service, cid = %cid, error = %err, "failed to persist last-received checkpoint");
            }
        }
        self.flush_watcher.notify(service);
    }

    fn request_service_flush(&self, service: &str) {
        let sender = self
            .services
            .lock()
            .unwrap()
            .get(service)
            .and_then(|handle| handle.flush_tx.clone());
        if let Some(flush_tx) = sender {
            let _ = flush_tx.try_send(());
        }
    }
}

fn spawn_flush_worker(
    inner: Weak<Inner>,
    service: String,
    state: Arc<Mutex<ServiceState>>,
    mut flush_rx: mpsc::Receiver<()>,
) {
    tokio::spawn(async move {
        loop {
            let Some(inner_ref) = inner.upgrade() else {
                return;
            };

            let keep_alive = inner_ref.keep_alive;
            let debounce = inner_ref.debounce;
            drop(inner_ref);

            match tokio::time::timeout(keep_alive, flush_rx.recv()).await {
                Ok(Some(_)) => {
                    tokio::time::sleep(debounce).await;

                    // Drain any additional requests that arrived while we were sleeping; they're
                    // coalesced into the flush we're about to run.
                    while flush_rx.try_recv().is_ok() {}

                    let Some(inner_ref) = inner.upgrade() else {
                        return;
                    };
                    inner_ref.flush_service(&service, &state).await;
                }
                Ok(None) => return,
                Err(_) => {
                    let Some(inner_ref) = inner.upgrade() else {
                        return;
                    };

                    let mut services = inner_ref.services.lock().unwrap();
                    let Some(handle) = services.get_mut(&service) else {
                        return;
                    };

                    if handle.state.lock().unwrap().lines.is_empty() {
                        handle.flush_tx = None;
                        tracing::debug!(service=%service, keep_alive=?keep_alive, "stopping idle log flush worker");
                        return;
                    }
                }
            }
        }
    });
}

/// Cheap, non-cryptographic content hash used for exact-boundary dedup.
fn hash_line(line: &LogLine) -> u64 {
    let mut hasher = DefaultHasher::new();
    line.stream.hash(&mut hasher);
    line.line.hash(&mut hasher);
    hasher.finish()
}
