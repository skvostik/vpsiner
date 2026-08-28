use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::error::AppResult;
use crate::logs::metadata::LogMetadataStore;
use crate::logs::store::LogStore;
use crate::model::LogLine;

/// Owns in-memory buffering, per-container two-level dedup, and per-group debounced flushing to
/// the `LogStore`/`LogMetadataStore`. Each log group flushes independently so a burst in one
/// group never blocks or piles up with another.
pub struct LogBuffer {
    inner: Arc<Inner>,
}

struct Inner {
    logs: Arc<dyn LogStore>,
    metadata: Arc<dyn LogMetadataStore>,
    debounce: Duration,
    keep_alive: Duration,
    groups: Mutex<HashMap<String, GroupHandle>>,
}

struct GroupHandle {
    state: Arc<Mutex<GroupState>>,
    flush_tx: Option<mpsc::Sender<()>>,
}

#[derive(Default)]
struct GroupState {
    lines: Vec<LogLine>,
    // Last accepted (ts, content hash) per container, seeded from LogMetadataStore on startup.
    checkpoints: HashMap<String, (i64, u64)>,
}

impl LogBuffer {
    /// Preloads every known (log_group, container_id) checkpoint so dedup survives restarts.
    pub async fn new(
        logs: Arc<dyn LogStore>,
        metadata: Arc<dyn LogMetadataStore>,
        debounce: Duration,
        keep_alive: Duration,
    ) -> AppResult<Self> {
        tracing::info!(debounce = ?debounce, keep_alive = ?keep_alive, "initializing log buffer");
        tracing::info!("preloading log buffer checkpoints from metadata store");
        let mut seeded: HashMap<String, GroupState> = HashMap::new();
        for (log_group, container_id, checkpoint) in metadata.list_checkpoints().await? {
            seeded
                .entry(log_group)
                .or_default()
                .checkpoints
                .insert(container_id, (checkpoint.ts, checkpoint.line_hash));
        }

        let inner = Arc::new(Inner {
            logs,
            metadata,
            debounce,
            keep_alive,
            groups: Mutex::new(HashMap::new()),
        });
        let buffer = Self { inner };
        for (log_group, state) in seeded {
            buffer.seed_group(log_group, state);
        }
        Ok(buffer)
    }

    fn seed_group(&self, log_group: String, state: GroupState) {
        let state = Arc::new(Mutex::new(state));
        self.inner.groups.lock().unwrap().insert(
            log_group,
            GroupHandle {
                state,
                flush_tx: None,
            },
        );
    }

    fn group_state(&self, log_group: &str) -> Arc<Mutex<GroupState>> {
        let mut groups = self.inner.groups.lock().unwrap();
        if !groups.contains_key(log_group) {
            groups.insert(
                log_group.to_string(),
                GroupHandle {
                    state: Arc::new(Mutex::new(GroupState::default())),
                    flush_tx: None,
                },
            );
        }
        groups
            .get(log_group)
            .expect("group entry must exist")
            .state
            .clone()
    }

    fn schedule_flush(&self, log_group: &str) {
        let (state, flush_tx) = {
            let mut groups = self.inner.groups.lock().unwrap();
            let handle = groups
                .get_mut(log_group)
                .expect("group entry must exist before scheduling flush");
            if handle.flush_tx.is_none() {
                let (flush_tx, flush_rx) = mpsc::channel(1);
                tracing::info!(log_group=%log_group, "spawning log flush worker");
                spawn_flush_worker(
                    Arc::downgrade(&self.inner),
                    log_group.to_string(),
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
    /// debounced flush for its group: strictly-older lines are dropped, exact repeats at the
    /// boundary timestamp are dropped, anything else (including a distinct line sharing the
    /// same timestamp) is accepted.
    pub fn push(&self, line: LogLine) {
        let log_group = line.log_group.clone();
        let state = self.group_state(&log_group);
        {
            let mut guard = state.lock().unwrap();
            let hash = hash_line(&line);
            if let Some(&(ts_last, hash_last)) = guard.checkpoints.get(&line.cid) {
                if line.ts < ts_last || (line.ts == ts_last && hash == hash_last) {
                    return;
                }
            }
            guard.checkpoints.insert(line.cid.clone(), (line.ts, hash));
            guard.lines.push(line);
        }
        self.schedule_flush(&log_group);
    }

    /// Flushes every group immediately, bypassing the debounce. Used on shutdown.
    pub async fn flush_all(&self) {
        let handles: Vec<(String, Arc<Mutex<GroupState>>)> = self
            .inner
            .groups
            .lock()
            .unwrap()
            .iter()
            .map(|(group, handle)| (group.clone(), handle.state.clone()))
            .collect();
        for (group, state) in handles {
            self.inner.flush_group(&group, &state).await;
        }
    }
}

impl Inner {
    async fn flush_group(&self, log_group: &str, state: &Arc<Mutex<GroupState>>) {
        let (lines, checkpoints) = {
            let mut guard = state.lock().unwrap();
            if guard.lines.is_empty() {
                return;
            }
            let lines = std::mem::take(&mut guard.lines);
            let mut cids: Vec<&str> = lines.iter().map(|line| line.cid.as_str()).collect();
            cids.sort_unstable();
            cids.dedup();
            let checkpoints: Vec<(String, i64, u64)> = cids
                .into_iter()
                .filter_map(|cid| {
                    guard
                        .checkpoints
                        .get(cid)
                        .map(|&(ts, hash)| (cid.to_string(), ts, hash))
                })
                .collect();
            (lines, checkpoints)
        };

        let retry_lines = lines.clone();
        if let Err(err) = self.logs.append(log_group, lines).await {
            tracing::warn!(log_group = %log_group, error = %err, "failed to persist container logs; scheduling retry");

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

            self.request_group_flush(log_group);
            return; // don't advance checkpoints for data that didn't land
        }
        for (cid, ts, line_hash) in checkpoints {
            if let Err(err) = self
                .metadata
                .record_received(log_group, &cid, ts, line_hash)
                .await
            {
                tracing::error!(log_group = %log_group, cid = %cid, error = %err, "failed to persist last-received checkpoint");
            }
        }
    }

    fn request_group_flush(&self, log_group: &str) {
        let sender = self
            .groups
            .lock()
            .unwrap()
            .get(log_group)
            .and_then(|handle| handle.flush_tx.clone());
        if let Some(flush_tx) = sender {
            let _ = flush_tx.try_send(());
        }
    }
}

fn spawn_flush_worker(
    inner: Weak<Inner>,
    log_group: String,
    state: Arc<Mutex<GroupState>>,
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
                    inner_ref.flush_group(&log_group, &state).await;
                }
                Ok(None) => return,
                Err(_) => {
                    let Some(inner_ref) = inner.upgrade() else {
                        return;
                    };

                    let mut groups = inner_ref.groups.lock().unwrap();
                    let Some(handle) = groups.get_mut(&log_group) else {
                        return;
                    };

                    if handle.state.lock().unwrap().lines.is_empty() {
                        handle.flush_tx = None;
                        tracing::info!(log_group=%log_group, keep_alive=?keep_alive, "stopping idle log flush worker");
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
