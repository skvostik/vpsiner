//! Notifies SSE endpoints when a log group's buffered lines are durably persisted, so
//! `/api/stream/logs` and `/api/stream/logs/{log_group}` can react instead of polling.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::watch;

pub struct LogFlushWatcher {
    groups: Mutex<HashMap<String, watch::Sender<u64>>>,
    /// Bumped alongside every per-group notification, for listeners that care about "something
    /// changed somewhere" without subscribing to every group individually (the groups-list SSE).
    any: watch::Sender<u64>,
}

impl Default for LogFlushWatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl LogFlushWatcher {
    pub fn new() -> Self {
        let (any, _) = watch::channel(0);
        Self {
            groups: Mutex::new(HashMap::new()),
            any,
        }
    }

    pub fn subscribe(&self, log_group: &str) -> watch::Receiver<u64> {
        let mut groups = self.groups.lock().expect("flush watcher lock poisoned");
        groups
            .entry(log_group.to_string())
            .or_insert_with(|| watch::channel(0).0)
            .subscribe()
    }

    pub fn subscribe_any(&self) -> watch::Receiver<u64> {
        self.any.subscribe()
    }

    /// Called after a group's buffered lines are durably persisted (append + checkpoint both ok).
    pub fn notify(&self, log_group: &str) {
        {
            let mut groups = self.groups.lock().expect("flush watcher lock poisoned");
            let tx = groups
                .entry(log_group.to_string())
                .or_insert_with(|| watch::channel(0).0);
            tx.send_modify(|revision| *revision += 1);
        }
        self.any.send_modify(|revision| *revision += 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notifies_only_the_named_group_and_the_global_signal() {
        let watcher = LogFlushWatcher::new();
        let project_web = watcher.subscribe("project-web");
        let project_db = watcher.subscribe("project-db");
        let any = watcher.subscribe_any();

        watcher.notify("project-web");

        assert!(project_web.has_changed().unwrap());
        assert!(!project_db.has_changed().unwrap());
        assert!(any.has_changed().unwrap());
    }
}
