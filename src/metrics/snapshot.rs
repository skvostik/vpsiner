//! Latest-value metrics with server-computed rates, kept in memory for `/api/metrics/current`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::watch;

use crate::metrics::rate::rate;
use crate::model::{
    ContainerPoint, ContainerSample, GroupPoint, HostPoint, HostSample, MetricsSnapshot,
    TimestampMs,
};

/// Records older than this multiple of the collection interval are dropped on read.
const STALE_INTERVALS: u32 = 3;

fn now_ms() -> TimestampMs {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Default)]
struct Inner {
    host: Option<HostPoint>,
    previous_host: Option<HostSample>,
    containers: HashMap<String, ContainerPoint>,
    previous_containers: HashMap<String, ContainerSample>,
}

pub struct MetricsSnapshotState {
    inner: Mutex<Inner>,
    stale_after_ms: i64,
    /// Bumped on every record_* call so SSE subscribers know to re-read `current()`.
    revision_tx: watch::Sender<u64>,
}

impl MetricsSnapshotState {
    pub fn new(collect_interval: Duration) -> Self {
        let (revision_tx, _) = watch::channel(0);
        Self {
            inner: Mutex::new(Inner::default()),
            stale_after_ms: (collect_interval.as_millis() as i64) * i64::from(STALE_INTERVALS),
            revision_tx,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.revision_tx.subscribe()
    }

    pub fn record_host(&self, sample: &HostSample) {
        let mut inner = self.inner.lock().expect("snapshot state poisoned");
        let (net_rx_rate, net_tx_rate, disk_read_rate, disk_write_rate) = match inner.previous_host
        {
            Some(previous) => {
                let dt_ms = sample.ts - previous.ts;
                (
                    rate(sample.net_rx, previous.net_rx, dt_ms),
                    rate(sample.net_tx, previous.net_tx, dt_ms),
                    rate(sample.disk_read, previous.disk_read, dt_ms),
                    rate(sample.disk_write, previous.disk_write, dt_ms),
                )
            }
            None => (0.0, 0.0, 0.0, 0.0),
        };

        inner.host = Some(HostPoint {
            ts: sample.ts,
            cpu_pct: sample.cpu_pct,
            mem_used: sample.mem_used,
            mem_total: sample.mem_total,
            storage_used: sample.storage_used,
            storage_total: sample.storage_total,
            metrics_size: sample.metrics_size,
            logs_size: sample.logs_size,
            net_rx_rate,
            net_tx_rate,
            disk_read_rate,
            disk_write_rate,
        });
        inner.previous_host = Some(*sample);
        drop(inner);
        self.revision_tx.send_modify(|revision| *revision += 1);
    }

    /// Each batch carries the full set of sampled containers, so the map is replaced wholesale.
    pub fn record_containers(&self, samples: &[ContainerSample]) {
        let mut inner = self.inner.lock().expect("snapshot state poisoned");
        let mut containers = HashMap::with_capacity(samples.len());
        let mut previous_containers = HashMap::with_capacity(samples.len());

        for sample in samples {
            let (net_rx_rate, net_tx_rate, blk_read_rate, blk_write_rate) =
                match inner.previous_containers.get(&sample.cid) {
                    Some(previous) => {
                        let dt_ms = sample.ts - previous.ts;
                        (
                            rate(sample.net_rx, previous.net_rx, dt_ms),
                            rate(sample.net_tx, previous.net_tx, dt_ms),
                            rate(sample.blk_read, previous.blk_read, dt_ms),
                            rate(sample.blk_write, previous.blk_write, dt_ms),
                        )
                    }
                    None => (0.0, 0.0, 0.0, 0.0),
                };

            containers.insert(
                sample.cid.clone(),
                ContainerPoint {
                    ts: sample.ts,
                    log_group: sample.log_group.clone(),
                    cpu_pct: sample.cpu_pct,
                    mem_used: sample.mem_used,
                    mem_limit: sample.mem_limit,
                    net_rx_rate,
                    net_tx_rate,
                    blk_read_rate,
                    blk_write_rate,
                },
            );
            previous_containers.insert(sample.cid.clone(), sample.clone());
        }

        inner.containers = containers;
        inner.previous_containers = previous_containers;
        drop(inner);
        self.revision_tx.send_modify(|revision| *revision += 1);
    }

    pub fn current(&self) -> MetricsSnapshot {
        self.current_at(now_ms())
    }

    fn current_at(&self, now: TimestampMs) -> MetricsSnapshot {
        let inner = self.inner.lock().expect("snapshot state poisoned");
        let cutoff = now - self.stale_after_ms;

        let host = inner.host.filter(|snapshot| snapshot.ts >= cutoff);
        let containers: HashMap<String, ContainerPoint> = inner
            .containers
            .iter()
            .filter(|(_, snapshot)| snapshot.ts >= cutoff)
            .map(|(cid, snapshot)| (cid.clone(), snapshot.clone()))
            .collect();

        let mut log_groups: HashMap<String, GroupPoint> = HashMap::new();
        for snapshot in containers.values() {
            let group = log_groups
                .entry(snapshot.log_group.clone())
                .or_insert_with(|| GroupPoint {
                    ts: snapshot.ts,
                    ..GroupPoint::default()
                });
            group.ts = group.ts.max(snapshot.ts);
            group.cpu_pct += snapshot.cpu_pct;
            group.mem_used = group.mem_used.saturating_add(snapshot.mem_used);
            group.mem_limit = group.mem_limit.saturating_add(snapshot.mem_limit);
            group.net_rx_rate += snapshot.net_rx_rate;
            group.net_tx_rate += snapshot.net_tx_rate;
            group.blk_read_rate += snapshot.blk_read_rate;
            group.blk_write_rate += snapshot.blk_write_rate;
        }

        MetricsSnapshot {
            host,
            containers,
            log_groups,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> MetricsSnapshotState {
        MetricsSnapshotState::new(Duration::from_secs(10))
    }

    fn host_sample(ts: TimestampMs, net_rx: u64) -> HostSample {
        HostSample {
            ts,
            cpu_pct: 12.5,
            mem_used: 100,
            mem_total: 200,
            storage_used: 300,
            storage_total: 400,
            metrics_size: 500,
            logs_size: 600,
            net_rx,
            net_tx: 0,
            disk_read: 0,
            disk_write: 0,
        }
    }

    fn container_sample(
        ts: TimestampMs,
        cid: &str,
        log_group: &str,
        net_rx: u64,
    ) -> ContainerSample {
        ContainerSample {
            ts,
            log_group: log_group.into(),
            cid: cid.into(),
            cpu_pct: 5.0,
            mem_used: 100,
            mem_limit: 200,
            net_rx,
            net_tx: 0,
            blk_read: 0,
            blk_write: 0,
        }
    }

    #[test]
    fn first_sample_has_zero_rates() {
        let state = state();
        state.record_host(&host_sample(10_000, 5_000));
        assert_eq!(state.current_at(10_000).host.unwrap().net_rx_rate, 0.0);
    }

    #[test]
    fn computes_rate_between_consecutive_samples() {
        let state = state();
        state.record_host(&host_sample(10_000, 5_000));
        state.record_host(&host_sample(20_000, 25_000));
        assert_eq!(state.current_at(20_000).host.unwrap().net_rx_rate, 2_000.0);
    }

    #[test]
    fn counter_reset_yields_zero_rate() {
        let state = state();
        state.record_host(&host_sample(10_000, 25_000));
        state.record_host(&host_sample(20_000, 5_000));
        assert_eq!(state.current_at(20_000).host.unwrap().net_rx_rate, 0.0);
    }

    #[test]
    fn stale_host_sample_is_dropped() {
        let state = state();
        state.record_host(&host_sample(10_000, 5_000));
        assert!(state.current_at(40_001).host.is_none());
        assert!(state.current_at(40_000).host.is_some());
    }

    #[test]
    fn stale_container_samples_are_dropped() {
        let state = state();
        state.record_containers(&[container_sample(10_000, "abc", "web", 0)]);
        assert!(state.current_at(40_001).containers.is_empty());
        assert!(state.current_at(40_001).log_groups.is_empty());
        assert!(!state.current_at(40_000).containers.is_empty());
    }

    #[test]
    fn batch_replaces_previously_seen_containers() {
        let state = state();
        state.record_containers(&[
            container_sample(10_000, "abc", "web", 0),
            container_sample(10_000, "def", "web", 0),
        ]);
        state.record_containers(&[container_sample(20_000, "abc", "web", 0)]);

        let snapshot = state.current_at(20_000);
        assert!(snapshot.containers.contains_key("abc"));
        assert!(!snapshot.containers.contains_key("def"));
    }

    #[test]
    fn container_rates_are_tracked_per_container() {
        let state = state();
        state.record_containers(&[
            container_sample(10_000, "abc", "web", 1_000),
            container_sample(10_000, "def", "web", 5_000),
        ]);
        state.record_containers(&[
            container_sample(20_000, "abc", "web", 2_000),
            container_sample(20_000, "def", "web", 25_000),
        ]);

        let snapshot = state.current_at(20_000);
        assert_eq!(snapshot.containers["abc"].net_rx_rate, 100.0);
        assert_eq!(snapshot.containers["def"].net_rx_rate, 2_000.0);
    }

    #[test]
    fn log_groups_sum_their_containers() {
        let state = state();
        state.record_containers(&[
            container_sample(10_000, "abc", "web", 0),
            container_sample(10_000, "def", "web", 0),
            container_sample(10_000, "ghi", "db", 0),
        ]);

        let snapshot = state.current_at(10_000);
        assert_eq!(snapshot.log_groups["web"].cpu_pct, 10.0);
        assert_eq!(snapshot.log_groups["web"].mem_used, 200);
        assert_eq!(snapshot.log_groups["db"].cpu_pct, 5.0);
        assert_eq!(snapshot.log_groups["db"].mem_used, 100);
    }

    #[test]
    fn empty_state_is_a_valid_snapshot() {
        let snapshot = state().current_at(10_000);
        assert!(snapshot.host.is_none());
        assert!(snapshot.containers.is_empty());
        assert!(snapshot.log_groups.is_empty());
    }
}
