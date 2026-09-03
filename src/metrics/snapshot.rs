//! Latest-value metrics with server-computed rates, kept in memory for `/api/metrics/current`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::watch;

use crate::metrics::rate::rate;
use crate::model::{
    ContainerPoint, ContainerSample, ContainersSnapshot, GroupPoint, HostPoint, HostSample,
    MetricsSnapshot, TimestampMs,
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
struct HostState {
    current: Option<HostPoint>,
}

#[derive(Default)]
struct ContainersState {
    current: HashMap<String, ContainerPoint>,
    previous: HashMap<String, ContainerSample>,
}

/// Host and container samples are recorded by independent tasks, so each half keeps its own
/// lock and revision channel and is never blocked or woken by the other.
pub struct MetricsSnapshotState {
    host: Mutex<HostState>,
    containers: Mutex<ContainersState>,
    stale_after_ms: i64,
    host_revision_tx: watch::Sender<u64>,
    containers_revision_tx: watch::Sender<u64>,
}

impl MetricsSnapshotState {
    pub fn new(collect_interval: Duration) -> Self {
        let (host_revision_tx, _) = watch::channel(0);
        let (containers_revision_tx, _) = watch::channel(0);
        Self {
            host: Mutex::new(HostState::default()),
            containers: Mutex::new(ContainersState::default()),
            stale_after_ms: (collect_interval.as_millis() as i64) * i64::from(STALE_INTERVALS),
            host_revision_tx,
            containers_revision_tx,
        }
    }

    pub fn subscribe_host(&self) -> watch::Receiver<u64> {
        self.host_revision_tx.subscribe()
    }

    pub fn subscribe_containers(&self) -> watch::Receiver<u64> {
        self.containers_revision_tx.subscribe()
    }

    pub fn record_host(&self, sample: &HostSample) {
        let mut state = self.host.lock().expect("snapshot state poisoned");
        state.current = Some(HostPoint {
            ts: sample.ts,
            cpu_pct: sample.cpu_pct_mill as f64 / 1_000.0,
            mem_used: sample.mem_used,
            mem_total: sample.mem_total,
            storage_used: sample.storage_used,
            storage_total: sample.storage_total,
            metrics_size: sample.metrics_size,
            logs_size: sample.logs_size,
            net_rx_rate: sample.net_rx_rate_mill.map(|rate| rate as f64 / 1_000.0),
            net_tx_rate: sample.net_tx_rate_mill.map(|rate| rate as f64 / 1_000.0),
            disk_read_rate: sample.disk_read_rate_mill.map(|rate| rate as f64 / 1_000.0),
            disk_write_rate: sample
                .disk_write_rate_mill
                .map(|rate| rate as f64 / 1_000.0),
        });
        drop(state);
        self.host_revision_tx.send_modify(|revision| *revision += 1);
    }

    /// Each batch carries the full set of sampled containers, so the map is replaced wholesale.
    pub fn record_containers(&self, samples: &[ContainerSample]) {
        let mut state = self.containers.lock().expect("snapshot state poisoned");
        let mut current = HashMap::with_capacity(samples.len());
        let mut previous = HashMap::with_capacity(samples.len());

        for sample in samples {
            let (net_rx_rate, net_tx_rate, blk_read_rate, blk_write_rate) =
                match state.previous.get(&sample.cid) {
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

            current.insert(
                sample.cid.clone(),
                ContainerPoint {
                    ts: sample.ts,
                    service: sample.service.clone(),
                    cpu_pct: sample.cpu_pct,
                    mem_used: sample.mem_used,
                    mem_limit: sample.mem_limit,
                    net_rx_rate,
                    net_tx_rate,
                    blk_read_rate,
                    blk_write_rate,
                },
            );
            previous.insert(sample.cid.clone(), sample.clone());
        }

        state.current = current;
        state.previous = previous;
        drop(state);
        self.containers_revision_tx
            .send_modify(|revision| *revision += 1);
    }

    pub fn current_host(&self) -> Option<HostPoint> {
        self.current_host_at(now_ms())
    }

    pub fn current_containers(&self) -> ContainersSnapshot {
        self.current_containers_at(now_ms())
    }

    pub fn current(&self) -> MetricsSnapshot {
        self.current_at(now_ms())
    }

    fn cutoff(&self, now: TimestampMs) -> TimestampMs {
        now - self.stale_after_ms
    }

    fn current_host_at(&self, now: TimestampMs) -> Option<HostPoint> {
        let cutoff = self.cutoff(now);
        self.host
            .lock()
            .expect("snapshot state poisoned")
            .current
            .filter(|point| point.ts >= cutoff)
    }

    fn current_containers_at(&self, now: TimestampMs) -> ContainersSnapshot {
        let cutoff = self.cutoff(now);
        let containers: HashMap<String, ContainerPoint> = self
            .containers
            .lock()
            .expect("snapshot state poisoned")
            .current
            .iter()
            .filter(|(_, point)| point.ts >= cutoff)
            .map(|(cid, point)| (cid.clone(), point.clone()))
            .collect();

        let mut services: HashMap<String, GroupPoint> = HashMap::new();
        for point in containers.values() {
            let service = services
                .entry(point.service.clone())
                .or_insert_with(|| GroupPoint {
                    ts: point.ts,
                    ..GroupPoint::default()
                });
            service.ts = service.ts.max(point.ts);
            service.cpu_pct += point.cpu_pct;
            service.mem_used = service.mem_used.saturating_add(point.mem_used);
            service.mem_limit = service.mem_limit.saturating_add(point.mem_limit);
            service.net_rx_rate += point.net_rx_rate;
            service.net_tx_rate += point.net_tx_rate;
            service.blk_read_rate += point.blk_read_rate;
            service.blk_write_rate += point.blk_write_rate;
        }

        ContainersSnapshot {
            containers,
            services,
        }
    }

    fn current_at(&self, now: TimestampMs) -> MetricsSnapshot {
        let host = self.current_host_at(now);
        let ContainersSnapshot {
            containers,
            services,
        } = self.current_containers_at(now);
        MetricsSnapshot {
            host,
            containers,
            services,
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
            cpu_pct_mill: 12_500,
            mem_used: 100,
            mem_total: 200,
            storage_used: 300,
            storage_total: 400,
            metrics_size: 500,
            logs_size: 600,
            net_rx_rate_mill: Some(net_rx),
            net_tx_rate_mill: Some(0),
            disk_read_rate_mill: Some(0),
            disk_write_rate_mill: Some(0),
        }
    }

    fn container_sample(ts: TimestampMs, cid: &str, service: &str, net_rx: u64) -> ContainerSample {
        ContainerSample {
            ts,
            service: service.into(),
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
    fn host_snapshot_uses_stored_bucketed_rate() {
        let state = state();
        state.record_host(&host_sample(10_000, 5_000));
        assert_eq!(
            state.current_at(10_000).host.unwrap().net_rx_rate,
            Some(5.0)
        );
    }

    #[test]
    fn host_snapshot_replaces_previous_bucket() {
        let state = state();
        state.record_host(&host_sample(10_000, 5_000));
        state.record_host(&host_sample(20_000, 2_000_000));
        assert_eq!(
            state.current_at(20_000).host.unwrap().net_rx_rate,
            Some(2_000.0)
        );
    }

    #[test]
    fn host_snapshot_does_not_recompute_rates() {
        let state = state();
        state.record_host(&host_sample(10_000, 25_000));
        state.record_host(&host_sample(20_000, 5_000));
        assert_eq!(
            state.current_at(20_000).host.unwrap().net_rx_rate,
            Some(5.0)
        );
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
        assert!(state.current_at(40_001).services.is_empty());
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
    fn services_sum_their_containers() {
        let state = state();
        state.record_containers(&[
            container_sample(10_000, "abc", "web", 0),
            container_sample(10_000, "def", "web", 0),
            container_sample(10_000, "ghi", "db", 0),
        ]);

        let snapshot = state.current_at(10_000);
        assert_eq!(snapshot.services["web"].cpu_pct, 10.0);
        assert_eq!(snapshot.services["web"].mem_used, 200);
        assert_eq!(snapshot.services["db"].cpu_pct, 5.0);
        assert_eq!(snapshot.services["db"].mem_used, 100);
    }

    #[test]
    fn empty_state_is_a_valid_snapshot() {
        let snapshot = state().current_at(10_000);
        assert!(snapshot.host.is_none());
        assert!(snapshot.containers.is_empty());
        assert!(snapshot.services.is_empty());
    }

    #[test]
    fn halves_apply_staleness_independently() {
        let state = state();
        state.record_host(&host_sample(10_000, 5_000));
        state.record_containers(&[container_sample(30_000, "abc", "web", 0)]);

        assert!(state.current_host_at(40_001).is_none());
        assert!(!state.current_containers_at(40_001).containers.is_empty());
    }
}
