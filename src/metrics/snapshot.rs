//! Latest-value metrics with server-computed rates, kept in memory for `/api/metrics/current`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

use crate::metadata::ServiceRegistry;
use crate::metrics::downsampling::add_optional;
use crate::metrics::rate::optional_rate;
use crate::model::{
    container_id::ContainerId,
    metrics::{
        ContainerPoint, ContainerRawSample, ContainersSnapshot, CurrentHostPoint, GroupPoint,
        HostRawSample, MetricsSnapshot,
    },
    time::TimestampMs,
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
    current: Option<CurrentHostPoint>,
    previous: Option<HostRawSample>,
}

#[derive(Default)]
struct ContainersState {
    current: HashMap<ContainerId, ContainerPoint>,
    previous: HashMap<ContainerId, ContainerRawSample>,
}

/// Host and container samples are recorded by independent tasks, so each half keeps its own
/// lock and revision channel and is never blocked or woken by the other.
pub struct MetricsSnapshotState {
    host: Mutex<HostState>,
    containers: Mutex<ContainersState>,
    /// Resolves each sample's `ServiceId` back to the name the API serializes.
    services: Arc<ServiceRegistry>,
    stale_after_ms: i64,
    host_revision_tx: watch::Sender<u64>,
    containers_revision_tx: watch::Sender<u64>,
}

impl MetricsSnapshotState {
    pub fn new(services: Arc<ServiceRegistry>, collect_interval: Duration) -> Self {
        let (host_revision_tx, _) = watch::channel(0);
        let (containers_revision_tx, _) = watch::channel(0);
        Self {
            host: Mutex::new(HostState::default()),
            containers: Mutex::new(ContainersState::default()),
            services,
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

    pub fn record_host(&self, sample: &HostRawSample) {
        let mut state = self.host.lock().expect("snapshot state poisoned");
        let (net_rx_rate, net_tx_rate, disk_read_rate, disk_write_rate) = match state.previous {
            Some(previous) => {
                let dt_ms = sample.ts - previous.ts;
                (
                    optional_rate(sample.net_rx, previous.net_rx, dt_ms),
                    optional_rate(sample.net_tx, previous.net_tx, dt_ms),
                    optional_rate(sample.disk_read, previous.disk_read, dt_ms),
                    optional_rate(sample.disk_write, previous.disk_write, dt_ms),
                )
            }
            None => (None, None, None, None),
        };

        state.current = Some(CurrentHostPoint {
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
        state.previous = Some(*sample);
        drop(state);
        self.host_revision_tx.send_modify(|revision| *revision += 1);
    }

    /// Each batch carries the full set of sampled containers, so the map is replaced wholesale.
    pub fn record_containers(&self, samples: &[ContainerRawSample]) {
        let mut state = self.containers.lock().expect("snapshot state poisoned");
        let mut current = HashMap::with_capacity(samples.len());
        let mut previous = HashMap::with_capacity(samples.len());

        for sample in samples {
            let (net_rx_rate, net_tx_rate, blk_read_rate, blk_write_rate) =
                match state.previous.get(&sample.cid) {
                    Some(previous) => {
                        let dt_ms = sample.ts - previous.ts;
                        (
                            optional_rate(sample.net_rx, previous.net_rx, dt_ms),
                            optional_rate(sample.net_tx, previous.net_tx, dt_ms),
                            optional_rate(sample.blk_read, previous.blk_read, dt_ms),
                            optional_rate(sample.blk_write, previous.blk_write, dt_ms),
                        )
                    }
                    None => (None, None, None, None),
                };

            // Derived from the rate of two cumulative counters, same as the bucketizer; the
            // first sample for a container has no prior reading to diff against.
            let cpu_pct = state
                .previous
                .get(&sample.cid)
                .and_then(|previous| {
                    let dt_ms = sample.ts - previous.ts;
                    let cpu_rate =
                        optional_rate(sample.cpu_usage_ns, previous.cpu_usage_ns, dt_ms)?;
                    let system_rate = optional_rate(
                        sample.system_cpu_usage_ns,
                        previous.system_cpu_usage_ns,
                        dt_ms,
                    )?;
                    (system_rate > 0.0)
                        .then(|| cpu_rate / system_rate * sample.cpu_count as f64 * 100.0)
                })
                .unwrap_or(0.0);

            current.insert(
                sample.cid,
                ContainerPoint {
                    ts: sample.ts,
                    service: self
                        .services
                        .name(sample.service)
                        .map(|name| name.to_string())
                        .unwrap_or_default(),
                    cpu_pct,
                    mem_used: sample.mem_used,
                    net_rx_rate,
                    net_tx_rate,
                    blk_read_rate,
                    blk_write_rate,
                },
            );
            previous.insert(sample.cid, sample.clone());
        }

        state.current = current;
        state.previous = previous;
        drop(state);
        self.containers_revision_tx
            .send_modify(|revision| *revision += 1);
    }

    pub fn current_host(&self) -> Option<CurrentHostPoint> {
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

    fn current_host_at(&self, now: TimestampMs) -> Option<CurrentHostPoint> {
        let cutoff = self.cutoff(now);
        self.host
            .lock()
            .expect("snapshot state poisoned")
            .current
            .filter(|point| point.ts >= cutoff)
    }

    fn current_containers_at(&self, now: TimestampMs) -> ContainersSnapshot {
        let cutoff = self.cutoff(now);
        let containers: HashMap<ContainerId, ContainerPoint> = self
            .containers
            .lock()
            .expect("snapshot state poisoned")
            .current
            .iter()
            .filter(|(_, point)| point.ts >= cutoff)
            .map(|(cid, point)| (*cid, point.clone()))
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
            add_optional(&mut service.net_rx_rate, point.net_rx_rate);
            add_optional(&mut service.net_tx_rate, point.net_tx_rate);
            add_optional(&mut service.blk_read_rate, point.blk_read_rate);
            add_optional(&mut service.blk_write_rate, point.blk_write_rate);
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
    use crate::model::service_id::ServiceId;

    fn state() -> MetricsSnapshotState {
        MetricsSnapshotState::new(
            ServiceRegistry::fixture(&["web", "db"]),
            Duration::from_secs(10),
        )
    }

    /// Must mirror the order of the fixture above.
    fn test_sid(service: &str) -> ServiceId {
        match service {
            "web" => ServiceId::from_u32(1),
            "db" => ServiceId::from_u32(2),
            other => panic!("add {other} to the registry fixture"),
        }
    }

    fn host_sample(ts: TimestampMs, net_rx: u64) -> HostRawSample {
        HostRawSample {
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
        service: &str,
        net_rx: u64,
    ) -> ContainerRawSample {
        // One CPU online, advancing so the ratio between any two samples is a steady 5% usage.
        let seconds = (ts / 1_000) as u64;
        ContainerRawSample {
            ts,
            service: test_sid(service),
            cid: test_cid(cid),
            cpu_usage_ns: seconds * 50_000_000,
            system_cpu_usage_ns: seconds * 1_000_000_000,
            cpu_count: 1,
            mem_used: 100,
            net_rx,
            net_tx: 0,
            blk_read: 0,
            blk_write: 0,
        }
    }

    /// Maps a short mnemonic label to a distinct, valid `ContainerId` for test readability.
    fn test_cid(label: &str) -> crate::model::container_id::ContainerId {
        let hex = match label {
            "abc" => "aaaaaaaaaaaa",
            "def" => "bbbbbbbbbbbb",
            "ghi" => "cccccccccccc",
            other => panic!("add a hex mapping for test cid {other}"),
        };
        crate::model::container_id::ContainerId::parse(hex).unwrap()
    }

    #[test]
    fn host_rate_is_unknown_until_a_second_sample() {
        let state = state();
        state.record_host(&host_sample(10_000, 5_000));
        assert_eq!(state.current_at(10_000).host.unwrap().net_rx_rate, None);
    }

    #[test]
    fn host_rate_is_derived_from_consecutive_samples() {
        let state = state();
        state.record_host(&host_sample(10_000, 5_000));
        state.record_host(&host_sample(20_000, 25_000));
        assert_eq!(
            state.current_at(20_000).host.unwrap().net_rx_rate,
            Some(2_000.0)
        );
    }

    #[test]
    fn host_counter_reset_reads_as_unknown() {
        let state = state();
        state.record_host(&host_sample(10_000, 25_000));
        state.record_host(&host_sample(20_000, 5_000));
        assert_eq!(state.current_at(20_000).host.unwrap().net_rx_rate, None);
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
        assert!(snapshot.containers.contains_key(&test_cid("abc")));
        assert!(!snapshot.containers.contains_key(&test_cid("def")));
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
        assert_eq!(
            snapshot.containers[&test_cid("abc")].net_rx_rate,
            Some(100.0)
        );
        assert_eq!(
            snapshot.containers[&test_cid("def")].net_rx_rate,
            Some(2_000.0)
        );
    }

    #[test]
    fn container_rate_is_unknown_until_a_second_sample() {
        let state = state();
        state.record_containers(&[container_sample(10_000, "abc", "web", 1_000)]);

        let snapshot = state.current_at(10_000);
        assert_eq!(snapshot.containers[&test_cid("abc")].net_rx_rate, None);
        assert_eq!(snapshot.services["web"].net_rx_rate, None);
    }

    #[test]
    fn services_sum_their_containers() {
        let state = state();
        state.record_containers(&[
            container_sample(10_000, "abc", "web", 0),
            container_sample(10_000, "def", "web", 0),
            container_sample(10_000, "ghi", "db", 0),
        ]);
        // cpu_pct is a rate, so it needs a second sample per container to be non-zero.
        state.record_containers(&[
            container_sample(20_000, "abc", "web", 0),
            container_sample(20_000, "def", "web", 0),
            container_sample(20_000, "ghi", "db", 0),
        ]);

        let snapshot = state.current_at(20_000);
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
