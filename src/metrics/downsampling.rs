//! Aggregates stored 10s samples into the coarser buckets held by the rollup tables.

use std::collections::HashMap;

use crate::model::container_id::ContainerId;
use crate::model::metrics::{
    ContainerPoint, ContainerSample, GroupPoint, HostSample, MetricsResolution,
};

/// The end of the half-open `(bucket_end - bucket_ms, bucket_end]` window containing `ts`.
pub(crate) fn bucket_end(ts: i64, bucket_ms: u64) -> i64 {
    let bucket_ms = i64::try_from(bucket_ms).expect("bucket size exceeds i64");
    -(-ts).div_euclid(bucket_ms) * bucket_ms
}

/// Adds one contributor to an aggregate rate; the total stays `None` only while every
/// contributor is `None`.
pub(crate) fn add_optional(total: &mut Option<f64>, value: Option<f64>) {
    if let Some(value) = value {
        *total = Some(total.unwrap_or(0.0) + value);
    }
}

/// Tracks the largest silence between present samples, including the edges of the bucket
/// itself, so a bucket built from too little of its own window can be discarded.
#[derive(Default)]
struct GapTracker {
    first_ts: Option<i64>,
    last_ts: Option<i64>,
    max_interior_gap: i64,
}

impl GapTracker {
    fn observe(&mut self, ts: i64) {
        if let Some(last_ts) = self.last_ts {
            self.max_interior_gap = self.max_interior_gap.max(ts - last_ts);
        }
        self.first_ts.get_or_insert(ts);
        self.last_ts = Some(ts);
    }

    /// `bucket_end` is the closing timestamp of the `(bucket_end - bucket_ms, bucket_end]` window.
    fn exceeds_max_gap(&self, bucket_end: i64, bucket_ms: u64, max_gap_pct: u8) -> bool {
        let bucket_start = bucket_end - i64::try_from(bucket_ms).expect("bucket size exceeds i64");
        let edge_start = self.first_ts.expect("gap tracker has samples") - bucket_start;
        let edge_end = bucket_end - self.last_ts.expect("gap tracker has samples");
        let observed_max = self.max_interior_gap.max(edge_start).max(edge_end);
        let threshold = (bucket_ms * max_gap_pct as u64 / 100) as i64;
        observed_max > threshold
    }
}

#[derive(Default)]
struct HostGauges {
    count: u64,
    gap: GapTracker,
    cpu_pct_mill: u128,
    mem_used: u128,
    storage_used: u128,
    metrics_size: u128,
    logs_size: u128,
    net_rx_rate_mill: OptionalGauge,
    net_tx_rate_mill: OptionalGauge,
    disk_read_rate_mill: OptionalGauge,
    disk_write_rate_mill: OptionalGauge,
}

#[derive(Default)]
struct OptionalGauge {
    count: u64,
    total: u128,
}

impl OptionalGauge {
    fn add(&mut self, value: Option<u64>) {
        if let Some(value) = value {
            self.count += 1;
            self.total += value as u128;
        }
    }

    fn mean_rate(&self) -> Option<u64> {
        (self.count > 0).then(|| (self.total / self.count as u128) as u64)
    }
}

impl HostGauges {
    fn add(&mut self, sample: &HostSample) {
        self.count += 1;
        self.gap.observe(sample.ts);
        self.cpu_pct_mill += sample.cpu_pct_mill as u128;
        self.mem_used += sample.mem_used as u128;
        self.storage_used += sample.storage_used as u128;
        self.metrics_size += sample.metrics_size as u128;
        self.logs_size += sample.logs_size as u128;
        self.net_rx_rate_mill.add(sample.net_rx_rate_mill);
        self.net_tx_rate_mill.add(sample.net_tx_rate_mill);
        self.disk_read_rate_mill.add(sample.disk_read_rate_mill);
        self.disk_write_rate_mill.add(sample.disk_write_rate_mill);
    }

    fn finish(&self, ts: i64, bucket_ms: u64, max_gap_pct: u8) -> Option<HostSample> {
        if self.count == 0 || self.gap.exceeds_max_gap(ts, bucket_ms, max_gap_pct) {
            return None;
        }
        let count = self.count as u128;
        Some(HostSample {
            ts,
            cpu_pct_mill: (self.cpu_pct_mill / count) as u64,
            mem_used: (self.mem_used / count) as u64,
            storage_used: (self.storage_used / count) as u64,
            metrics_size: (self.metrics_size / count) as u64,
            logs_size: (self.logs_size / count) as u64,
            net_rx_rate_mill: self.net_rx_rate_mill.mean_rate(),
            net_tx_rate_mill: self.net_tx_rate_mill.mean_rate(),
            disk_read_rate_mill: self.disk_read_rate_mill.mean_rate(),
            disk_write_rate_mill: self.disk_write_rate_mill.mean_rate(),
        })
    }
}

/// Expects samples ordered by `ts`; every bucket it emits is fully elapsed.
pub fn downsample_host(
    samples: &[HostSample],
    resolution: MetricsResolution,
    max_gap_pct: u8,
) -> Vec<HostSample> {
    let bucket_ms = resolution.bucket_ms();
    let mut points: Vec<HostSample> = Vec::new();
    let mut current_bucket = i64::MIN;
    let mut gauges = HostGauges::default();

    for sample in samples {
        let bucket = bucket_end(sample.ts, bucket_ms);
        if bucket != current_bucket {
            points.extend(gauges.finish(current_bucket, bucket_ms, max_gap_pct));
            gauges = HostGauges::default();
            current_bucket = bucket;
        }

        gauges.add(sample);
    }

    points.extend(gauges.finish(current_bucket, bucket_ms, max_gap_pct));
    points
}

#[derive(Default)]
struct ContainerGauges {
    count: u64,
    gap: GapTracker,
    cpu_pct_mill: u128,
    mem_used: u128,
    net_rx_rate_mill: OptionalGauge,
    net_tx_rate_mill: OptionalGauge,
    blk_read_rate_mill: OptionalGauge,
    blk_write_rate_mill: OptionalGauge,
    service: String,
    cid: ContainerId,
}

impl ContainerGauges {
    fn add(&mut self, sample: &ContainerSample) {
        if self.count == 0 {
            self.service = sample.service.clone();
            self.cid = sample.cid;
        }
        self.count += 1;
        self.gap.observe(sample.ts);
        self.cpu_pct_mill += sample.cpu_pct_mill as u128;
        self.mem_used += sample.mem_used as u128;
        self.net_rx_rate_mill.add(sample.net_rx_rate_mill);
        self.net_tx_rate_mill.add(sample.net_tx_rate_mill);
        self.blk_read_rate_mill.add(sample.blk_read_rate_mill);
        self.blk_write_rate_mill.add(sample.blk_write_rate_mill);
    }

    fn finish(&self, ts: i64, bucket_ms: u64, max_gap_pct: u8) -> Option<ContainerSample> {
        if self.count == 0 || self.gap.exceeds_max_gap(ts, bucket_ms, max_gap_pct) {
            return None;
        }
        let count = self.count as u128;
        Some(ContainerSample {
            ts,
            service: self.service.clone(),
            cid: self.cid,
            cpu_pct_mill: (self.cpu_pct_mill / count) as u64,
            mem_used: (self.mem_used / count) as u64,
            net_rx_rate_mill: self.net_rx_rate_mill.mean_rate(),
            net_tx_rate_mill: self.net_tx_rate_mill.mean_rate(),
            blk_read_rate_mill: self.blk_read_rate_mill.mean_rate(),
            blk_write_rate_mill: self.blk_write_rate_mill.mean_rate(),
        })
    }
}

/// Expects the samples of a single container, ordered by `ts`; callers can pass a slice or a
/// filtered iterator of references, so a shared buffer can be split by container without copying.
pub fn downsample_container<'a>(
    samples: impl IntoIterator<Item = &'a ContainerSample>,
    resolution: MetricsResolution,
    max_gap_pct: u8,
) -> Vec<ContainerSample> {
    let bucket_ms = resolution.bucket_ms();
    let mut points: Vec<ContainerSample> = Vec::new();
    let mut current_bucket = i64::MIN;
    let mut gauges = ContainerGauges::default();

    for sample in samples {
        let bucket = bucket_end(sample.ts, bucket_ms);
        if bucket != current_bucket {
            points.extend(gauges.finish(current_bucket, bucket_ms, max_gap_pct));
            gauges = ContainerGauges::default();
            current_bucket = bucket;
        }

        gauges.add(sample);
    }

    points.extend(gauges.finish(current_bucket, bucket_ms, max_gap_pct));
    points
}

/// Rates add cleanly across containers, so a group series is the per-bucket sum of its members.
pub fn sum_by_bucket<'a>(series: impl Iterator<Item = &'a Vec<ContainerPoint>>) -> Vec<GroupPoint> {
    let mut totals: HashMap<i64, GroupPoint> = HashMap::new();
    for points in series {
        for point in points {
            let total = totals.entry(point.ts).or_insert(GroupPoint {
                ts: point.ts,
                ..GroupPoint::default()
            });
            total.cpu_pct += point.cpu_pct;
            total.mem_used = total.mem_used.saturating_add(point.mem_used);
            add_optional(&mut total.net_rx_rate, point.net_rx_rate);
            add_optional(&mut total.net_tx_rate, point.net_tx_rate);
            add_optional(&mut total.blk_read_rate, point.blk_read_rate);
            add_optional(&mut total.blk_write_rate, point.blk_write_rate);
        }
    }

    let mut sum: Vec<GroupPoint> = totals.into_values().collect();
    sum.sort_by_key(|point| point.ts);
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host_sample(ts: i64) -> HostSample {
        HostSample {
            ts,
            cpu_pct_mill: 12_500,
            mem_used: 100,
            storage_used: 700,
            metrics_size: 900,
            logs_size: 1_000,
            net_rx_rate_mill: Some(300_000),
            net_tx_rate_mill: Some(400_000),
            disk_read_rate_mill: Some(500_000),
            disk_write_rate_mill: Some(600_000),
        }
    }

    fn host_counter(ts: i64, net_rx_rate_mill: u64) -> HostSample {
        HostSample {
            net_rx_rate_mill: Some(net_rx_rate_mill),
            ..host_sample(ts)
        }
    }

    /// `label` only documents intent here; downsampling never inspects `cid`'s value.
    fn container_counter(ts: i64, label: &str, net_rx_rate_mill: u64) -> ContainerSample {
        let _ = label;
        ContainerSample {
            ts,
            service: "web".into(),
            cid: ContainerId::parse("aaaaaaaaaaaa").unwrap(),
            cpu_pct_mill: 25_000,
            mem_used: 1_000,
            net_rx_rate_mill: Some(net_rx_rate_mill),
            net_tx_rate_mill: Some(4_000_000),
            blk_read_rate_mill: Some(5_000_000),
            blk_write_rate_mill: Some(6_000_000),
        }
    }

    #[test]
    fn averages_database_sizes_within_host_buckets() {
        let mut first = host_sample(10_000);
        first.metrics_size = 100;
        first.logs_size = 200;
        let mut second = host_sample(20_000);
        second.metrics_size = 300;
        second.logs_size = 600;

        let points = downsample_host(&[first, second], MetricsResolution::OneMinute, 100);

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].ts, 60_000);
        assert_eq!(points[0].metrics_size, 200);
        assert_eq!(points[0].logs_size, 400);
    }

    #[test]
    fn averages_host_rates_within_larger_buckets() {
        let samples = vec![host_counter(70_000, 100_000), host_counter(80_000, 300_000)];

        let points = downsample_host(&samples, MetricsResolution::OneMinute, 100);

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].ts, 120_000);
        assert_eq!(points[0].net_rx_rate_mill, Some(200_000));
    }

    #[test]
    fn first_host_sample_uses_stored_rate() {
        let points = downsample_host(
            &[host_counter(60_000, 5_000_000)],
            MetricsResolution::OneMinute,
            100,
        );

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].net_rx_rate_mill, Some(5_000_000));
    }

    #[test]
    fn host_rate_stays_none_without_present_values() {
        let points = downsample_host(
            &[HostSample {
                net_rx_rate_mill: None,
                ..host_sample(60_000)
            }],
            MetricsResolution::OneMinute,
            100,
        );

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].net_rx_rate_mill, None);
    }

    #[test]
    fn averages_container_rates_within_larger_buckets() {
        let samples = vec![
            container_counter(70_000, "web", 100_000),
            container_counter(80_000, "web", 300_000),
        ];

        let points = downsample_container(&samples, MetricsResolution::OneMinute, 100);

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].ts, 120_000);
        assert_eq!(points[0].net_rx_rate_mill, Some(200_000));
    }

    #[test]
    fn container_rate_stays_none_without_present_values() {
        let points = downsample_container(
            &[ContainerSample {
                net_rx_rate_mill: None,
                ..container_counter(60_000, "web", 0)
            }],
            MetricsResolution::OneMinute,
            100,
        );

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].net_rx_rate_mill, None);
    }

    fn points(samples: Vec<ContainerSample>) -> Vec<ContainerPoint> {
        samples.into_iter().map(ContainerPoint::from).collect()
    }

    #[test]
    fn group_sum_adds_present_rates_only() {
        let known = points(downsample_container(
            &[container_counter(60_000, "known", 1_000_000)],
            MetricsResolution::OneMinute,
            100,
        ));
        let unknown = points(downsample_container(
            &[ContainerSample {
                net_rx_rate_mill: None,
                ..container_counter(60_000, "unknown", 0)
            }],
            MetricsResolution::OneMinute,
            100,
        ));

        let sum = sum_by_bucket([known, unknown].iter());

        assert_eq!(sum.len(), 1);
        assert_eq!(sum[0].net_rx_rate, Some(1_000.0));
    }

    #[test]
    fn group_sum_does_not_spike_when_a_container_appears() {
        let steady = points(downsample_container(
            &[
                container_counter(60_000, "steady", 0),
                container_counter(120_000, "steady", 1_000_000),
            ],
            MetricsResolution::OneMinute,
            100,
        ));
        // Joins late; its stored value is already a rate, so nothing accumulates into a spike.
        let joining = points(downsample_container(
            &[container_counter(120_000, "joining", 0)],
            MetricsResolution::OneMinute,
            100,
        ));

        let sum = sum_by_bucket([steady, joining].iter());

        let second = sum.iter().find(|point| point.ts == 120_000).unwrap();
        assert_eq!(second.net_rx_rate, Some(1_000.0));
    }

    #[test]
    fn discards_bucket_with_a_lone_sample_far_from_both_edges_at_default_threshold() {
        // Bucket is (0, 60_000]; a single sample at 30_000 leaves 30s edge gaps each side,
        // exceeding the default 40% (24s) threshold.
        let points = downsample_host(&[host_sample(30_000)], MetricsResolution::OneMinute, 40);

        assert!(points.is_empty());
    }

    #[test]
    fn keeps_lone_sample_bucket_when_gap_check_is_disabled() {
        let points = downsample_host(&[host_sample(30_000)], MetricsResolution::OneMinute, 100);

        assert_eq!(points.len(), 1);
    }

    #[test]
    fn discards_bucket_when_interior_gap_exceeds_threshold() {
        // 60s bucket, 40% threshold is 24_000ms; a 30s interior gap exceeds it.
        let samples = vec![host_sample(10_000), host_sample(40_000)];

        let points = downsample_host(&samples, MetricsResolution::OneMinute, 40);

        assert!(points.is_empty());
    }

    #[test]
    fn keeps_fully_populated_bucket_at_default_threshold() {
        let samples: Vec<HostSample> = (1..=6).map(|n| host_sample(n * 10_000)).collect();

        let points = downsample_host(&samples, MetricsResolution::OneMinute, 40);

        assert_eq!(points.len(), 1);
    }

    #[test]
    fn keeps_bucket_when_gap_exactly_equals_the_threshold() {
        // 60s bucket, 40% threshold is 24_000ms; edge gaps of exactly 24s on both sides.
        let samples = vec![host_sample(24_000), host_sample(36_000)];

        let points = downsample_host(&samples, MetricsResolution::OneMinute, 40);

        assert_eq!(points.len(), 1);
    }
}
