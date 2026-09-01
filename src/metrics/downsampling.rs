//! Turns raw stored samples into the bucketed, rate-carrying points the API returns.

use std::collections::HashMap;

use crate::metrics::rate::{bytes_per_second, counter_delta};
use crate::model::{
    ContainerPoint, ContainerSample, GroupPoint, HostPoint, HostSample, MetricsResolution,
};

/// The end of the half-open `(bucket_end - bucket_ms, bucket_end]` window containing `ts`.
pub(crate) fn bucket_end(ts: i64, bucket_ms: i64) -> i64 {
    -(-ts).div_euclid(bucket_ms) * bucket_ms
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

/// Time-weighted counter accumulation for one bucket: observed delta over observed elapsed time.
#[derive(Default, Clone, Copy)]
struct RateAccumulator {
    delta: u128,
    elapsed_ms: i64,
}

impl RateAccumulator {
    fn observe(&mut self, current: u64, previous: u64, dt_ms: i64) {
        // An interval spanning a counter reset carries unknown traffic, so it is left out entirely.
        let Some(delta) = counter_delta(current, previous).filter(|_| dt_ms > 0) else {
            return;
        };
        self.delta += u128::from(delta);
        self.elapsed_ms += dt_ms;
    }

    fn per_second(self) -> f64 {
        bytes_per_second(self.delta, self.elapsed_ms)
    }
}

/// Four cumulative counters bucketed together. `previous` deliberately survives bucket
/// boundaries so the interval straddling a boundary counts toward the later bucket.
#[derive(Default, Clone, Copy)]
struct CounterRates {
    accumulators: [RateAccumulator; 4],
    previous: Option<(i64, [u64; 4])>,
}

impl CounterRates {
    fn observe(&mut self, ts: i64, values: [u64; 4]) {
        if let Some((previous_ts, previous_values)) = self.previous {
            let dt_ms = ts - previous_ts;
            for index in 0..4 {
                self.accumulators[index].observe(values[index], previous_values[index], dt_ms);
            }
        }
        self.previous = Some((ts, values));
    }

    fn per_second(&self) -> [f64; 4] {
        self.accumulators.map(RateAccumulator::per_second)
    }

    fn start_bucket(&mut self) {
        self.accumulators = [RateAccumulator::default(); 4];
    }
}

#[derive(Default)]
struct HostGauges {
    count: u64,
    cpu_pct: f64,
    mem_used: u128,
    mem_total: u128,
    storage_used: u128,
    storage_total: u128,
    metrics_size: u128,
    logs_size: u128,
}

impl HostGauges {
    fn add(&mut self, sample: &HostSample) {
        self.count += 1;
        self.cpu_pct += sample.cpu_pct;
        self.mem_used += sample.mem_used as u128;
        self.mem_total += sample.mem_total as u128;
        self.storage_used += sample.storage_used as u128;
        self.storage_total += sample.storage_total as u128;
        self.metrics_size += sample.metrics_size as u128;
        self.logs_size += sample.logs_size as u128;
    }

    fn finish(&self, ts: i64, rates: [f64; 4]) -> Option<HostPoint> {
        if self.count == 0 {
            return None;
        }
        let count = self.count as u128;
        Some(HostPoint {
            ts,
            cpu_pct: self.cpu_pct / self.count as f64,
            mem_used: (self.mem_used / count) as u64,
            mem_total: (self.mem_total / count) as u64,
            storage_used: (self.storage_used / count) as u64,
            storage_total: (self.storage_total / count) as u64,
            metrics_size: (self.metrics_size / count) as u64,
            logs_size: (self.logs_size / count) as u64,
            net_rx_rate: rates[0],
            net_tx_rate: rates[1],
            disk_read_rate: rates[2],
            disk_write_rate: rates[3],
        })
    }
}

pub fn downsample_host(samples: Vec<HostSample>, resolution: MetricsResolution) -> Vec<HostPoint> {
    let bucket_ms = resolution.bucket_ms();
    let mut points: Vec<HostPoint> = Vec::new();
    let mut current_bucket = i64::MIN;
    let mut gauges = HostGauges::default();
    let mut rates = CounterRates::default();

    for sample in samples {
        let bucket = bucket_end(sample.ts, bucket_ms);
        if bucket != current_bucket {
            points.extend(gauges.finish(current_bucket, rates.per_second()));
            gauges = HostGauges::default();
            rates.start_bucket();
            current_bucket = bucket;
        }

        gauges.add(&sample);
        rates.observe(
            sample.ts,
            [
                sample.net_rx,
                sample.net_tx,
                sample.disk_read,
                sample.disk_write,
            ],
        );
    }

    points.extend(gauges.finish(current_bucket, rates.per_second()));
    // ts is the bucket's closing instant, so a bucket has fully elapsed exactly when ts <= now.
    points.retain(|point| point.ts <= now_ms());
    points
}

#[derive(Default)]
struct ContainerGauges {
    count: u64,
    cpu_pct: f64,
    mem_used: u128,
    mem_limit: u128,
    service: String,
}

impl ContainerGauges {
    fn add(&mut self, sample: &ContainerSample) {
        if self.count == 0 {
            self.service = sample.service.clone();
        }
        self.count += 1;
        self.cpu_pct += sample.cpu_pct;
        self.mem_used += sample.mem_used as u128;
        self.mem_limit += sample.mem_limit as u128;
    }

    fn finish(&self, ts: i64, rates: [f64; 4]) -> Option<ContainerPoint> {
        if self.count == 0 {
            return None;
        }
        let count = self.count as u128;
        Some(ContainerPoint {
            ts,
            service: self.service.clone(),
            cpu_pct: self.cpu_pct / self.count as f64,
            mem_used: (self.mem_used / count) as u64,
            mem_limit: (self.mem_limit / count) as u64,
            net_rx_rate: rates[0],
            net_tx_rate: rates[1],
            blk_read_rate: rates[2],
            blk_write_rate: rates[3],
        })
    }
}

/// Expects the samples of a single container, ordered by `ts`.
pub fn downsample_container(
    samples: Vec<ContainerSample>,
    resolution: MetricsResolution,
) -> Vec<ContainerPoint> {
    let bucket_ms = resolution.bucket_ms();
    let mut points: Vec<ContainerPoint> = Vec::new();
    let mut current_bucket = i64::MIN;
    let mut gauges = ContainerGauges::default();
    let mut rates = CounterRates::default();

    for sample in samples {
        let bucket = bucket_end(sample.ts, bucket_ms);
        if bucket != current_bucket {
            points.extend(gauges.finish(current_bucket, rates.per_second()));
            gauges = ContainerGauges::default();
            rates.start_bucket();
            current_bucket = bucket;
        }

        gauges.add(&sample);
        rates.observe(
            sample.ts,
            [
                sample.net_rx,
                sample.net_tx,
                sample.blk_read,
                sample.blk_write,
            ],
        );
    }

    points.extend(gauges.finish(current_bucket, rates.per_second()));
    // ts is the bucket's closing instant, so a bucket has fully elapsed exactly when ts <= now.
    points.retain(|point| point.ts <= now_ms());
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
            total.mem_limit = total.mem_limit.saturating_add(point.mem_limit);
            total.net_rx_rate += point.net_rx_rate;
            total.net_tx_rate += point.net_tx_rate;
            total.blk_read_rate += point.blk_read_rate;
            total.blk_write_rate += point.blk_write_rate;
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
            cpu_pct: 12.5,
            mem_used: 100,
            mem_total: 200,
            storage_used: 700,
            storage_total: 800,
            metrics_size: 900,
            logs_size: 1_000,
            net_rx: 300,
            net_tx: 400,
            disk_read: 500,
            disk_write: 600,
        }
    }

    fn host_counter(ts: i64, net_rx: u64) -> HostSample {
        HostSample {
            net_rx,
            ..host_sample(ts)
        }
    }

    fn container_counter(ts: i64, cid: &str, net_rx: u64) -> ContainerSample {
        ContainerSample {
            ts,
            service: "web".into(),
            cid: cid.into(),
            cpu_pct: 25.0,
            mem_used: 1_000,
            mem_limit: 2_000,
            net_rx,
            net_tx: 4_000,
            blk_read: 5_000,
            blk_write: 6_000,
        }
    }

    #[test]
    fn averages_database_sizes_within_host_buckets() {
        let mut first = host_sample(100);
        first.metrics_size = 100;
        first.logs_size = 200;
        let mut second = host_sample(200);
        second.metrics_size = 300;
        second.logs_size = 600;

        let points = downsample_host(vec![first, second], MetricsResolution::TenSeconds);

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].metrics_size, 200);
        assert_eq!(points[0].logs_size, 400);
    }

    #[test]
    fn weights_bucket_rate_by_elapsed_time_not_sample_count() {
        // Bucket (60s, 120s] with a missing sample: intervals are 10s, 10s, 30s, 10s and all
        // traffic falls in the 30s one. A simple mean of pair rates would report 250 B/s.
        let samples = vec![
            host_counter(60_000, 0),
            host_counter(70_000, 0),
            host_counter(80_000, 0),
            host_counter(110_000, 30_000),
            host_counter(120_000, 30_000),
        ];

        let points = downsample_host(samples, MetricsResolution::OneMinute);

        assert_eq!(points.len(), 2);
        assert_eq!(points[1].ts, 120_000);
        assert_eq!(points[1].net_rx_rate, 500.0);
    }

    #[test]
    fn excludes_reset_intervals_from_both_delta_and_elapsed_time() {
        // 1000 bytes over 10s, then a reset; counting the reset's 10s would halve the rate.
        let samples = vec![
            host_counter(60_000, 1_000),
            host_counter(70_000, 2_000),
            host_counter(80_000, 500),
        ];

        let points = downsample_host(samples, MetricsResolution::OneMinute);

        assert_eq!(points.len(), 2);
        assert_eq!(points[1].net_rx_rate, 100.0);
    }

    #[test]
    fn first_sample_without_predecessor_has_no_rate() {
        let points = downsample_host(
            vec![host_counter(60_000, 5_000)],
            MetricsResolution::OneMinute,
        );

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].net_rx_rate, 0.0);
    }

    #[test]
    fn group_sum_does_not_spike_when_a_container_appears() {
        let steady = downsample_container(
            vec![
                container_counter(60_000, "steady", 0),
                container_counter(120_000, "steady", 60_000),
            ],
            MetricsResolution::OneMinute,
        );
        // Joins late carrying a large lifetime counter; summing counters would spike here.
        let joining = downsample_container(
            vec![container_counter(120_000, "joining", 10_000_000)],
            MetricsResolution::OneMinute,
        );

        let sum = sum_by_bucket([steady, joining].iter());

        let second = sum.iter().find(|point| point.ts == 120_000).unwrap();
        assert_eq!(second.net_rx_rate, 1_000.0);
    }
}
