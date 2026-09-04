use std::collections::HashMap;
use std::time::Duration;

use crate::metrics::{
    bucketizer::{Bucketizer, CounterBucketizer, GaugeBucketizer, buffer_capacity},
    downsampling::bucket_end,
};
use crate::model::{
    container_id::ContainerId,
    metrics::{ContainerRawSample, ContainerSample, MetricsResolution},
    time::TimestampMs,
};

/// Buckets a container produced nothing for before its bucketizers are dropped.
const MAX_IDLE_FLUSHES: u32 = 2;

struct ContainerBucketizer {
    bck_cpu_usage_ns: CounterBucketizer,
    bck_system_cpu_usage_ns: CounterBucketizer,
    /// Not bucketized: the online CPU count essentially never changes mid-run.
    last_cpu_count: u32,
    bck_mem_used: GaugeBucketizer,
    bck_mem_limit: GaugeBucketizer,
    bck_net_rx_rate_mill: CounterBucketizer,
    bck_net_tx_rate_mill: CounterBucketizer,
    bck_blk_read_rate_mill: CounterBucketizer,
    bck_blk_write_rate_mill: CounterBucketizer,
}

impl ContainerBucketizer {
    fn new(collect_interval: Duration) -> Self {
        let bucket_len_ms = MetricsResolution::TenSeconds.bucket_ms();
        let capacity = buffer_capacity(collect_interval, bucket_len_ms);

        Self {
            bck_cpu_usage_ns: CounterBucketizer::new(capacity, bucket_len_ms),
            bck_system_cpu_usage_ns: CounterBucketizer::new(capacity, bucket_len_ms),
            last_cpu_count: 1,
            bck_mem_used: GaugeBucketizer::new(capacity, bucket_len_ms),
            bck_mem_limit: GaugeBucketizer::new(capacity, bucket_len_ms),
            bck_net_rx_rate_mill: CounterBucketizer::new(capacity, bucket_len_ms),
            bck_net_tx_rate_mill: CounterBucketizer::new(capacity, bucket_len_ms),
            bck_blk_read_rate_mill: CounterBucketizer::new(capacity, bucket_len_ms),
            bck_blk_write_rate_mill: CounterBucketizer::new(capacity, bucket_len_ms),
        }
    }

    fn push(&mut self, sample: &ContainerRawSample) {
        self.bck_cpu_usage_ns.push(sample.ts, sample.cpu_usage_ns);
        self.bck_system_cpu_usage_ns
            .push(sample.ts, sample.system_cpu_usage_ns);
        self.last_cpu_count = sample.cpu_count;
        self.bck_mem_used.push(sample.ts, sample.mem_used);
        self.bck_mem_limit.push(sample.ts, sample.mem_limit);
        self.bck_net_rx_rate_mill.push(sample.ts, sample.net_rx);
        self.bck_net_tx_rate_mill.push(sample.ts, sample.net_tx);
        self.bck_blk_read_rate_mill.push(sample.ts, sample.blk_read);
        self.bck_blk_write_rate_mill
            .push(sample.ts, sample.blk_write);
    }

    /// Derives CPU% from the ratio of two counter rates rather than averaging instantaneous
    /// samples, so it reflects the true usage across the whole bucket window.
    fn cpu_pct_mill(&self, bucket_end: TimestampMs) -> Option<u64> {
        let cpu_rate = self.bck_cpu_usage_ns.collect(bucket_end)?;
        let system_rate = self.bck_system_cpu_usage_ns.collect(bucket_end)?;
        if system_rate == 0 {
            return None;
        }
        u64::try_from(
            cpu_rate as u128 * self.last_cpu_count as u128 * 100_000 / system_rate as u128,
        )
        .ok()
    }

    fn collect(
        &self,
        bucket_end: TimestampMs,
        service: &str,
        cid: ContainerId,
    ) -> Option<ContainerSample> {
        Some(ContainerSample {
            ts: bucket_end,
            service: service.to_owned(),
            cid,
            cpu_pct_mill: self.cpu_pct_mill(bucket_end)?,
            mem_used: self.bck_mem_used.collect(bucket_end)?,
            mem_limit: self.bck_mem_limit.collect(bucket_end)?,
            net_rx_rate_mill: self.bck_net_rx_rate_mill.collect(bucket_end),
            net_tx_rate_mill: self.bck_net_tx_rate_mill.collect(bucket_end),
            blk_read_rate_mill: self.bck_blk_read_rate_mill.collect(bucket_end),
            blk_write_rate_mill: self.bck_blk_write_rate_mill.collect(bucket_end),
        })
    }
}

struct ContainerEntry {
    service: String,
    bucketizer: ContainerBucketizer,
    idle_flushes: u32,
}

pub(crate) struct ContainerCollectorState {
    containers: HashMap<ContainerId, ContainerEntry>,
    last_raw_bucket_end: Option<TimestampMs>,
    collect_interval: Duration,
}

impl ContainerCollectorState {
    pub(crate) fn new(collect_interval: Duration) -> Self {
        Self {
            containers: HashMap::new(),
            last_raw_bucket_end: None,
            collect_interval,
        }
    }

    /// Flushes every tracked container at the same bucket end, so a bucket's rows share one
    /// timestamp and a container that vanished mid-bucket still gets its last complete bucket.
    pub(crate) fn observe(&mut self, batch: &[ContainerRawSample]) -> Vec<ContainerSample> {
        let bucket_len_ms = MetricsResolution::TenSeconds.bucket_ms();
        let Some(current_bucket_end) = batch
            .iter()
            .map(|sample| sample.ts)
            .max()
            .map(|ts| bucket_end(ts, bucket_len_ms))
        else {
            return Vec::new();
        };

        let completed_bucket_end = self
            .last_raw_bucket_end
            .filter(|last_bucket_end| current_bucket_end > *last_bucket_end);

        for sample in batch {
            let entry = self
                .containers
                .entry(sample.cid)
                .or_insert_with(|| ContainerEntry {
                    service: sample.service.clone(),
                    bucketizer: ContainerBucketizer::new(self.collect_interval),
                    idle_flushes: 0,
                });
            entry.service.clone_from(&sample.service);
            entry.bucketizer.push(sample);
        }
        self.last_raw_bucket_end = Some(current_bucket_end);

        // Collected after pushing, because closing a bucket interpolates against the
        // first sample at or past its end.
        completed_bucket_end
            .map(|bucket_end| self.flush(bucket_end))
            .unwrap_or_default()
    }

    fn flush(&mut self, bucket_end: TimestampMs) -> Vec<ContainerSample> {
        let mut bucketed = Vec::with_capacity(self.containers.len());
        self.containers.retain(|cid, entry| {
            match entry.bucketizer.collect(bucket_end, &entry.service, *cid) {
                Some(sample) => {
                    entry.idle_flushes = 0;
                    bucketed.push(sample);
                    true
                }
                None => {
                    entry.idle_flushes += 1;
                    entry.idle_flushes < MAX_IDLE_FLUSHES
                }
            }
        });
        bucketed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tests below feed one sample per second.
    fn state() -> ContainerCollectorState {
        ContainerCollectorState::new(Duration::from_secs(1))
    }

    fn raw_sample(ts: TimestampMs, cid: &str, counter: u64) -> ContainerRawSample {
        // One CPU online, advancing so the ratio is a steady 25% usage.
        let seconds = (ts / 1_000) as u64;
        ContainerRawSample {
            ts,
            service: "web".into(),
            cid: test_cid(cid),
            cpu_usage_ns: seconds * 250_000_000,
            system_cpu_usage_ns: seconds * 1_000_000_000,
            cpu_count: 1,
            mem_used: 1_000,
            mem_limit: 2_000,
            net_rx: counter,
            net_tx: counter,
            blk_read: counter,
            blk_write: counter,
        }
    }

    /// Maps a short mnemonic label to a distinct, valid `ContainerId` for test readability.
    fn test_cid(label: &str) -> ContainerId {
        let hex = match label {
            "a" => "aaaaaaaaaaaa",
            "b" => "bbbbbbbbbbbb",
            "steady" => "cccccccccccc",
            "joining" => "dddddddddddd",
            other => panic!("add a hex mapping for test cid {other}"),
        };
        ContainerId::parse(hex).unwrap()
    }

    /// Feeds one sample per second across `seconds`, returning everything that got bucketed.
    fn run(
        state: &mut ContainerCollectorState,
        cids: &[&str],
        seconds: i64,
    ) -> Vec<ContainerSample> {
        let mut bucketed = Vec::new();
        for second in 0..seconds {
            let ts = second * 1_000;
            let batch: Vec<ContainerRawSample> = cids
                .iter()
                .map(|cid| raw_sample(ts, cid, second as u64 * 1_000))
                .collect();
            bucketed.extend(state.observe(&batch));
        }
        bucketed
    }

    #[test]
    fn containers_in_one_bucket_share_a_timestamp() {
        let mut state = state();

        let bucketed = run(&mut state, &["a", "b"], 25);

        let first_bucket: Vec<&ContainerSample> = bucketed
            .iter()
            .filter(|sample| sample.ts == 10_000)
            .collect();
        assert_eq!(first_bucket.len(), 2);
        assert!(bucketed.iter().all(|sample| sample.ts % 10_000 == 0));
    }

    #[test]
    fn derives_rates_from_counters() {
        let mut state = state();

        let bucketed = run(&mut state, &["a"], 25);

        let sample = bucketed.iter().find(|sample| sample.ts == 10_000).unwrap();
        // The counter advances by 1_000 per second, so the rate is 1_000 units/s in milli-units.
        assert_eq!(sample.net_rx_rate_mill, Some(1_000_000));
        assert_eq!(sample.cpu_pct_mill, 25_000);
    }

    #[test]
    fn a_late_joining_container_does_not_disturb_the_others() {
        let mut state = state();
        run(&mut state, &["steady"], 12);

        let mut bucketed = Vec::new();
        for second in 12..35 {
            let ts = second * 1_000;
            bucketed.extend(state.observe(&[
                raw_sample(ts, "steady", second as u64 * 1_000),
                // Joins carrying a large lifetime counter.
                raw_sample(ts, "joining", 10_000_000 + second as u64 * 1_000),
            ]));
        }

        // Its first complete bucket is 30s: 20s is missing the sample needed to open it.
        assert!(
            !bucketed
                .iter()
                .any(|sample| sample.cid == test_cid("joining") && sample.ts == 20_000)
        );
        let joining = bucketed
            .iter()
            .find(|sample| sample.cid == test_cid("joining") && sample.ts == 30_000)
            .unwrap();
        assert_eq!(joining.net_rx_rate_mill, Some(1_000_000));

        let steady = bucketed
            .iter()
            .find(|sample| sample.cid == test_cid("steady") && sample.ts == 30_000)
            .unwrap();
        assert_eq!(steady.net_rx_rate_mill, Some(1_000_000));
    }

    #[test]
    fn drops_a_container_that_stopped_reporting() {
        let mut state = state();
        run(&mut state, &["a", "b"], 12);

        for second in 12..45 {
            let ts = second * 1_000;
            state.observe(&[raw_sample(ts, "a", second as u64 * 1_000)]);
        }

        assert!(!state.containers.contains_key(&test_cid("b")));
        assert!(state.containers.contains_key(&test_cid("a")));
    }
}
