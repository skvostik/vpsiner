use std::{sync::Arc, time::Duration};

use crate::{
    logs::store::LogStore,
    metrics::{
        bucket_watcher::{BucketWatcher, MetricsSource},
        bucketizer::{Bucketizer, CounterBucketizer, GaugeBucketizer, buffer_capacity},
        downsampling::bucket_end,
        host::HostMetricsSource,
        snapshot::MetricsSnapshotState,
        store::MetricsStore,
    },
    model::{
        metrics::{HostRawSample, HostSample, MetricsResolution},
        time::TimestampMs,
    },
};

pub(crate) fn cpu_pct_mill(cpu_pct: f64) -> u64 {
    if cpu_pct.is_finite() && cpu_pct > 0.0 {
        (cpu_pct * 1_000.0).round() as u64
    } else {
        0
    }
}

struct HostBucketizer {
    bck_cpu_pct_mill: GaugeBucketizer,
    bck_mem_used: GaugeBucketizer,
    bck_mem_total: GaugeBucketizer,
    bck_storage_used: GaugeBucketizer,
    bck_storage_total: GaugeBucketizer,
    bck_metrics_size: GaugeBucketizer,
    bck_logs_size: GaugeBucketizer,
    bck_net_rx_rate_mill: CounterBucketizer,
    bck_net_tx_rate_mill: CounterBucketizer,
    bck_disk_read_rate_mill: CounterBucketizer,
    bck_disk_write_rate_mill: CounterBucketizer,
}

impl HostBucketizer {
    fn new(collect_interval: Duration) -> Self {
        let bucket_len_ms = MetricsResolution::TenSeconds.bucket_ms();
        let capacity = buffer_capacity(collect_interval, bucket_len_ms);

        Self {
            bck_cpu_pct_mill: GaugeBucketizer::new(capacity, bucket_len_ms),
            bck_mem_used: GaugeBucketizer::new(capacity, bucket_len_ms),
            bck_mem_total: GaugeBucketizer::new(capacity, bucket_len_ms),
            bck_storage_used: GaugeBucketizer::new(capacity, bucket_len_ms),
            bck_storage_total: GaugeBucketizer::new(capacity, bucket_len_ms),
            bck_metrics_size: GaugeBucketizer::new(capacity, bucket_len_ms),
            bck_logs_size: GaugeBucketizer::new(capacity, bucket_len_ms),
            bck_net_rx_rate_mill: CounterBucketizer::new(capacity, bucket_len_ms),
            bck_net_tx_rate_mill: CounterBucketizer::new(capacity, bucket_len_ms),
            bck_disk_read_rate_mill: CounterBucketizer::new(capacity, bucket_len_ms),
            bck_disk_write_rate_mill: CounterBucketizer::new(capacity, bucket_len_ms),
        }
    }

    fn push(&mut self, sample: &HostRawSample) {
        self.bck_cpu_pct_mill
            .push(sample.ts, cpu_pct_mill(sample.cpu_pct));
        self.bck_mem_used.push(sample.ts, sample.mem_used);
        self.bck_mem_total.push(sample.ts, sample.mem_total);
        self.bck_storage_used.push(sample.ts, sample.storage_used);
        self.bck_storage_total.push(sample.ts, sample.storage_total);
        self.bck_metrics_size.push(sample.ts, sample.metrics_size);
        self.bck_logs_size.push(sample.ts, sample.logs_size);
        self.bck_net_rx_rate_mill.push(sample.ts, sample.net_rx);
        self.bck_net_tx_rate_mill.push(sample.ts, sample.net_tx);
        self.bck_disk_read_rate_mill
            .push(sample.ts, sample.disk_read);
        self.bck_disk_write_rate_mill
            .push(sample.ts, sample.disk_write);
    }

    fn collect(&self, bucket_end: TimestampMs) -> Option<HostSample> {
        Some(HostSample {
            ts: bucket_end,
            cpu_pct_mill: self.bck_cpu_pct_mill.collect(bucket_end)?,
            mem_used: self.bck_mem_used.collect(bucket_end)?,
            mem_total: self.bck_mem_total.collect(bucket_end)?,
            storage_used: self.bck_storage_used.collect(bucket_end)?,
            storage_total: self.bck_storage_total.collect(bucket_end)?,
            metrics_size: self.bck_metrics_size.collect(bucket_end)?,
            logs_size: self.bck_logs_size.collect(bucket_end)?,
            net_rx_rate_mill: self.bck_net_rx_rate_mill.collect(bucket_end),
            net_tx_rate_mill: self.bck_net_tx_rate_mill.collect(bucket_end),
            disk_read_rate_mill: self.bck_disk_read_rate_mill.collect(bucket_end),
            disk_write_rate_mill: self.bck_disk_write_rate_mill.collect(bucket_end),
        })
    }
}

pub(crate) struct HostCollectorState {
    bucketizer: HostBucketizer,
    last_raw_bucket_end: Option<TimestampMs>,
}

impl HostCollectorState {
    pub(crate) fn new(collect_interval: Duration) -> Self {
        Self {
            bucketizer: HostBucketizer::new(collect_interval),
            last_raw_bucket_end: None,
        }
    }

    fn observe(&mut self, sample: &HostRawSample) -> Option<HostSample> {
        let bucket_len_ms = MetricsResolution::TenSeconds.bucket_ms();
        let current_bucket_end = bucket_end(sample.ts, bucket_len_ms);
        let completed_bucket_end = self
            .last_raw_bucket_end
            .filter(|last_bucket_end| current_bucket_end > *last_bucket_end);

        self.bucketizer.push(sample);
        self.last_raw_bucket_end = Some(current_bucket_end);
        completed_bucket_end.and_then(|bucket_end| self.bucketizer.collect(bucket_end))
    }
}

pub(crate) async fn collect_host_once(
    state: &mut HostCollectorState,
    host: &Arc<dyn HostMetricsSource>,
    metrics: &Arc<dyn MetricsStore>,
    logs: &Arc<dyn LogStore>,
    snapshot: &Arc<MetricsSnapshotState>,
    bucket_watcher: &Arc<BucketWatcher>,
) {
    match host.sample().await {
        Ok(mut sample) => {
            sample.metrics_size = match metrics.database_size_bytes().await {
                Ok(size) => size,
                Err(err) => {
                    tracing::error!(error = %err, "failed to measure metrics database size");
                    return;
                }
            };
            sample.logs_size = match logs.database_size_bytes().await {
                Ok(size) => size,
                Err(err) => {
                    tracing::error!(error = %err, "failed to measure logs database size");
                    return;
                }
            };
            snapshot.record_host(&sample);

            let Some(bucketed_sample) = state.observe(&sample) else {
                return;
            };
            match metrics.insert_host(bucketed_sample).await {
                Ok(()) => bucket_watcher.observe_sample(MetricsSource::Host, bucketed_sample.ts),
                Err(err) => tracing::error!(error = %err, "failed to persist host metrics"),
            }
        }
        Err(err) => tracing::error!(error = %err, "failed to sample host metrics"),
    }
}
