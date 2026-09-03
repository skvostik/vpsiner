use std::sync::Arc;

use crate::{
    logs::store::LogStore,
    metrics::{
        bucket_watcher::{BucketWatcher, MetricsSource},
        host::HostMetricsSource,
        snapshot::MetricsSnapshotState,
        store::MetricsStore,
    },
};

pub(crate) async fn collect_host_once(
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
            match metrics.insert_host(sample).await {
                Ok(()) => bucket_watcher.observe_sample(MetricsSource::Host, sample.ts),
                Err(err) => tracing::error!(error = %err, "failed to persist host metrics"),
            }
        }
        Err(err) => tracing::error!(error = %err, "failed to sample host metrics"),
    }
}
