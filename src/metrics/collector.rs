use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;

use crate::docker::DockerService;
use crate::logs::store::LogStore;
use crate::metrics::bucket_watcher::{BucketWatcher, MetricsSource};
use crate::metrics::collector_host::{HostCollectorState, collect_host_once};
use crate::metrics::host::HostMetricsSource;
use crate::metrics::snapshot::MetricsSnapshotState;
use crate::metrics::store::MetricsStore;

pub async fn run_containers(
    docker: Arc<dyn DockerService>,
    metrics: Arc<dyn MetricsStore>,
    snapshot: Arc<MetricsSnapshotState>,
    bucket_watcher: Arc<BucketWatcher>,
    _interval: Duration,
) {
    let mut samples = docker.container_samples();
    while let Some(samples) = samples.next().await {
        snapshot.record_containers(&samples);
        let latest_ts = samples.iter().map(|sample| sample.ts).max();
        match metrics.insert_containers(samples).await {
            Ok(()) => {
                if let Some(ts) = latest_ts {
                    bucket_watcher.observe_sample(MetricsSource::Containers, ts);
                }
            }
            Err(err) => tracing::error!(error = %err, "failed to persist container metrics"),
        }
    }

    tracing::warn!("container metrics stream ended");
}

pub async fn run_host(
    host: Arc<dyn HostMetricsSource>,
    metrics: Arc<dyn MetricsStore>,
    logs: Arc<dyn LogStore>,
    snapshot: Arc<MetricsSnapshotState>,
    bucket_watcher: Arc<BucketWatcher>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    let mut state = HostCollectorState::new(interval);
    loop {
        ticker.tick().await;
        collect_host_once(
            &mut state,
            &host,
            &metrics,
            &logs,
            &snapshot,
            &bucket_watcher,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream::{self, BoxStream};
    use std::sync::Mutex;

    use crate::docker::MockDockerService;
    use crate::logs::store::MockLogStore;
    use crate::metrics::host::MockHostMetricsSource;
    use crate::metrics::store::MockMetricsStore;
    use crate::model::{ContainerSample, HostRawSample};

    #[tokio::test]
    async fn adds_database_sizes_to_host_samples() {
        let mut host = MockHostMetricsSource::new();
        let samples = Arc::new(Mutex::new(
            vec![
                HostRawSample {
                    ts: -5_000,
                    cpu_pct: 12.5,
                    mem_used: 100,
                    mem_total: 200,
                    storage_used: 300,
                    storage_total: 400,
                    metrics_size: 0,
                    logs_size: 0,
                    net_rx: 500,
                    net_tx: 600,
                    disk_read: 700,
                    disk_write: 800,
                },
                HostRawSample {
                    ts: 5_000,
                    cpu_pct: 12.5,
                    mem_used: 100,
                    mem_total: 200,
                    storage_used: 300,
                    storage_total: 400,
                    metrics_size: 0,
                    logs_size: 0,
                    net_rx: 1_500,
                    net_tx: 1_600,
                    disk_read: 1_700,
                    disk_write: 1_800,
                },
                HostRawSample {
                    ts: 15_000,
                    cpu_pct: 12.5,
                    mem_used: 100,
                    mem_total: 200,
                    storage_used: 300,
                    storage_total: 400,
                    metrics_size: 0,
                    logs_size: 0,
                    net_rx: 2_500,
                    net_tx: 2_600,
                    disk_read: 2_700,
                    disk_write: 2_800,
                },
            ]
            .into_iter(),
        ));
        host.expect_sample().times(3).returning(move || {
            Ok(samples
                .lock()
                .expect("host sample queue poisoned")
                .next()
                .expect("host sample queue exhausted"))
        });

        let mut metrics = MockMetricsStore::new();
        metrics
            .expect_database_size_bytes()
            .times(3)
            .returning(|| Ok(5_242_880));
        metrics
            .expect_insert_host()
            .withf(|sample| sample.metrics_size == 5_242_880 && sample.logs_size == 73_400_320)
            .returning(|_| Ok(()));

        let mut logs = MockLogStore::new();
        logs.expect_database_size_bytes()
            .times(3)
            .returning(|| Ok(73_400_320));

        let host: Arc<dyn HostMetricsSource> = Arc::new(host);
        let metrics: Arc<dyn MetricsStore> = Arc::new(metrics);
        let logs: Arc<dyn LogStore> = Arc::new(logs);
        let snapshot = Arc::new(MetricsSnapshotState::new(Duration::from_secs(10)));
        let bucket_watcher = Arc::new(BucketWatcher::new());
        let mut state = HostCollectorState::new(Duration::from_secs(10));
        for _ in 0..3 {
            collect_host_once(
                &mut state,
                &host,
                &metrics,
                &logs,
                &snapshot,
                &bucket_watcher,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn persists_container_sample_batches() {
        let mut docker = MockDockerService::new();
        docker.expect_container_samples().returning(|| {
            Box::pin(stream::iter(vec![vec![ContainerSample {
                ts: 123,
                service: "shop-web".into(),
                cid: "short-id".into(),
                cpu_pct: 12.5,
                mem_used: 100,
                mem_limit: 200,
                net_rx: 300,
                net_tx: 400,
                blk_read: 500,
                blk_write: 600,
            }]])) as BoxStream<'static, _>
        });

        let mut metrics = MockMetricsStore::new();
        metrics
            .expect_insert_containers()
            .withf(|samples| {
                samples.len() == 1
                    && samples[0].service == "shop-web"
                    && samples[0].cid == "short-id"
            })
            .returning(|_| Ok(()));

        let docker: Arc<dyn DockerService> = Arc::new(docker);
        let metrics: Arc<dyn MetricsStore> = Arc::new(metrics);
        let snapshot = Arc::new(MetricsSnapshotState::new(Duration::from_secs(10)));
        let bucket_watcher = Arc::new(BucketWatcher::new());
        run_containers(
            docker,
            metrics,
            snapshot,
            bucket_watcher,
            Duration::from_secs(10),
        )
        .await;
    }
}
