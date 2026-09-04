use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;

use crate::docker::DockerService;
use crate::logs::store::LogStore;
use crate::metrics::bucket_watcher::{BucketWatcher, MetricsSource};
use crate::metrics::collector_containers::ContainerCollectorState;
use crate::metrics::collector_host::{HostCollectorState, collect_host_once};
use crate::metrics::host::HostMetricsSource;
use crate::metrics::snapshot::MetricsSnapshotState;
use crate::metrics::store::MetricsStore;

pub async fn run_containers(
    docker: Arc<dyn DockerService>,
    metrics: Arc<dyn MetricsStore>,
    snapshot: Arc<MetricsSnapshotState>,
    bucket_watcher: Arc<BucketWatcher>,
    interval: Duration,
) {
    let mut samples = docker.container_samples();
    let mut state = ContainerCollectorState::new(interval);
    while let Some(batch) = samples.next().await {
        snapshot.record_containers(&batch);
        let bucketed = state.observe(&batch);
        let Some(latest_ts) = bucketed.iter().map(|sample| sample.ts).max() else {
            continue;
        };
        match metrics.insert_containers(bucketed).await {
            Ok(()) => bucket_watcher.observe_sample(MetricsSource::Containers, latest_ts),
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
    use crate::metadata::ServiceRegistry;
    use crate::metrics::host::MockHostMetricsSource;
    use crate::metrics::store::MockMetricsStore;
    use crate::model::{
        container_id::ContainerId,
        metrics::{ContainerRawSample, HostRawSample},
        service_id::ServiceId,
    };

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
        let snapshot = Arc::new(MetricsSnapshotState::new(
            ServiceRegistry::fixture(&[]),
            Duration::from_secs(10),
        ));
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
    async fn persists_container_batches_once_a_bucket_completes() {
        let mut docker = MockDockerService::new();
        docker.expect_container_samples().returning(|| {
            // Runs one second past the bucket end, which is what closes the bucket.
            let batches: Vec<Vec<ContainerRawSample>> = (0..=11)
                .map(|second: i64| {
                    vec![ContainerRawSample {
                        ts: second * 1_000,
                        service: ServiceId::from_u32(1),
                        cid: ContainerId::parse("aaaaaaaaaaaa").unwrap(),
                        // One CPU online, advancing so the ratio is a steady 12.5% usage.
                        cpu_usage_ns: second as u64 * 125_000_000,
                        system_cpu_usage_ns: second as u64 * 1_000_000_000,
                        cpu_count: 1,
                        mem_used: 100,
                        net_rx: second as u64 * 1_000,
                        net_tx: second as u64 * 1_000,
                        blk_read: second as u64 * 1_000,
                        blk_write: second as u64 * 1_000,
                    }]
                })
                .collect();
            Box::pin(stream::iter(batches)) as BoxStream<'static, _>
        });

        let mut metrics = MockMetricsStore::new();
        metrics
            .expect_insert_containers()
            .times(1)
            .withf(|samples| {
                samples.len() == 1
                    && samples[0].ts == 10_000
                    && samples[0].service == ServiceId::from_u32(1)
                    && samples[0].cid == ContainerId::parse("aaaaaaaaaaaa").unwrap()
                    && samples[0].cpu_pct_mill == 12_500
                    && samples[0].net_rx_rate_mill == Some(1_000_000)
            })
            .returning(|_| Ok(()));

        let docker: Arc<dyn DockerService> = Arc::new(docker);
        let metrics: Arc<dyn MetricsStore> = Arc::new(metrics);
        let snapshot = Arc::new(MetricsSnapshotState::new(
            ServiceRegistry::fixture(&["shop-web"]),
            Duration::from_secs(1),
        ));
        let bucket_watcher = Arc::new(BucketWatcher::new());
        // Must match the fixture's cadence, or the buffers are sized for far fewer samples.
        run_containers(
            docker,
            metrics,
            snapshot,
            bucket_watcher,
            Duration::from_secs(1),
        )
        .await;
    }
}
