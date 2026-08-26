use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;

use crate::docker::DockerService;
use crate::logs::store::LogStore;
use crate::metrics::host::HostMetricsSource;
use crate::metrics::store::MetricsStore;

pub async fn collect_once(
    host: &Arc<dyn HostMetricsSource>,
    metrics: &Arc<dyn MetricsStore>,
    logs: &Arc<dyn LogStore>,
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
            if let Err(err) = metrics.insert_host(sample).await {
                tracing::error!(error = %err, "failed to persist host metrics");
            }
        }
        Err(err) => tracing::error!(error = %err, "failed to sample host metrics"),
    }
}

pub async fn run_containers(
    docker: Arc<dyn DockerService>,
    metrics: Arc<dyn MetricsStore>,
    _interval: Duration,
) {
    let mut samples = docker.container_samples();
    while let Some(samples) = samples.next().await {
        if let Err(err) = metrics.insert_containers(samples).await {
            tracing::error!(error = %err, "failed to persist container metrics");
        }
    }

    tracing::warn!("container metrics stream ended");
}

pub async fn run(
    host: Arc<dyn HostMetricsSource>,
    metrics: Arc<dyn MetricsStore>,
    logs: Arc<dyn LogStore>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        collect_once(&host, &metrics, &logs).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream::{self, BoxStream};

    use crate::docker::MockDockerService;
    use crate::logs::store::MockLogStore;
    use crate::metrics::host::MockHostMetricsSource;
    use crate::metrics::store::MockMetricsStore;
    use crate::model::{ContainerSample, HostSample};

    #[tokio::test]
    async fn adds_database_sizes_to_host_samples() {
        let mut host = MockHostMetricsSource::new();
        host.expect_sample().returning(|| {
            Ok(HostSample {
                ts: 123,
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
            })
        });

        let mut metrics = MockMetricsStore::new();
        metrics
            .expect_database_size_bytes()
            .returning(|| Ok(5_242_880));
        metrics
            .expect_insert_host()
            .withf(|sample| sample.metrics_size == 5_242_880 && sample.logs_size == 73_400_320)
            .returning(|_| Ok(()));

        let mut logs = MockLogStore::new();
        logs.expect_database_size_bytes()
            .returning(|| Ok(73_400_320));

        let host: Arc<dyn HostMetricsSource> = Arc::new(host);
        let metrics: Arc<dyn MetricsStore> = Arc::new(metrics);
        let logs: Arc<dyn LogStore> = Arc::new(logs);
        collect_once(&host, &metrics, &logs).await;
    }

    #[tokio::test]
    async fn persists_container_sample_batches() {
        let mut docker = MockDockerService::new();
        docker.expect_container_samples().returning(|| {
            Box::pin(stream::iter(vec![vec![ContainerSample {
                ts: 123,
                log_group: "shop-web".into(),
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
                    && samples[0].log_group == "shop-web"
                    && samples[0].cid == "short-id"
            })
            .returning(|_| Ok(()));

        let docker: Arc<dyn DockerService> = Arc::new(docker);
        let metrics: Arc<dyn MetricsStore> = Arc::new(metrics);
        run_containers(docker, metrics, std::time::Duration::from_secs(10)).await;
    }
}
