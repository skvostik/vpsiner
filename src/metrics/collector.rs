use futures_util::StreamExt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::docker::DockerService;
use crate::logs::store::LogStore;
use crate::metrics::host::HostMetricsSource;
use crate::metrics::store::MetricsStore;
use crate::model::{ContainerSample, ContainerState, short_container_id};
use crate::state::ContainerRegistry;
use tokio::sync::RwLock;

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

pub async fn collect_containers_once(
    docker: &Arc<dyn DockerService>,
    metrics: &Arc<dyn MetricsStore>,
) {
    let containers = match docker.list_containers().await {
        Ok(containers) => containers,
        Err(err) => {
            tracing::error!(error = %err, "failed to list containers for metrics");
            return;
        }
    };

    let mut samples = Vec::new();
    let collection_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default();
    for container in containers {
        if container.state != ContainerState::Running {
            continue;
        }

        let mut stream = match docker.stats_stream(&container.id).await {
            Ok(stream) => stream,
            Err(err) => {
                tracing::warn!(container = %container.name, error = %err, "failed to open container stats");
                continue;
            }
        };

        match stream.next().await {
            Some(Ok(stats)) => {
                tracing::debug!(container = %container.name, cpu_pct = stats.cpu_pct, mem_used = stats.mem_used, "collected container metrics");
                samples.push(ContainerSample {
                    ts: collection_ts,
                    log_group: container.log_group,
                    cid: stats.cid,
                    cpu_pct: stats.cpu_pct,
                    mem_used: stats.mem_used,
                    mem_limit: stats.mem_limit,
                    net_rx: stats.net_rx,
                    net_tx: stats.net_tx,
                    blk_read: stats.blk_read,
                    blk_write: stats.blk_write,
                });
            }
            Some(Err(err)) => {
                tracing::warn!(container = %container.name, error = %err, "failed to read container stats")
            }
            None => {
                tracing::warn!(container = %container.name, "container stats stream ended without a sample")
            }
        }
    }

    if let Err(err) = metrics.insert_containers(samples).await {
        tracing::error!(error = %err, "failed to persist container metrics");
    }
}

pub async fn run_containers(
    docker: Arc<dyn DockerService>,
    metrics: Arc<dyn MetricsStore>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        collect_containers_once(&docker, &metrics).await;
    }
}

pub async fn run_registry(
    docker: Arc<dyn DockerService>,
    registry: Arc<RwLock<ContainerRegistry>>,
) {
    match docker.list_containers().await {
        Ok(containers) => {
            let mut current = registry.write().await;
            current.clear();
            current.extend(
                containers
                    .into_iter()
                    .map(|container| (container.log_group.clone(), container)),
            );
        }
        Err(err) => tracing::error!(error = %err, "failed to initialize container registry"),
    }

    let mut events = match docker.events().await {
        Ok(events) => events,
        Err(err) => {
            tracing::error!(error = %err, "failed to open Docker event stream");
            return;
        }
    };

    while let Some(event) = events.next().await {
        match event {
            Ok(event) => {
                tracing::debug!(kind = ?event.kind, container_id = %short_container_id(&event.container_id), "Docker container event received");
                match docker.list_containers().await {
                    Ok(containers) => {
                        let mut current = registry.write().await;
                        current.clear();
                        current.extend(
                            containers
                                .into_iter()
                                .map(|container| (container.log_group.clone(), container)),
                        );
                        tracing::debug!(kind = ?event.kind, container_id = %short_container_id(&event.container_id), "container registry synchronized");
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to refresh container registry after event")
                    }
                }
            }
            Err(err) => tracing::warn!(error = %err, "Docker event stream error"),
        }
    }

    tracing::warn!("Docker event stream ended");
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
    use crate::model::{ContainerState, ContainerStats, ContainerSummary, HostSample};

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
    async fn enriches_stats_with_container_log_group() {
        let mut docker = MockDockerService::new();
        docker.expect_list_containers().returning(|| {
            Ok(vec![ContainerSummary {
                id: "container-id".into(),
                name: "web-1".into(),
                log_group: "shop-web".into(),
                image: "nginx:latest".into(),
                image_sha: String::new(),
                ports: Vec::new(),
                labels: Vec::new(),
                state: ContainerState::Running,
                started_at: Some(1_700_000_000_000),
            }])
        });
        docker
            .expect_stats_stream()
            .withf(|id| id == "container-id")
            .returning(|_| {
                Ok(Box::pin(stream::iter(vec![Ok(ContainerStats {
                    ts: 123,
                    cid: "short-id".into(),
                    cpu_pct: 12.5,
                    mem_used: 100,
                    mem_limit: 200,
                    net_rx: 300,
                    net_tx: 400,
                    blk_read: 500,
                    blk_write: 600,
                })])) as BoxStream<'static, _>)
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
        collect_containers_once(&docker, &metrics).await;
    }
}
