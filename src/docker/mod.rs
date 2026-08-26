use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
mod container_registry;
mod mapping;
mod raw;

use bollard::{
    API_DEFAULT_VERSION, Docker, container::LogOutput, query_parameters::LogsOptionsBuilder,
};
use futures_util::{StreamExt, stream::BoxStream};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::docker::container_registry::ObservedContainer;
use crate::docker::mapping::receiver_stream;
use crate::error::{AppError, AppResult};
use crate::model::{
    ContainerCommandResult, ContainerSample, ContainerState, ContainerSummary, LogLine, LogStream,
};

use self::mapping::map_log_output;
use self::raw::{sample_container_stats, supports_write_operations};

use self::container_registry::{BollardContainerRegistry, ContainerRegistry};

const CONTAINER_INSPECT_TIMEOUT: Duration = Duration::from_secs(1);
const CONTAINER_STATS_TIMEOUT: Duration = Duration::from_secs(5);
const LOG_OBSERVER_INTERVAL: Duration = Duration::from_secs(5);
const CONTAINER_SAMPLE_CONCURRENCY: usize = 8;
const CONTAINER_INSPECT_CONCURRENCY: usize = 8;

fn container_stats_timeout(sample_interval: Duration) -> Duration {
    CONTAINER_STATS_TIMEOUT.min(sample_interval / 2)
}

/// Everything that talks to the Docker socket / proxy goes through this trait.
#[allow(dead_code)] // remaining methods are consumed by the collectors added in later steps
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait DockerService: Send + Sync + 'static {
    fn containers_info(&self) -> AppResult<Vec<ContainerSummary>>;

    fn container_info(&self, id: &str) -> AppResult<Option<ContainerSummary>>;

    fn controls_available(&self) -> bool;

    fn logs(&self) -> BoxStream<'static, LogLine>;

    fn container_samples(&self) -> BoxStream<'static, Vec<ContainerSample>>;

    async fn start_container(&self, id: &str) -> AppResult<ContainerCommandResult>;

    async fn stop_container(&self, id: &str) -> AppResult<ContainerCommandResult>;

    async fn restart_container(&self, id: &str) -> AppResult<ContainerCommandResult>;
}

/// Bollard-backed implementation. Wired up in the composition root.
pub struct BollardDocker {
    docker: Docker,
    container_registry: Arc<dyn ContainerRegistry>,
    controls_available: Arc<AtomicBool>,
    logs_rx: Mutex<Option<mpsc::Receiver<LogLine>>>,
    samples_rx: Mutex<Option<mpsc::Receiver<Vec<ContainerSample>>>>,
}

impl BollardDocker {
    pub fn new(
        docker_host: impl Into<String>,
        timeout_secs: u64,
        sample_interval: Duration,
        controls_probe_interval: Duration,
    ) -> Self {
        let host = docker_host.into();
        let docker = if host.starts_with("unix://") {
            Docker::connect_with_unix(&host, timeout_secs, API_DEFAULT_VERSION)
                .expect("docker socket must be reachable")
        } else {
            Docker::connect_with_http(&host, timeout_secs, API_DEFAULT_VERSION)
                .expect("docker proxy/daemon must be reachable")
        };

        let controls_available = Arc::new(AtomicBool::new(false));
        let (logs_tx, logs_rx) = mpsc::channel::<LogLine>(10_000);
        let (samples_tx, samples_rx) = mpsc::channel::<Vec<ContainerSample>>(32);

        let container_registry = BollardContainerRegistry::new(docker.clone());
        spawn_write_probe(
            docker.clone(),
            controls_available.clone(),
            controls_probe_interval,
        );
        spawn_log_observer(docker.clone(), container_registry.clone(), logs_tx);
        spawn_sample_observer(
            docker.clone(),
            container_registry.clone(),
            samples_tx,
            sample_interval,
        );

        Self {
            docker,
            container_registry,
            controls_available,
            logs_rx: Mutex::new(Some(logs_rx)),
            samples_rx: Mutex::new(Some(samples_rx)),
        }
    }
}

#[async_trait]
impl DockerService for BollardDocker {
    fn containers_info(&self) -> AppResult<Vec<ContainerSummary>> {
        self.container_registry.containers_info()
    }

    fn container_info(&self, id: &str) -> AppResult<Option<ContainerSummary>> {
        self.container_registry.container_info(id)
    }

    fn controls_available(&self) -> bool {
        self.controls_available.load(Ordering::Relaxed)
    }

    fn logs(&self) -> BoxStream<'static, LogLine> {
        match self.logs_rx.lock().ok().and_then(|mut rx| rx.take()) {
            Some(rx) => receiver_stream(rx),
            None => Box::pin(futures_util::stream::pending()),
        }
    }

    fn container_samples(&self) -> BoxStream<'static, Vec<ContainerSample>> {
        match self.samples_rx.lock().ok().and_then(|mut rx| rx.take()) {
            Some(rx) => receiver_stream(rx),
            None => Box::pin(futures_util::stream::pending()),
        }
    }

    async fn start_container(&self, id: &str) -> AppResult<ContainerCommandResult> {
        if !self.controls_available() {
            return Err(AppError::Forbidden(
                "container controls are disabled or unavailable on this backend".into(),
            ));
        }
        let container = self
            .container_registry
            .container_info(id)?
            .ok_or_else(|| AppError::NotFound(format!("container not found: {id}")))?;
        match container.state.ok_or_else(|| {
            AppError::Docker(format!("container state unknown: {}", container.log_id()))
        })? {
            ContainerState::Running => return Ok(ContainerCommandResult::Noop),
            ContainerState::Removing | ContainerState::Dead | ContainerState::Restarting => {
                return Err(AppError::Conflict(format!(
                    "cannot start container while state is {:?}",
                    container.state
                )));
            }
            _ => {}
        }
        self.docker
            .start_container(&container.id, None)
            .await
            .map_err(|err| AppError::Docker(err.to_string()))?;
        Ok(ContainerCommandResult::Submitted)
    }

    async fn stop_container(&self, id: &str) -> AppResult<ContainerCommandResult> {
        if !self.controls_available() {
            return Err(AppError::Forbidden(
                "container controls are disabled or unavailable on this backend".into(),
            ));
        }
        let container = self
            .container_registry
            .container_info(id)?
            .ok_or_else(|| AppError::NotFound(format!("container not found: {id}")))?;
        match container.state.ok_or_else(|| {
            AppError::Docker(format!("container state unknown: {}", container.log_id()))
        })? {
            ContainerState::Created | ContainerState::Exited => {
                return Ok(ContainerCommandResult::Noop);
            }
            ContainerState::Removing | ContainerState::Dead => {
                return Err(AppError::Conflict(format!(
                    "cannot stop container while state is {:?}",
                    container.state
                )));
            }
            _ => {}
        }
        self.docker
            .stop_container(&container.id, None)
            .await
            .map_err(|err| AppError::Docker(err.to_string()))?;
        Ok(ContainerCommandResult::Submitted)
    }

    async fn restart_container(&self, id: &str) -> AppResult<ContainerCommandResult> {
        if !self.controls_available() {
            return Err(AppError::Forbidden(
                "container controls are disabled or unavailable on this backend".into(),
            ));
        }
        let container = self
            .container_registry
            .container_info(id)?
            .ok_or_else(|| AppError::NotFound(format!("container not found: {id}")))?;
        match container.state.ok_or_else(|| {
            AppError::Docker(format!("container state unknown: {}", container.log_id()))
        })? {
            ContainerState::Created | ContainerState::Exited => {
                self.docker
                    .start_container(&container.id, None)
                    .await
                    .map_err(|err| AppError::Docker(err.to_string()))?;
            }
            ContainerState::Running | ContainerState::Paused => {
                self.docker
                    .restart_container(&container.id, None)
                    .await
                    .map_err(|err| AppError::Docker(err.to_string()))?;
            }
            ContainerState::Removing | ContainerState::Dead | ContainerState::Restarting => {
                return Err(AppError::Conflict(format!(
                    "cannot restart container while state is {:?}",
                    container.state
                )));
            }
            _ => {}
        }
        Ok(ContainerCommandResult::Submitted)
    }
}

fn spawn_write_probe(docker: Docker, flag: Arc<AtomicBool>, interval: Duration) {
    tokio::spawn(async move {
        let mut last_logged: Option<bool> = None;
        loop {
            match supports_write_operations(&docker).await {
                Ok(available) => {
                    flag.store(available, Ordering::Relaxed);
                    if last_logged != Some(available) {
                        tracing::info!(
                            docker_controls_available = available,
                            "docker write-capability probe result"
                        );
                        last_logged = Some(available);
                    }
                }
                Err(err) => tracing::warn!(error = %err, "docker write-capability probe failed"),
            }
            tokio::time::sleep(interval).await;
        }
    });
}

fn spawn_log_observer(
    docker: Docker,
    registry: Arc<dyn ContainerRegistry>,
    sender: mpsc::Sender<LogLine>,
) {
    tokio::spawn(async move {
        let mut tasks = HashMap::<String, LogTask>::new();
        let mut ticker = tokio::time::interval(LOG_OBSERVER_INTERVAL);

        loop {
            ticker.tick().await;
            tasks.retain(|_container_id, task| {
                if task.handle.is_finished() {
                    tracing::info!(container = %&task.container.log_id(), "log task finished");
                    false
                } else {
                    true
                }
            });

            let running = registry.observed_containers();
            if let Err(e) = running {
                tracing::warn!(error = %e, "failed to fetch observed containers");
                continue;
            }
            let running = running.unwrap();

            for container in running {
                if !tasks.contains_key(&container.id) {
                    tracing::info!(container= %container.log_id(), "spawning log task");
                    tasks.insert(
                        container.id.clone(),
                        LogTask {
                            container: container.clone(),
                            handle: spawn_container_log_task(
                                docker.clone(),
                                container,
                                sender.clone(),
                            ),
                        },
                    );
                }
            }
        }
    });
}

struct LogTask {
    container: ObservedContainer,
    handle: JoinHandle<()>,
}

fn spawn_container_log_task(
    docker: Docker,
    container: ObservedContainer,
    sender: mpsc::Sender<LogLine>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let since_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i32)
            .unwrap_or_default();
        let options = LogsOptionsBuilder::default()
            .follow(true)
            .stdout(true)
            .stderr(true)
            .timestamps(true)
            .since(since_secs)
            .tail("all")
            .build();
        let mut stream = docker.logs(&container.id, Some(options));
        while let Some(result) = stream.next().await {
            let lines = match result {
                Ok(LogOutput::StdOut { message }) => map_log_output(
                    container.id.clone(),
                    container.log_group.clone(),
                    message,
                    LogStream::Stdout,
                ),
                Ok(LogOutput::StdErr { message }) => map_log_output(
                    container.id.clone(),
                    container.log_group.clone(),
                    message,
                    LogStream::Stderr,
                ),
                Ok(_) => Vec::new(),
                Err(err) => {
                    tracing::warn!(container = %container.name, error = %err, "log stream error");
                    break;
                }
            };
            for line in lines {
                if sender.send(line).await.is_err() {
                    return;
                }
            }
        }
    })
}

fn spawn_sample_observer(
    docker: Docker,
    registry: Arc<dyn ContainerRegistry>,
    sender: mpsc::Sender<Vec<ContainerSample>>,
    interval: Duration,
) {
    tracing::info!(sample_interval = ?interval, "starting container sample observer");
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        let sample_timeout = container_stats_timeout(interval);
        loop {
            ticker.tick().await;

            let running = registry.observed_containers();
            if let Err(e) = running {
                tracing::warn!(error = %e, "failed to get observed containers");
                continue;
            }
            let running = running.unwrap();

            let collection_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis() as i64);
            if let Err(e) = collection_ts {
                tracing::warn!(error = %e, "failed to get collection timestamp");
                continue;
            }
            let collection_ts = collection_ts.unwrap();

            let samples = futures_util::stream::iter(running)
                .map(|container| {
                    let docker = docker.clone();
                    async move {
                        match tokio::time::timeout(
                            sample_timeout,
                            sample_container_stats(&docker, &container.id),
                        )
                        .await
                        {
                            Ok(Ok(stats)) => Some(ContainerSample {
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
                            }),
                            Ok(Err(err)) => {
                                tracing::warn!(container = %container.log_id(), error = %err, "failed to sample container stats");
                                None
                            }
                            Err(_) => {
                                tracing::warn!(container = %container.log_id(), "timed out sampling container stats");
                                None
                            }
                        }
                    }
                })
                .buffer_unordered(CONTAINER_SAMPLE_CONCURRENCY)
                .filter_map(|sample| async move { sample })
                .collect::<Vec<_>>()
                .await;
            if !samples.is_empty() && sender.send(samples).await.is_err() {
                return;
            }
        }
    });
}
