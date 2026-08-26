use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::{collections::HashMap, collections::HashSet};

use async_trait::async_trait;
mod mapping;
mod raw;

use bollard::{
    API_DEFAULT_VERSION, Docker,
    container::LogOutput,
    query_parameters::{EventsOptionsBuilder, LogsOptionsBuilder},
};
use futures_util::{StreamExt, stream::BoxStream};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::error::{AppError, AppResult};
use crate::model::{
    ContainerCommandResult, ContainerSample, ContainerState, ContainerSummary, LogLine, LogStream,
};

use self::mapping::{ContainerObserveAction, map_container_observe_event, map_log_output};
use self::raw::{list_containers, sample_container_stats, supports_write_operations};

const CONTAINER_INSPECT_TIMEOUT: Duration = Duration::from_secs(1);
const CONTAINER_STATS_TIMEOUT: Duration = Duration::from_secs(5);
const CONTAINER_RECONCILE_RETRY_DELAY: Duration = Duration::from_secs(5);
const CONTAINER_RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
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
    fn containers(&self) -> Vec<ContainerSummary>;

    fn container(&self, id: &str) -> Option<ContainerSummary>;

    fn controls_available(&self) -> bool;

    fn logs(&self) -> BoxStream<'static, AppResult<LogLine>>;

    fn container_samples(&self) -> BoxStream<'static, AppResult<Vec<ContainerSample>>>;

    async fn start_container(&self, id: &str) -> AppResult<ContainerCommandResult>;

    async fn stop_container(&self, id: &str) -> AppResult<ContainerCommandResult>;

    async fn restart_container(&self, id: &str) -> AppResult<ContainerCommandResult>;
}

/// Bollard-backed implementation. Wired up in the composition root.
pub struct BollardDocker {
    docker: Docker,
    containers: Arc<RwLock<Vec<ContainerSummary>>>,
    controls_available: Arc<AtomicBool>,
    logs_rx: Mutex<Option<mpsc::Receiver<AppResult<LogLine>>>>,
    samples_rx: Mutex<Option<mpsc::Receiver<AppResult<Vec<ContainerSample>>>>>,
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

        let containers = Arc::new(RwLock::new(Vec::new()));
        let controls_available = Arc::new(AtomicBool::new(false));
        let (logs_tx, logs_rx) = mpsc::channel(10_000);
        let (samples_tx, samples_rx) = mpsc::channel(32);

        spawn_container_reconciler(docker.clone(), containers.clone());
        spawn_write_probe(
            docker.clone(),
            controls_available.clone(),
            controls_probe_interval,
        );
        spawn_log_observer(docker.clone(), containers.clone(), logs_tx);
        spawn_sample_observer(
            docker.clone(),
            containers.clone(),
            samples_tx,
            sample_interval,
        );

        Self {
            docker,
            containers,
            controls_available,
            logs_rx: Mutex::new(Some(logs_rx)),
            samples_rx: Mutex::new(Some(samples_rx)),
        }
    }

    fn resolve_container(&self, id: &str) -> Option<ContainerSummary> {
        let needle = id.trim();
        self.containers
            .read()
            .ok()?
            .iter()
            .find(|container| container.id == needle)
            .cloned()
    }
}

#[async_trait]
impl DockerService for BollardDocker {
    fn containers(&self) -> Vec<ContainerSummary> {
        self.containers
            .read()
            .map(|containers| containers.clone())
            .unwrap_or_default()
    }

    fn container(&self, id: &str) -> Option<ContainerSummary> {
        self.resolve_container(id)
    }

    fn controls_available(&self) -> bool {
        self.controls_available.load(Ordering::Relaxed)
    }

    fn logs(&self) -> BoxStream<'static, AppResult<LogLine>> {
        match self.logs_rx.lock().ok().and_then(|mut rx| rx.take()) {
            Some(rx) => receiver_stream(rx),
            None => Box::pin(futures_util::stream::pending()),
        }
    }

    fn container_samples(&self) -> BoxStream<'static, AppResult<Vec<ContainerSample>>> {
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
            .resolve_container(id)
            .ok_or_else(|| AppError::NotFound(format!("container not found: {id}")))?;
        match container.state {
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
            .resolve_container(id)
            .ok_or_else(|| AppError::NotFound(format!("container not found: {id}")))?;
        match container.state {
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
            .resolve_container(id)
            .ok_or_else(|| AppError::NotFound(format!("container not found: {id}")))?;
        match container.state {
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
        }
        Ok(ContainerCommandResult::Submitted)
    }
}

fn receiver_stream<T: Send + 'static>(rx: mpsc::Receiver<T>) -> BoxStream<'static, T> {
    Box::pin(futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    }))
}

fn spawn_container_reconciler(docker: Docker, registry: Arc<RwLock<Vec<ContainerSummary>>>) {
    tokio::spawn(async move {
        loop {
            if let Err(err) = reconcile_containers(&docker, &registry).await {
                tracing::warn!(error = %err, "failed to reconcile Docker containers");
                tokio::time::sleep(CONTAINER_RECONCILE_RETRY_DELAY).await;
                continue;
            }

            let mut filters = HashMap::new();
            filters.insert("type".to_string(), vec!["container".to_string()]);
            let options = EventsOptionsBuilder::default().filters(&filters).build();
            let mut events = docker.events(Some(options));
            let mut reconcile_timer = tokio::time::interval(CONTAINER_RECONCILE_INTERVAL);

            loop {
                tokio::select! {
                    _ = reconcile_timer.tick() => {
                        if let Err(err) = reconcile_containers(&docker, &registry).await {
                            tracing::warn!(error = %err, "failed periodic Docker container reconciliation");
                            break;
                        }
                    }
                    event = events.next() => {
                        match event {
                            Some(Ok(message)) => {
                                let event = map_container_observe_event(message);
                                if event.action != ContainerObserveAction::Ignore {
                                    let container_id = event.container_id.as_deref().unwrap_or("unknown");
                                    tracing::info!(action = ?event.action, container_id = %crate::model::short_container_id(container_id), "Docker container observation event received");
                                    if let Err(err) = reconcile_containers(&docker, &registry).await {
                                        tracing::warn!(error = %err, "failed to reconcile Docker containers after event");
                                        break;
                                    }
                                }
                            }
                            Some(Err(err)) => {
                                tracing::warn!(error = %err, "Docker event stream error");
                                break;
                            }
                            None => {
                                tracing::warn!("Docker event stream ended");
                                break;
                            }
                        }
                    }
                }
            }

            tokio::time::sleep(CONTAINER_RECONCILE_RETRY_DELAY).await;
        }
    });
}

async fn reconcile_containers(
    docker: &Docker,
    registry: &Arc<RwLock<Vec<ContainerSummary>>>,
) -> AppResult<()> {
    tracing::info!("reconciling Docker containers");
    let previous = registry
        .read()
        .map(|containers| containers.clone())
        .unwrap_or_default();
    let containers = list_containers(docker, &previous).await?;
    let mut current = registry
        .write()
        .map_err(|err| AppError::Docker(format!("container registry lock poisoned: {err}")))?;
    *current = containers;
    Ok(())
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
    registry: Arc<RwLock<Vec<ContainerSummary>>>,
    sender: mpsc::Sender<AppResult<LogLine>>,
) {
    tokio::spawn(async move {
        let mut tasks = HashMap::<String, LogTask>::new();
        let mut ticker = tokio::time::interval(LOG_OBSERVER_INTERVAL);

        loop {
            ticker.tick().await;
            tasks.retain(|container_id, task| {
                if task.handle.is_finished() {
                    tracing::debug!(container_id = %crate::model::short_container_id(container_id), log_group = %task.log_group, "container log task finished");
                    false
                } else {
                    true
                }
            });

            let running = registry
                .read()
                .map(|containers| {
                    containers
                        .iter()
                        .filter(|container| container.state == ContainerState::Running)
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let running_ids = running
                .iter()
                .map(|container| container.id.clone())
                .collect::<HashSet<_>>();

            tasks.retain(|container_id, task| {
                if running_ids.contains(container_id) {
                    true
                } else {
                    tracing::info!(container_id = %crate::model::short_container_id(container_id), log_group = %task.log_group, "stopping log task for container");
                    task.handle.abort();
                    false
                }
            });

            for container in running {
                if !tasks.contains_key(&container.id) {
                    tracing::info!(container_id = %crate::model::short_container_id(&container.id), container_name = %container.name, log_group = %container.log_group, "spawning log task for container");
                    tasks.insert(
                        container.id.clone(),
                        LogTask {
                            log_group: container.log_group.clone(),
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
    log_group: String,
    handle: JoinHandle<()>,
}

fn spawn_container_log_task(
    docker: Docker,
    container: ContainerSummary,
    sender: mpsc::Sender<AppResult<LogLine>>,
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
                    tracing::warn!(container = %container.name, error = %err, "Docker log stream error");
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
    registry: Arc<RwLock<Vec<ContainerSummary>>>,
    sender: mpsc::Sender<AppResult<Vec<ContainerSample>>>,
    interval: Duration,
) {
    tracing::info!(sample_interval = ?interval, "starting container sample observer");
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        let sample_timeout = container_stats_timeout(interval);
        loop {
            ticker.tick().await;
            let running = registry
                .read()
                .map(|containers| {
                    containers
                        .iter()
                        .filter(|container| container.state == ContainerState::Running)
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let collection_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis() as i64)
                .unwrap_or_default();
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
                                tracing::warn!(container = %container.name, error = %err, "failed to sample container stats");
                                None
                            }
                            Err(_) => {
                                tracing::warn!(container = %container.name, "timed out sampling container stats");
                                None
                            }
                        }
                    }
                })
                .buffer_unordered(CONTAINER_SAMPLE_CONCURRENCY)
                .filter_map(|sample| async move { sample })
                .collect::<Vec<_>>()
                .await;
            if !samples.is_empty() && sender.send(Ok(samples)).await.is_err() {
                return;
            }
        }
    });
}
