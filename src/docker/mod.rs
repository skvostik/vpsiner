use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

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

use crate::docker::container_registry::{ContainerObserveAction, ObservedContainer};
use crate::docker::mapping::receiver_stream;
use crate::error::{AppError, AppResult};
use crate::model::{
    ContainerCommandResult, ContainerSample, ContainerState, ContainerSummary, LogLine, LogStream,
};

use self::mapping::map_log_output;
use self::raw::{sample_container_stats, supports_write_operations};

use self::container_registry::{BollardContainerRegistry, ContainerRegistry};

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
    logs_tx: mpsc::Sender<LogLine>,
    samples_tx: mpsc::Sender<Vec<ContainerSample>>,
    logs_rx: Mutex<Option<mpsc::Receiver<LogLine>>>,
    samples_rx: Mutex<Option<mpsc::Receiver<Vec<ContainerSample>>>>,
}

impl BollardDocker {
    pub fn new(
        docker_host: impl Into<String>,
        timeout_secs: u64,
        sample_interval: Duration,
        docker_probe_interval: Duration,
        request_concurrency: usize,
        docker_retry_delay: Duration,
        log_channel_capacity: usize,
        samples_channel_capacity: usize,
        docker_events_channel_capacity: usize,
    ) -> Arc<Self> {
        let host = docker_host.into();
        let docker = if host.starts_with("unix://") {
            Docker::connect_with_unix(&host, timeout_secs, API_DEFAULT_VERSION)
                .expect("docker socket must be reachable")
        } else {
            Docker::connect_with_http(&host, timeout_secs, API_DEFAULT_VERSION)
                .expect("docker proxy/daemon must be reachable")
        };

        let controls_available = Arc::new(AtomicBool::new(false));
        let request_timeout = Duration::from_secs(timeout_secs);
        let (logs_tx, logs_rx) = mpsc::channel::<LogLine>(log_channel_capacity);
        let (samples_tx, samples_rx) =
            mpsc::channel::<Vec<ContainerSample>>(samples_channel_capacity);

        let container_registry = BollardContainerRegistry::new(
            docker.clone(),
            request_concurrency,
            request_timeout,
            docker_probe_interval,
            docker_retry_delay,
            docker_events_channel_capacity,
        );

        let registry = Arc::new(Self {
            docker,
            container_registry,
            controls_available,
            logs_tx,
            samples_tx,
            logs_rx: Mutex::new(Some(logs_rx)),
            samples_rx: Mutex::new(Some(samples_rx)),
        });

        spawn_write_probe(Arc::downgrade(&registry), docker_probe_interval);
        spawn_log_observer(Arc::downgrade(&registry), docker_probe_interval);
        spawn_sample_observer(
            Arc::downgrade(&registry),
            sample_interval,
            request_concurrency,
            request_timeout,
        );

        registry
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
        tracing::info!(container=%&container.log_id(), "attempting to start container");
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
        tracing::info!(container=%&container.log_id(), "attempting to stop container");
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
        tracing::info!(container=%&container.log_id(), "attempting to restart container");
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

fn spawn_write_probe(registry: Weak<BollardDocker>, interval: Duration) {
    tokio::spawn(async move {
        let mut last_logged: Option<bool> = None;
        loop {
            let Some(registry_ref) = registry.upgrade() else {
                tracing::debug!("stopping write probe worker because docker service was dropped");
                return;
            };

            let docker = registry_ref.docker.clone();
            let flag = registry_ref.controls_available.clone();
            drop(registry_ref);

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

fn spawn_log_observer(registry: Weak<BollardDocker>, interval: Duration) {
    tokio::spawn(async move {
        let mut tasks = HashMap::<String, LogTask>::new();
        let mut observe_events = registry.upgrade().and_then(|registry_ref| {
            registry_ref
                .container_registry
                .take_observe_events_stream()
                .ok()
        });

        loop {
            // Check for container start events with a timeout
            if let Some(events) = observe_events.as_mut() {
                match tokio::time::timeout(interval, async {
                    while let Some(event) = events.next().await {
                        if event.action == ContainerObserveAction::Start {
                            return true;
                        }
                    }
                    false
                })
                .await
                {
                    // Event triggered
                    Ok(true) => true,
                    // Stream ended
                    Ok(false) => {
                        tracing::warn!(
                            "container observe events stream ended; using timer fallback"
                        );
                        observe_events = None;
                        false
                    }
                    // Timeout occurred, no new events
                    Err(_) => false,
                }
            } else {
                tokio::time::sleep(interval).await;
                false
            };

            let Some(registry_ref) = registry.upgrade() else {
                tracing::debug!("stopping log observer because docker service was dropped");
                return;
            };

            let docker = registry_ref.docker.clone();
            let sender = registry_ref.logs_tx.clone();
            let running = registry_ref.container_registry.observed_containers();
            drop(registry_ref);

            // Retain only the log tasks that are still running
            tasks.retain(|_container_id, task| {
                if task.handle.is_finished() {
                    tracing::info!(container = %&task.container.log_id(), "log task finished");
                    false
                } else {
                    true
                }
            });

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
    registry: Weak<BollardDocker>,
    interval: Duration,
    request_concurrency: usize,
    request_timeout: Duration,
) {
    tracing::info!(sample_interval = ?interval, "starting container sample observer");
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;

            let Some(registry_ref) = registry.upgrade() else {
                tracing::debug!("stopping sample observer because docker service was dropped");
                return;
            };

            let docker = registry_ref.docker.clone();
            let registry_client = registry_ref.container_registry.clone();
            let sender = registry_ref.samples_tx.clone();
            drop(registry_ref);

            let running = registry_client.observed_containers();
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
                            request_timeout,
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
                .buffer_unordered(request_concurrency)
                .filter_map(|sample| async move { sample })
                .collect::<Vec<_>>()
                .await;
            if !samples.is_empty() && sender.send(samples).await.is_err() {
                return;
            }
        }
    });
}
