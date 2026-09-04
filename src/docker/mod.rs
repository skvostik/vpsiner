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
use tokio::{sync::mpsc, sync::watch, task::JoinHandle};

use crate::config::DockerControlsMode;
use crate::docker::container_registry::{ContainerObserveAction, ObservedContainer};
use crate::docker::mapping::receiver_stream;
use crate::error::{AppError, AppResult};
use crate::metadata::MetadataStore;
use crate::model::{
    container_id::ContainerId,
    containers::{ContainerCommandResult, ContainerState, ContainerSummary},
    logs::{LogLine, LogStream},
    metrics::ContainerRawSample,
};

use self::mapping::map_log_output;
use self::raw::{ping, sample_container_stats, supports_write_operations};

use self::container_registry::{BollardContainerRegistry, ContainerRegistry};

/// Everything that talks to the Docker socket / proxy goes through this trait.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait DockerService: Send + Sync + 'static {
    fn containers_info(&self) -> AppResult<Vec<ContainerSummary>>;

    /// Bumped whenever `containers_info` is refreshed, so SSE subscribers know to re-read it.
    fn subscribe_containers_info(&self) -> watch::Receiver<u64>;

    fn controls_available(&self) -> bool;

    /// Whether the last probe reached the Docker socket/proxy.
    fn connected(&self) -> bool;

    fn logs(&self) -> BoxStream<'static, LogLine>;

    fn container_samples(&self) -> BoxStream<'static, Vec<ContainerRawSample>>;

    async fn start_container(&self, id: ContainerId) -> AppResult<ContainerCommandResult>;

    async fn stop_container(&self, id: ContainerId) -> AppResult<ContainerCommandResult>;

    async fn restart_container(&self, id: ContainerId) -> AppResult<ContainerCommandResult>;
}

/// Bollard-backed implementation. Wired up in the composition root.
pub struct BollardDocker {
    inner: Arc<Inner>,
}

struct Inner {
    docker: Docker,
    container_registry: Arc<dyn ContainerRegistry>,
    controls_available: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
    logs_tx: mpsc::Sender<LogLine>,
    samples_tx: mpsc::Sender<Vec<ContainerRawSample>>,
    logs_rx: Mutex<Option<mpsc::Receiver<LogLine>>>,
    samples_rx: Mutex<Option<mpsc::Receiver<Vec<ContainerRawSample>>>>,
    metadata: Arc<dyn MetadataStore>,
    retention_weeks: u32,
}

impl BollardDocker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        docker_host: impl Into<String>,
        timeout_secs: u64,
        request_timeout_secs: u64,
        sample_interval: Duration,
        docker_probe_interval: Duration,
        request_concurrency: usize,
        docker_retry_delay: Duration,
        log_channel_capacity: usize,
        samples_channel_capacity: usize,
        docker_events_channel_capacity: usize,
        docker_debounce: Duration,
        controls_mode: DockerControlsMode,
        metadata: Arc<dyn MetadataStore>,
        retention_weeks: u32,
    ) -> Self {
        let host = docker_host.into();
        let docker = if host.starts_with("unix://") {
            Docker::connect_with_unix(&host, timeout_secs, API_DEFAULT_VERSION)
                .expect("docker socket must be reachable")
        } else {
            Docker::connect_with_http(&host, timeout_secs, API_DEFAULT_VERSION)
                .expect("docker proxy/daemon must be reachable")
        };

        let controls_available = Arc::new(AtomicBool::new(matches!(
            controls_mode,
            DockerControlsMode::Enabled
        )));
        // Optimistic until the first probe completes, so startup does not block on Docker.
        let connected = Arc::new(AtomicBool::new(true));
        let request_timeout = Duration::from_secs(request_timeout_secs);
        let (logs_tx, logs_rx) = mpsc::channel::<LogLine>(log_channel_capacity);
        let (samples_tx, samples_rx) =
            mpsc::channel::<Vec<ContainerRawSample>>(samples_channel_capacity);

        let container_registry: Arc<dyn ContainerRegistry> =
            Arc::new(BollardContainerRegistry::new(
                docker.clone(),
                connected.clone(),
                request_concurrency,
                request_timeout,
                docker_probe_interval,
                docker_retry_delay,
                docker_events_channel_capacity,
                docker_debounce,
            ));

        let inner = Arc::new(Inner {
            docker,
            container_registry,
            controls_available,
            connected,
            logs_tx,
            samples_tx,
            logs_rx: Mutex::new(Some(logs_rx)),
            samples_rx: Mutex::new(Some(samples_rx)),
            metadata,
            retention_weeks,
        });

        match controls_mode {
            DockerControlsMode::Auto => {}
            DockerControlsMode::Enabled => {
                tracing::info!(
                    docker_controls_available = true,
                    "docker controls forced enabled by configuration"
                );
            }
            DockerControlsMode::Disabled => {
                tracing::info!(
                    docker_controls_available = false,
                    "docker controls forced disabled by configuration"
                );
            }
        }
        spawn_docker_probe(
            Arc::downgrade(&inner),
            docker_probe_interval,
            request_timeout,
            controls_mode,
        );
        spawn_log_observer(Arc::downgrade(&inner), docker_probe_interval);
        spawn_sample_observer(
            Arc::downgrade(&inner),
            sample_interval,
            request_concurrency,
            request_timeout,
        );

        Self { inner }
    }
}

#[async_trait]
impl DockerService for BollardDocker {
    fn containers_info(&self) -> AppResult<Vec<ContainerSummary>> {
        self.inner.container_registry.containers_info()
    }

    fn subscribe_containers_info(&self) -> watch::Receiver<u64> {
        self.inner.container_registry.subscribe()
    }

    fn controls_available(&self) -> bool {
        self.inner.controls_available.load(Ordering::Relaxed)
    }

    fn connected(&self) -> bool {
        self.inner.connected.load(Ordering::Relaxed)
    }

    fn logs(&self) -> BoxStream<'static, LogLine> {
        match self.inner.logs_rx.lock().ok().and_then(|mut rx| rx.take()) {
            Some(rx) => receiver_stream(rx),
            None => Box::pin(futures_util::stream::pending()),
        }
    }

    fn container_samples(&self) -> BoxStream<'static, Vec<ContainerRawSample>> {
        match self
            .inner
            .samples_rx
            .lock()
            .ok()
            .and_then(|mut rx| rx.take())
        {
            Some(rx) => receiver_stream(rx),
            None => Box::pin(futures_util::stream::pending()),
        }
    }

    async fn start_container(&self, id: ContainerId) -> AppResult<ContainerCommandResult> {
        ensure_connected(self.connected())?;
        if !self.controls_available() {
            return Err(AppError::Forbidden(
                "container controls are disabled or unavailable on this backend".into(),
            ));
        }
        let container = self
            .inner
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
        let docker = self.inner.docker.clone();
        let full_id = container.full_id.clone();
        let log_id = container.log_id();
        tokio::spawn(async move {
            if let Err(error) = docker.start_container(&full_id, None).await {
                tracing::warn!(container = %log_id, error = %error, "failed to start container");
            }
        });
        Ok(ContainerCommandResult::Submitted)
    }

    async fn stop_container(&self, id: ContainerId) -> AppResult<ContainerCommandResult> {
        ensure_connected(self.connected())?;
        if !self.controls_available() {
            return Err(AppError::Forbidden(
                "container controls are disabled or unavailable on this backend".into(),
            ));
        }
        let container = self
            .inner
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
        let docker = self.inner.docker.clone();
        let full_id = container.full_id.clone();
        let log_id = container.log_id();
        tokio::spawn(async move {
            if let Err(error) = docker.stop_container(&full_id, None).await {
                tracing::warn!(container = %log_id, error = %error, "failed to stop container");
            }
        });
        Ok(ContainerCommandResult::Submitted)
    }

    async fn restart_container(&self, id: ContainerId) -> AppResult<ContainerCommandResult> {
        ensure_connected(self.connected())?;
        if !self.controls_available() {
            return Err(AppError::Forbidden(
                "container controls are disabled or unavailable on this backend".into(),
            ));
        }
        let container = self
            .inner
            .container_registry
            .container_info(id)?
            .ok_or_else(|| AppError::NotFound(format!("container not found: {id}")))?;
        tracing::info!(container=%&container.log_id(), "attempting to restart container");
        match container.state.ok_or_else(|| {
            AppError::Docker(format!("container state unknown: {}", container.log_id()))
        })? {
            ContainerState::Created | ContainerState::Exited => {
                let docker = self.inner.docker.clone();
                let full_id = container.full_id.clone();
                let log_id = container.log_id();
                tokio::spawn(async move {
                    if let Err(error) = docker.start_container(&full_id, None).await {
                        tracing::warn!(container = %log_id, error = %error, "failed to start container");
                    }
                });
            }
            ContainerState::Running | ContainerState::Paused => {
                let docker = self.inner.docker.clone();
                let full_id = container.full_id.clone();
                let log_id = container.log_id();
                tokio::spawn(async move {
                    if let Err(error) = docker.restart_container(&full_id, None).await {
                        tracing::warn!(container = %log_id, error = %error, "failed to restart container");
                    }
                });
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

fn ensure_connected(connected: bool) -> AppResult<()> {
    if connected {
        return Ok(());
    }
    Err(AppError::Docker(
        "docker socket/proxy is unreachable".into(),
    ))
}

fn spawn_docker_probe(
    registry: Weak<Inner>,
    interval: Duration,
    request_timeout: Duration,
    controls_mode: DockerControlsMode,
) {
    tokio::spawn(async move {
        let mut last_connected: Option<bool> = None;
        let mut last_controls: Option<bool> = None;
        loop {
            let Some(registry_ref) = registry.upgrade() else {
                tracing::debug!("stopping docker probe worker because docker service was dropped");
                return;
            };

            let docker = registry_ref.docker.clone();
            let connected_flag = registry_ref.connected.clone();
            let controls_flag = registry_ref.controls_available.clone();
            drop(registry_ref);

            let ping_result = ping(&docker, request_timeout).await;
            let connected = ping_result.is_ok();
            connected_flag.store(connected, Ordering::Relaxed);
            if last_connected != Some(connected) {
                match &ping_result {
                    Ok(()) => tracing::info!(docker_connected = true, "docker connection is up"),
                    Err(err) => {
                        tracing::warn!(docker_connected = false, error = %err, "docker connection is down")
                    }
                }
                last_connected = Some(connected);
            }

            if connected && matches!(controls_mode, DockerControlsMode::Auto) {
                match supports_write_operations(&docker, request_timeout).await {
                    Ok(available) => {
                        controls_flag.store(available, Ordering::Relaxed);
                        if last_controls != Some(available) {
                            tracing::info!(
                                docker_controls_available = available,
                                "docker write-capability probe result"
                            );
                            last_controls = Some(available);
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "docker write-capability probe failed")
                    }
                }
            }
            tokio::time::sleep(interval).await;
        }
    });
}

fn spawn_log_observer(registry: Weak<Inner>, interval: Duration) {
    tokio::spawn(async move {
        let mut tasks = HashMap::<ContainerId, LogTask>::new();
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

            if !registry_ref.connected.load(Ordering::Relaxed) {
                drop(registry_ref);
                tracing::debug!("skipping log observer cycle while docker is unreachable");
                continue;
            }

            let docker = registry_ref.docker.clone();
            let sender = registry_ref.logs_tx.clone();
            let metadata = registry_ref.metadata.clone();
            let retention_weeks = registry_ref.retention_weeks;
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
                    tasks.insert(
                        container.id,
                        LogTask {
                            container: container.clone(),
                            handle: spawn_container_log_task(
                                docker.clone(),
                                container,
                                sender.clone(),
                                metadata.clone(),
                                retention_weeks,
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

fn clamp_to_i32(value: i64) -> i32 {
    match i32::try_from(value) {
        Ok(value) => value,
        Err(_) if value < 0 => i32::MIN,
        Err(_) => i32::MAX,
    }
}

fn log_since_secs(
    checkpoint: Option<crate::metadata::LogCheckpoint>,
    now: time::OffsetDateTime,
    retention_weeks: u32,
) -> i32 {
    let cutoff_secs = crate::retention::retention_cutoff_ms(now, retention_weeks).div_euclid(1_000);
    let checkpoint_secs = checkpoint.map(|checkpoint| checkpoint.ts.div_euclid(1_000));
    let since_secs = checkpoint_secs
        .map(|checkpoint_secs| checkpoint_secs.max(cutoff_secs))
        .unwrap_or(cutoff_secs);
    clamp_to_i32(since_secs)
}

fn spawn_container_log_task(
    docker: Docker,
    container: ObservedContainer,
    sender: mpsc::Sender<LogLine>,
    metadata: Arc<dyn MetadataStore>,
    retention_weeks: u32,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let checkpoint = match metadata
            .log_checkpoint(&container.service, container.id)
            .await
        {
            Ok(checkpoint) => checkpoint,
            Err(err) => {
                tracing::error!(container = %container.log_id(), error = %err, "failed to read log checkpoint; log task will respawn");
                return;
            }
        };

        let now = time::OffsetDateTime::now_utc();
        let cutoff_secs =
            crate::retention::retention_cutoff_ms(now, retention_weeks).div_euclid(1_000);
        if let Some(checkpoint) = checkpoint
            && checkpoint.ts.div_euclid(1_000) < cutoff_secs
        {
            tracing::debug!(
                container = %container.log_id(),
                checkpoint = %crate::logs::format_timestamp_ms(checkpoint.ts),
                cutoff = %crate::logs::format_timestamp_ms(cutoff_secs * 1_000),
                "clamping stale log checkpoint to retention cutoff"
            );
        }

        let since_secs = log_since_secs(checkpoint, now, retention_weeks);
        tracing::info!(container = %container.log_id(), since = %crate::logs::format_timestamp_ms(i64::from(since_secs) * 1_000), "spawning log task");
        let options = LogsOptionsBuilder::default()
            .follow(true)
            .stdout(true)
            .stderr(true)
            .timestamps(true)
            .since(since_secs)
            .tail("all")
            .build();
        let mut stream = docker.logs(&container.full_id, Some(options));
        while let Some(result) = stream.next().await {
            let lines = match result {
                Ok(LogOutput::StdOut { message }) => map_log_output(
                    container.id,
                    container.service.clone(),
                    message,
                    LogStream::Stdout,
                ),
                Ok(LogOutput::StdErr { message }) => map_log_output(
                    container.id,
                    container.service.clone(),
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
    registry: Weak<Inner>,
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

            if !registry_ref.connected.load(Ordering::Relaxed) {
                drop(registry_ref);
                tracing::debug!("skipping sample observer cycle while docker is unreachable");
                continue;
            }

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
                        match sample_container_stats(&docker, &container.full_id, request_timeout).await {
                            Ok(stats) => Some(ContainerRawSample {
                                ts: collection_ts,
                                service: container.service,
                                cid: stats.cid,
                                cpu_usage_ns: stats.cpu_usage_ns,
                                system_cpu_usage_ns: stats.system_cpu_usage_ns,
                                cpu_count: stats.cpu_count,
                                mem_used: stats.mem_used,
                                mem_limit: stats.mem_limit,
                                net_rx: stats.net_rx,
                                net_tx: stats.net_tx,
                                blk_read: stats.blk_read,
                                blk_write: stats.blk_write,
                            }),
                            Err(err) => {
                                tracing::warn!(container = %container.log_id(), error = %err, "failed to sample container stats");
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

#[cfg(test)]
mod tests {
    use super::{clamp_to_i32, log_since_secs};
    use crate::metadata::LogCheckpoint;

    #[test]
    fn uses_cutoff_when_checkpoint_is_older_than_retention_window() {
        let now = time::OffsetDateTime::new_utc(
            time::Date::from_calendar_date(2026, time::Month::August, 23).unwrap(),
            time::Time::MIDNIGHT,
        );

        let since_secs = log_since_secs(
            Some(LogCheckpoint {
                ts: 1_000,
                line_hash: 0,
            }),
            now,
            4,
        );

        let expected_cutoff_secs =
            crate::retention::retention_cutoff_ms(now, 4).div_euclid(1_000) as i32;
        assert_eq!(since_secs, expected_cutoff_secs);
    }

    #[test]
    fn keeps_recent_checkpoint_when_it_is_newer_than_cutoff() {
        let now = time::OffsetDateTime::new_utc(
            time::Date::from_calendar_date(2026, time::Month::August, 23).unwrap(),
            time::Time::MIDNIGHT,
        );
        let recent_checkpoint = crate::retention::retention_cutoff_ms(now, 4) + 5_000;

        let since_secs = log_since_secs(
            Some(LogCheckpoint {
                ts: recent_checkpoint,
                line_hash: 0,
            }),
            now,
            4,
        );

        assert_eq!(since_secs, (recent_checkpoint / 1_000) as i32);
    }

    #[test]
    fn clamps_large_values_to_i32_bounds() {
        assert_eq!(clamp_to_i32(i64::from(i32::MAX) + 1), i32::MAX);
        assert_eq!(clamp_to_i32(i64::from(i32::MIN) - 1), i32::MIN);
    }
}
