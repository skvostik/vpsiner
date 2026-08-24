use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bollard::{
    API_DEFAULT_VERSION, Docker,
    container::LogOutput,
    models::{ContainerStatsResponse, EventMessage},
    query_parameters::{
        EventsOptionsBuilder, ListContainersOptions, LogsOptionsBuilder, StatsOptionsBuilder,
    },
};
use futures_util::{StreamExt, stream::BoxStream};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{AppError, AppResult};
use crate::logs::{detect_level, parse_docker_timestamp, strip_ansi_escape_codes};
use crate::model::{
    ContainerState, ContainerStats, ContainerSummary, DockerEvent, DockerInfo, LogLine, LogStream,
    resolve_log_group,
};

/// Everything that talks to the Docker socket / proxy goes through this trait.
#[allow(dead_code)] // remaining methods are consumed by the collectors added in later steps
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait DockerService: Send + Sync + 'static {
    async fn info(&self) -> AppResult<DockerInfo>;

    async fn list_containers(&self) -> AppResult<Vec<ContainerSummary>>;

    async fn container_state(&self, id: &str) -> AppResult<ContainerState>;

    async fn start_container(&self, id: &str) -> AppResult<()>;

    async fn stop_container(&self, id: &str) -> AppResult<()>;

    async fn restart_container(&self, id: &str) -> AppResult<()>;

    /// Probes whether write endpoints (start/stop/restart) are reachable, e.g. rejected by a
    /// read-only socket proxy. Must not mutate any real container.
    async fn supports_write_operations(&self) -> AppResult<bool>;

    async fn stats_stream(
        &self,
        id: &str,
    ) -> AppResult<BoxStream<'static, AppResult<ContainerStats>>>;

    async fn log_stream(
        &self,
        id: &str,
        since_secs: i32,
        follow: bool,
    ) -> AppResult<BoxStream<'static, AppResult<LogLine>>>;

    async fn events(&self) -> AppResult<BoxStream<'static, AppResult<DockerEvent>>>;
}

/// Bollard-backed implementation. Wired up in the composition root.
pub struct BollardDocker {
    docker: Docker,
}

impl BollardDocker {
    pub fn new(docker_host: impl Into<String>, timeout_secs: u64) -> Self {
        let host = docker_host.into();
        let docker = if host.starts_with("unix://") {
            Docker::connect_with_unix(&host, timeout_secs, API_DEFAULT_VERSION)
                .expect("docker socket must be reachable")
        } else {
            Docker::connect_with_http(&host, timeout_secs, API_DEFAULT_VERSION)
                .expect("docker proxy/daemon must be reachable")
        };

        Self { docker }
    }
}

fn map_container_state(value: Option<&str>) -> Option<ContainerState> {
    match value {
        Some("created") => Some(ContainerState::Created),
        Some("restarting") => Some(ContainerState::Restarting),
        Some("running") => Some(ContainerState::Running),
        Some("removing") => Some(ContainerState::Removing),
        Some("paused") => Some(ContainerState::Paused),
        Some("exited") => Some(ContainerState::Exited),
        Some("dead") => Some(ContainerState::Dead),
        _ => None,
    }
}

fn map_container_stats(response: ContainerStatsResponse, fallback_id: &str) -> ContainerStats {
    let cpu_pct = response
        .cpu_stats
        .as_ref()
        .and_then(|current| {
            let current_total = current.cpu_usage.as_ref()?.total_usage?;
            let previous_total = response
                .precpu_stats
                .as_ref()?
                .cpu_usage
                .as_ref()?
                .total_usage?;
            let current_system = current.system_cpu_usage?;
            let previous_system = response.precpu_stats.as_ref()?.system_cpu_usage?;
            let cpu_delta = current_total.saturating_sub(previous_total);
            let system_delta = current_system.saturating_sub(previous_system);
            if system_delta == 0 {
                return Some(0.0);
            }
            let cpu_count = current
                .online_cpus
                .or_else(|| {
                    current
                        .cpu_usage
                        .as_ref()?
                        .percpu_usage
                        .as_ref()
                        .map(|cpus| cpus.len() as u32)
                })
                .unwrap_or(1);
            Some((cpu_delta as f64 / system_delta as f64) * cpu_count as f64 * 100.0)
        })
        .unwrap_or(0.0);

    let (net_rx, net_tx) =
        response
            .networks
            .unwrap_or_default()
            .values()
            .fold((0_u64, 0_u64), |totals, network| {
                (
                    totals.0.saturating_add(network.rx_bytes.unwrap_or(0)),
                    totals.1.saturating_add(network.tx_bytes.unwrap_or(0)),
                )
            });

    let (blk_read, blk_write) = response
        .blkio_stats
        .and_then(|stats| stats.io_service_bytes_recursive)
        .unwrap_or_default()
        .into_iter()
        .fold((0_u64, 0_u64), |totals, entry| {
            match entry.op.as_deref().map(str::to_ascii_lowercase).as_deref() {
                Some("read") => (totals.0.saturating_add(entry.value.unwrap_or(0)), totals.1),
                Some("write") => (totals.0, totals.1.saturating_add(entry.value.unwrap_or(0))),
                _ => totals,
            }
        });

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default();

    ContainerStats {
        ts,
        cid: response.id.unwrap_or_else(|| fallback_id.to_string()),
        cpu_pct,
        mem_used: response
            .memory_stats
            .as_ref()
            .and_then(|stats| stats.usage)
            .unwrap_or(0),
        mem_limit: response
            .memory_stats
            .as_ref()
            .and_then(|stats| stats.limit)
            .unwrap_or(0),
        net_rx,
        net_tx,
        blk_read,
        blk_write,
    }
}

fn map_docker_event(message: EventMessage) -> Option<DockerEvent> {
    let kind = match message.action.as_deref()? {
        "create" => crate::model::DockerEventKind::Create,
        "start" => crate::model::DockerEventKind::Start,
        "die" => crate::model::DockerEventKind::Die,
        "destroy" => crate::model::DockerEventKind::Destroy,
        _ => return None,
    };
    let ts = message.time.unwrap_or_default().saturating_mul(1_000);
    let container_id = message.actor?.id?;
    Some(DockerEvent {
        ts,
        kind,
        container_id,
    })
}

#[async_trait]
impl DockerService for BollardDocker {
    async fn info(&self) -> AppResult<DockerInfo> {
        let info = self
            .docker
            .info()
            .await
            .map_err(|err| AppError::Docker(err.to_string()))?;

        Ok(DockerInfo {
            version: info.server_version.unwrap_or_default(),
            engine: info.operating_system.unwrap_or_default(),
            containers_running: info.containers_running.unwrap_or_default() as u64,
        })
    }

    async fn list_containers(&self) -> AppResult<Vec<ContainerSummary>> {
        let options = ListContainersOptions {
            all: true,
            filters: None,
            ..Default::default()
        };

        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .map_err(|err| AppError::Docker(err.to_string()))?;

        let mut summaries = Vec::with_capacity(containers.len());

        for container in containers {
            let names = container.names.unwrap_or_default();
            let name = names
                .first()
                .map(|raw| raw.trim_start_matches('/').to_string())
                .unwrap_or_else(|| "unknown".to_string());

            let labels = container.labels.unwrap_or_default();
            let log_group = resolve_log_group(&labels, &name);
            let mut label_list = labels
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>();
            label_list.sort();
            let Some(state) =
                map_container_state(container.state.as_ref().map(|state| state.as_ref()))
            else {
                continue;
            };
            let mut ports = container
                .ports
                .unwrap_or_default()
                .into_iter()
                .map(|port| {
                    let sort_port = port.public_port.unwrap_or(port.private_port);
                    let private_port = port.private_port;
                    let protocol = port
                        .typ
                        .map(|typ| format!("{typ:?}").to_ascii_lowercase())
                        .unwrap_or_else(|| "tcp".to_string());
                    let display = match (port.ip.as_deref(), port.public_port) {
                        (Some(ip), Some(public_port)) => {
                            format!("{ip}:{public_port}->{} / {protocol}", port.private_port)
                        }
                        (None, Some(public_port)) => {
                            format!("{public_port}->{} / {protocol}", port.private_port)
                        }
                        _ => format!("{} / {protocol}", port.private_port),
                    };
                    (sort_port, private_port, display)
                })
                .collect::<Vec<_>>();
            ports.sort_by(|left, right| left.cmp(right));
            let ports = ports.into_iter().map(|(_, _, display)| display).collect();
            let id = container.id.unwrap_or_default();
            let started_at = if state == ContainerState::Running {
                self.docker
                    .inspect_container(&id, None)
                    .await
                    .ok()
                    .and_then(|details| details.state?.started_at)
                    .and_then(|value| {
                        time::OffsetDateTime::parse(
                            &value,
                            &time::format_description::well_known::Rfc3339,
                        )
                        .ok()
                    })
                    .map(|value| value.unix_timestamp_nanos().div_euclid(1_000_000) as i64)
            } else {
                None
            };

            summaries.push(ContainerSummary {
                id,
                name: name.clone(),
                log_group,
                image: container.image.unwrap_or_default(),
                image_sha: container.image_id.unwrap_or_default(),
                ports,
                labels: label_list,
                state,
                started_at,
            });
        }

        Ok(summaries)
    }

    async fn container_state(&self, id: &str) -> AppResult<ContainerState> {
        let containers = self.list_containers().await?;
        let needle = id.trim();
        containers
            .into_iter()
            .find(|container| {
                container.id == needle
                    || container.id.starts_with(needle)
                    || container.name == needle
            })
            .map(|container| container.state)
            .ok_or_else(|| AppError::NotFound(format!("container not found: {needle}")))
    }

    async fn start_container(&self, id: &str) -> AppResult<()> {
        self.docker
            .start_container(id, None)
            .await
            .map_err(|err| AppError::Docker(err.to_string()))?;
        Ok(())
    }

    async fn stop_container(&self, id: &str) -> AppResult<()> {
        self.docker
            .stop_container(id, None)
            .await
            .map_err(|err| AppError::Docker(err.to_string()))?;
        Ok(())
    }

    async fn restart_container(&self, id: &str) -> AppResult<()> {
        self.docker
            .restart_container(id, None)
            .await
            .map_err(|err| AppError::Docker(err.to_string()))?;
        Ok(())
    }

    async fn supports_write_operations(&self) -> AppResult<bool> {
        // Nonexistent id: a real daemon reports 404 (write path reached), while a socket proxy
        // that blocks the method reports 403/405 before ever looking the container up.
        const PROBE_ID: &str = "vpsiner-write-probe-0000000000000000000000000000000000000000";
        match self.docker.start_container(PROBE_ID, None).await {
            Ok(_) => Ok(true),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(true),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code,
                message: server_message,
            }) => {
                tracing::info!(
                    status_code,
                    server_message,
                    "docker write probe blocked; container controls will be reported unavailable"
                );
                Ok(false)
            }
            Err(err) => Err(AppError::Docker(err.to_string())),
        }
    }

    async fn stats_stream(
        &self,
        id: &str,
    ) -> AppResult<BoxStream<'static, AppResult<ContainerStats>>> {
        let options = StatsOptionsBuilder::default()
            .stream(true)
            .one_shot(false)
            .build();
        let stream = self.docker.stats(id, Some(options));
        let fallback_id = id.to_string();
        Ok(Box::pin(stream.skip(1).map(move |result| {
            result
                .map(|response| map_container_stats(response, &fallback_id))
                .map_err(|err| AppError::Docker(err.to_string()))
        })))
    }

    async fn log_stream(
        &self,
        id: &str,
        since_secs: i32,
        follow: bool,
    ) -> AppResult<BoxStream<'static, AppResult<LogLine>>> {
        let options = LogsOptionsBuilder::default()
            .follow(follow)
            .stdout(true)
            .stderr(true)
            .timestamps(true)
            .since(since_secs)
            .tail("all")
            .build();
        let stream = self.docker.logs(id, Some(options));
        let container_id = id.to_string();
        Ok(Box::pin(stream.flat_map(move |result| {
            let lines = match result {
                Ok(LogOutput::StdOut { message }) => {
                    map_log_output(container_id.clone(), message, LogStream::Stdout)
                }
                Ok(LogOutput::StdErr { message }) => {
                    map_log_output(container_id.clone(), message, LogStream::Stderr)
                }
                Ok(_) => Vec::new(),
                Err(err) => vec![Err(AppError::Docker(err.to_string()))],
            };
            futures_util::stream::iter(lines)
        })))
    }

    async fn events(&self) -> AppResult<BoxStream<'static, AppResult<DockerEvent>>> {
        let mut filters = HashMap::new();
        filters.insert("type".to_string(), vec!["container".to_string()]);
        let options = EventsOptionsBuilder::default().filters(&filters).build();
        let stream = self.docker.events(Some(options));
        Ok(Box::pin(stream.filter_map(|result| async move {
            match result {
                Ok(message) => map_docker_event(message).map(Ok),
                Err(err) => Some(Err(AppError::Docker(err.to_string()))),
            }
        })))
    }
}

fn map_log_output(
    container_id: String,
    message: bytes::Bytes,
    stream: LogStream,
) -> Vec<AppResult<LogLine>> {
    String::from_utf8_lossy(&message)
        .lines()
        .map(|line| {
            let (ts, raw_line) = parse_docker_timestamp(line);
            let line = strip_ansi_escape_codes(raw_line);
            Ok(LogLine {
                ts,
                cid: container_id.clone(),
                stream,
                level: detect_level(&line),
                line,
            })
        })
        .collect()
}

/// Repeatedly probes write-endpoint availability and keeps `flag` up to date, since a socket
/// proxy's policy can change (or the socket can be swapped) without restarting the app.
pub async fn run_write_probe(
    docker: Arc<dyn DockerService>,
    flag: Arc<AtomicBool>,
    interval: Duration,
) {
    let mut last_logged: Option<bool> = None;
    loop {
        match docker.supports_write_operations().await {
            Ok(available) => {
                flag.store(available, Ordering::Relaxed);
                // Only log on change to avoid spamming at every probe interval.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use bollard::models::EventActor;

    #[test]
    fn strips_ansi_codes_from_log_text() {
        assert_eq!(
            crate::logs::strip_ansi_escape_codes("\u{1b}[32mINFO\u{1b}[0m ready"),
            "INFO ready"
        );
        assert_eq!(crate::logs::strip_ansi_escape_codes("plain"), "plain");
    }

    #[test]
    fn map_log_output_strips_ansi_codes() {
        let line = "2024-07-04T12:33:54.000000000Z \u{1b}[32mINFO\u{1b}[0m ready\n";
        let lines = map_log_output(
            "container-1".to_string(),
            bytes::Bytes::from(line),
            LogStream::Stdout,
        );
        assert_eq!(lines.len(), 1);
        let line = lines.into_iter().next().unwrap().unwrap();
        let (expected_ts, _) =
            crate::logs::parse_docker_timestamp("2024-07-04T12:33:54.000000000Z INFO ready");
        assert_eq!(line.ts, expected_ts);
        assert_eq!(line.line, "INFO ready");
    }

    #[test]
    fn maps_container_event_action_and_timestamp() {
        let event = map_docker_event(EventMessage {
            action: Some("die".into()),
            actor: Some(EventActor {
                id: Some("container-id".into()),
                ..Default::default()
            }),
            time: Some(42),
            ..Default::default()
        })
        .unwrap();

        assert_eq!(event.kind, crate::model::DockerEventKind::Die);
        assert_eq!(event.container_id, "container-id");
        assert_eq!(event.ts, 42_000);
    }

    #[test]
    fn ignores_untracked_docker_actions() {
        let event = map_docker_event(EventMessage {
            action: Some("rename".into()),
            actor: Some(EventActor {
                id: Some("container-id".into()),
                ..Default::default()
            }),
            ..Default::default()
        });

        assert!(event.is_none());
    }
}
