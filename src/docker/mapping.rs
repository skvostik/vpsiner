use bollard::models::ContainerStatsResponse;
use bollard::plugin::ContainerSummaryStateEnum;
use bytes::Bytes;
use futures_util::stream::{BoxStream, StreamExt};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use crate::logs::{detect_level, parse_docker_timestamp, strip_ansi_escape_codes};
use crate::model::{ContainerState, ContainerStats, ContainerSummary, LogLine, LogStream};

pub(super) fn receiver_stream<T: Send + 'static>(rx: mpsc::Receiver<T>) -> BoxStream<'static, T> {
    futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    })
    .boxed()
}

pub(super) fn get_container_name(response: &bollard::models::ContainerSummary) -> String {
    let names = response.names.clone().unwrap_or_default();
    names
        .first()
        .map(|raw| raw.trim_start_matches('/').to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

pub(super) fn get_container_service(response: &bollard::models::ContainerSummary) -> String {
    let labels = response.labels.clone().unwrap_or_default();
    let name = get_container_name(response);
    if let Some(value) = labels
        .get("vpsiner.service")
        .filter(|value| !value.trim().is_empty())
    {
        return value.trim().to_string();
    }

    let compose_project = labels
        .get("com.docker.compose.project")
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_string());
    let compose_service = labels
        .get("com.docker.compose.service")
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_string());

    if let (Some(project), Some(service)) = (compose_project, compose_service) {
        return format!("{project}-{service}");
    }

    name.trim_start_matches('/').to_string()
}

pub(super) fn get_container_labels(response: &bollard::models::ContainerSummary) -> Vec<String> {
    let labels = response.labels.clone().unwrap_or_default();
    let mut label_list = labels
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    label_list.sort();
    label_list
}

pub(super) fn get_container_state(
    response: &bollard::models::ContainerSummary,
) -> Option<ContainerState> {
    let state = response.state;
    match state {
        Some(ContainerSummaryStateEnum::CREATED) => Some(ContainerState::Created),
        Some(ContainerSummaryStateEnum::RESTARTING) => Some(ContainerState::Restarting),
        Some(ContainerSummaryStateEnum::RUNNING) => Some(ContainerState::Running),
        Some(ContainerSummaryStateEnum::REMOVING) => Some(ContainerState::Removing),
        Some(ContainerSummaryStateEnum::PAUSED) => Some(ContainerState::Paused),
        Some(ContainerSummaryStateEnum::EXITED) => Some(ContainerState::Exited),
        Some(ContainerSummaryStateEnum::DEAD) => Some(ContainerState::Dead),
        Some(ContainerSummaryStateEnum::STOPPING) => Some(ContainerState::Stopping),
        Some(ContainerSummaryStateEnum::EMPTY) => Some(ContainerState::Empty),
        None => None,
    }
}

pub(super) fn get_container_ports(response: &bollard::models::ContainerSummary) -> Vec<String> {
    let mut ports = response
        .ports
        .clone()
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
    ports.into_iter().map(|(_, _, display)| display).collect()
}

pub(super) fn map_container_summary(
    response: &bollard::models::ContainerSummary,
) -> ContainerSummary {
    let name = get_container_name(response);
    let service = get_container_service(response);
    let label_list = get_container_labels(response);
    let state = get_container_state(response);
    let ports = get_container_ports(response);

    let id = response.id.clone().unwrap_or_default();
    ContainerSummary {
        id,
        name: name.clone(),
        service,
        image: response.image.clone().unwrap_or_default(),
        image_sha: response.image_id.clone().unwrap_or_default(),
        ports,
        labels: label_list,
        state,
        started_at: None,
    }
}

pub(super) fn map_container_stats(
    response: ContainerStatsResponse,
    fallback_id: &str,
) -> ContainerStats {
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

    let memory_stats = response.memory_stats.as_ref();
    let mem_usage = memory_stats.and_then(|stats| stats.usage).unwrap_or(0);
    // Page cache counted in cgroup usage isn't "used" memory; match `docker stats` by subtracting it.
    let mem_cache = memory_stats
        .and_then(|stats| stats.stats.as_ref())
        .and_then(|stats| {
            stats
                .get("inactive_file")
                .or_else(|| stats.get("total_inactive_file"))
        })
        .copied()
        .unwrap_or(0);

    ContainerStats {
        ts,
        cid: response.id.unwrap_or_else(|| fallback_id.to_string()),
        cpu_pct,
        mem_used: mem_usage.saturating_sub(mem_cache),
        mem_limit: memory_stats.and_then(|stats| stats.limit).unwrap_or(0),
        net_rx,
        net_tx,
        blk_read,
        blk_write,
    }
}

pub(super) fn map_log_output(
    container_id: String,
    service: String,
    message: Bytes,
    stream: LogStream,
) -> Vec<LogLine> {
    String::from_utf8_lossy(&message)
        .lines()
        .map(|line| {
            let (ts, raw_line) = parse_docker_timestamp(line);
            let line = strip_ansi_escape_codes(raw_line);
            LogLine {
                ts,
                service: service.clone(),
                cid: container_id.clone(),
                stream,
                level: detect_level(&line),
                line,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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
            "group-1".to_string(),
            Bytes::from(line),
            LogStream::Stdout,
        );
        assert_eq!(lines.len(), 1);
        let line = lines.into_iter().next().unwrap();
        let (expected_ts, _) =
            crate::logs::parse_docker_timestamp("2024-07-04T12:33:54.000000000Z INFO ready");
        assert_eq!(line.ts, expected_ts);
        assert_eq!(line.service, "group-1");
        assert_eq!(line.line, "INFO ready");
    }
}
