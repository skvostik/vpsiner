use bollard::models::{ContainerStatsResponse, EventMessage};
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::AppResult;
use crate::logs::{detect_level, parse_docker_timestamp, strip_ansi_escape_codes};
use crate::model::{ContainerState, ContainerStats, DockerEvent, LogLine, LogStream};

pub(super) fn map_container_state(value: Option<&str>) -> Option<ContainerState> {
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

pub(super) fn map_docker_event(message: EventMessage) -> Option<DockerEvent> {
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

pub(super) fn map_log_output(
    container_id: String,
    log_group: String,
    message: Bytes,
    stream: LogStream,
) -> Vec<AppResult<LogLine>> {
    String::from_utf8_lossy(&message)
        .lines()
        .map(|line| {
            let (ts, raw_line) = parse_docker_timestamp(line);
            let line = strip_ansi_escape_codes(raw_line);
            Ok(LogLine {
                ts,
                log_group: log_group.clone(),
                cid: container_id.clone(),
                stream,
                level: detect_level(&line),
                line,
            })
        })
        .collect()
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
            "group-1".to_string(),
            Bytes::from(line),
            LogStream::Stdout,
        );
        assert_eq!(lines.len(), 1);
        let line = lines.into_iter().next().unwrap().unwrap();
        let (expected_ts, _) =
            crate::logs::parse_docker_timestamp("2024-07-04T12:33:54.000000000Z INFO ready");
        assert_eq!(line.ts, expected_ts);
        assert_eq!(line.log_group, "group-1");
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
