use bollard::{
    Docker,
    query_parameters::{ListContainersOptions, StatsOptionsBuilder},
};
use futures_util::StreamExt;
use std::time::Duration;

use crate::error::{AppError, AppResult};
use crate::model::{
    ContainerState, ContainerStats, ContainerSummary, TimestampMs, resolve_log_group,
};

use super::mapping::{map_container_state, map_container_stats};

const STARTED_AT_INSPECT_TIMEOUT: Duration = Duration::from_millis(750);

pub(super) async fn list_containers(
    docker: &Docker,
    previous: &[ContainerSummary],
) -> AppResult<Vec<ContainerSummary>> {
    let options = ListContainersOptions {
        all: true,
        filters: None,
        ..Default::default()
    };

    let containers = docker
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
        let Some(state) = map_container_state(container.state.as_ref().map(|state| state.as_ref()))
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
            inspect_started_at(docker, &id)
                .await
                .or_else(|| previous_started_at(previous, &id))
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

fn previous_started_at(previous: &[ContainerSummary], id: &str) -> Option<TimestampMs> {
    previous
        .iter()
        .find(|container| container.id == id)
        .and_then(|container| container.started_at)
}

async fn inspect_started_at(docker: &Docker, id: &str) -> Option<TimestampMs> {
    tokio::time::timeout(
        STARTED_AT_INSPECT_TIMEOUT,
        docker.inspect_container(id, None),
    )
    .await
    .ok()?
    .ok()?
    .state?
    .started_at
    .and_then(|value| {
        time::OffsetDateTime::parse(&value, &time::format_description::well_known::Rfc3339).ok()
    })
    .map(|value| value.unix_timestamp_nanos().div_euclid(1_000_000) as i64)
}

pub(super) async fn supports_write_operations(docker: &Docker) -> AppResult<bool> {
    const PROBE_ID: &str = "vpsiner-write-probe-0000000000000000000000000000000000000000";
    match docker.start_container(PROBE_ID, None).await {
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

pub(super) async fn sample_container_stats(docker: &Docker, id: &str) -> AppResult<ContainerStats> {
    let options = StatsOptionsBuilder::default()
        .stream(false)
        .one_shot(false)
        .build();
    let mut stream = docker.stats(id, Some(options));
    match stream.next().await {
        Some(Ok(response)) => Ok(map_container_stats(response, id)),
        Some(Err(err)) => Err(AppError::Docker(err.to_string())),
        None => Err(AppError::Docker(
            "container stats stream ended without a sample".into(),
        )),
    }
}
