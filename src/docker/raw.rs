use bollard::{
    Docker,
    query_parameters::{ListContainersOptionsBuilder, StatsOptionsBuilder},
};
use futures_util::StreamExt;

use crate::model::{ContainerState, ContainerStats, ContainerSummary, TimestampMs};
use crate::{
    docker::{
        container_registry::ObservedContainer,
        mapping::{get_container_log_group, get_container_name},
    },
    error::{AppError, AppResult},
};

use super::mapping::{map_container_stats, map_container_summary};

pub(super) async fn list_running_containers(
    docker: &Docker,
    request_timeout: std::time::Duration,
) -> AppResult<Vec<ObservedContainer>> {
    let options = ListContainersOptionsBuilder::new().all(false).build();
    let containers = tokio::time::timeout(request_timeout, docker.list_containers(Some(options)))
        .await
        .map_err(|err| AppError::Docker(err.to_string()))?
        .map_err(|err| AppError::Docker(err.to_string()))?;

    let observed_containers = containers
        .into_iter()
        .map(|response| ObservedContainer {
            id: response.id.clone().unwrap_or_default(),
            name: get_container_name(&response),
            log_group: get_container_log_group(&response),
        })
        .collect::<Vec<_>>();

    Ok(observed_containers)
}

/// Lists all container details by inspecting each container.
/// Potentionally expensive operation as it inspects each container individually.
pub(super) async fn list_all_containers_details(
    docker: &Docker,
    request_concurrency: usize,
    request_timeout: std::time::Duration,
) -> AppResult<Vec<ContainerSummary>> {
    let options = ListContainersOptionsBuilder::new().all(true).build();
    let containers = tokio::time::timeout(request_timeout, docker.list_containers(Some(options)))
        .await
        .map_err(|err| AppError::Docker(err.to_string()))?
        .map_err(|err| AppError::Docker(err.to_string()))?;

    let summaries = futures_util::stream::iter(containers)
        .map(|container| async move {
            let summary = map_container_summary(&container);
            let started_at = if summary.state == Some(ContainerState::Running) {
                inspect_started_at(docker, &summary.id, request_timeout)
                    .await
                    .or_else(|| None)
            } else {
                None
            };

            Some(ContainerSummary {
                id: summary.id,
                name: summary.name,
                log_group: summary.log_group,
                image: summary.image,
                image_sha: summary.image_sha,
                ports: summary.ports,
                labels: summary.labels,
                state: summary.state,
                started_at,
            })
        })
        .buffer_unordered(request_concurrency)
        .filter_map(|summary| async move { summary })
        .collect::<Vec<_>>()
        .await;

    Ok(summaries)
}

async fn inspect_started_at(
    docker: &Docker,
    id: &str,
    request_timeout: std::time::Duration,
) -> Option<TimestampMs> {
    tokio::time::timeout(request_timeout, docker.inspect_container(id, None))
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

pub(super) async fn supports_write_operations(
    docker: &Docker,
    request_timeout: std::time::Duration,
) -> AppResult<bool> {
    const PROBE_ID: &str = "vpsiner-write-probe-0000000000000000000000000000000000000000";
    match tokio::time::timeout(request_timeout, docker.start_container(PROBE_ID, None)).await {
        Err(error) => Err(AppError::Docker(error.to_string())),
        Ok(Ok(_)) => Ok(true),
        Ok(Err(bollard::errors::Error::DockerResponseServerError {
            status_code: 404, ..
        })) => Ok(true),
        Ok(Err(bollard::errors::Error::DockerResponseServerError {
            status_code,
            message: server_message,
        })) => {
            tracing::info!(
                status_code,
                server_message,
                "docker write probe blocked; container controls will be reported unavailable"
            );
            Ok(false)
        }
        Ok(Err(err)) => Err(AppError::Docker(err.to_string())),
    }
}

pub(super) async fn sample_container_stats(
    docker: &Docker,
    id: &str,
    request_timeout: std::time::Duration,
) -> AppResult<ContainerStats> {
    let options = StatsOptionsBuilder::default()
        .stream(false)
        .one_shot(false)
        .build();
    let mut stream = docker.stats(id, Some(options));
    match tokio::time::timeout(request_timeout, stream.next()).await {
        Err(error) => Err(AppError::Docker(error.to_string())),
        Ok(None) => Err(AppError::Docker(
            "container stats stream ended without a sample".into(),
        )),
        Ok(Some(Ok(response))) => Ok(map_container_stats(response, id)),
        Ok(Some(Err(err))) => Err(AppError::Docker(err.to_string())),
    }
}
