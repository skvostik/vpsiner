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

use super::CONTAINER_INSPECT_CONCURRENCY;
use super::CONTAINER_INSPECT_TIMEOUT;
use super::mapping::{map_container_stats, map_container_summary};

pub(super) async fn list_running_containers(docker: &Docker) -> AppResult<Vec<ObservedContainer>> {
    let options = ListContainersOptionsBuilder::new().all(false).build();
    let containers = docker
        .list_containers(Some(options))
        .await
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
) -> AppResult<Vec<ContainerSummary>> {
    let options = ListContainersOptionsBuilder::new().all(true).build();
    let containers = docker
        .list_containers(Some(options))
        .await
        .map_err(|err| AppError::Docker(err.to_string()))?;

    let summaries = futures_util::stream::iter(containers)
        .map(|container| async move {
            let summary = map_container_summary(&container);
            let started_at = if summary.state == Some(ContainerState::Running) {
                inspect_started_at(docker, &summary.id)
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
        .buffer_unordered(CONTAINER_INSPECT_CONCURRENCY)
        .filter_map(|summary| async move { summary })
        .collect::<Vec<_>>()
        .await;

    Ok(summaries)
}

async fn inspect_started_at(docker: &Docker, id: &str) -> Option<TimestampMs> {
    tokio::time::timeout(
        CONTAINER_INSPECT_TIMEOUT,
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
