use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::Deserialize;
use std::collections::BTreeMap;

use crate::error::{AppError, AppResult};
use crate::model::logs::{LogFilter, LogLevel, LogPage, LogStream, ServiceStatus};
use crate::state::AppState;

#[derive(Debug, Default, Deserialize)]
pub struct LogQuery {
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub q: Option<String>,
    pub level: Option<String>,
    pub stream: Option<String>,
    pub limit: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
}

impl LogQuery {
    fn filter(self) -> AppResult<LogFilter> {
        if self.before.is_some() && self.after.is_some() {
            return Err(AppError::BadRequest(
                "before and after are mutually exclusive".into(),
            ));
        }
        if let (Some(from), Some(to)) = (self.from, self.to) {
            if from > to {
                return Err(AppError::BadRequest(
                    "from must be before or equal to to".into(),
                ));
            }
        }
        let levels = parse_levels(self.level)?;
        let streams = parse_streams(self.stream)?;
        Ok(LogFilter {
            from: self.from,
            to: self.to,
            query: self.q,
            levels,
            streams,
            limit: self.limit,
            before: self.before,
            after: self.after,
        })
    }
}

pub(crate) fn parse_levels(value: Option<String>) -> AppResult<Vec<LogLevel>> {
    value
        .map(|value| {
            value
                .split(',')
                .map(|item| match item {
                    "debug" => Ok(LogLevel::Debug),
                    "info" => Ok(LogLevel::Info),
                    "warn" => Ok(LogLevel::Warn),
                    "error" => Ok(LogLevel::Error),
                    _ => Err(AppError::BadRequest(format!("invalid log level: {item}"))),
                })
                .collect()
        })
        .transpose()
        .map(|value| value.unwrap_or_default())
}

pub(crate) fn parse_streams(value: Option<String>) -> AppResult<Vec<LogStream>> {
    value
        .map(|value| {
            value
                .split(',')
                .map(|item| match item {
                    "stdout" => Ok(LogStream::Stdout),
                    "stderr" => Ok(LogStream::Stderr),
                    _ => Err(AppError::BadRequest(format!("invalid log stream: {item}"))),
                })
                .collect()
        })
        .transpose()
        .map(|value| value.unwrap_or_default())
}

pub async fn list_groups(
    State(state): State<AppState>,
) -> AppResult<Json<BTreeMap<String, ServiceStatus>>> {
    let stored = state.metadata.list_service_log_watermarks().await?;
    let containers = state.docker.containers_info()?;
    Ok(Json(merge_services(stored, containers)))
}

pub(crate) fn merge_services(
    stored: BTreeMap<String, i64>,
    containers: Vec<crate::model::containers::ContainerSummary>,
) -> BTreeMap<String, ServiceStatus> {
    let mut services = stored
        .into_iter()
        .map(|(service, last_received)| {
            (
                service,
                ServiceStatus {
                    last_received: Some(last_received),
                    live: false,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for container in containers {
        let service = services
            .entry(container.service.clone())
            .or_insert_with(|| ServiceStatus {
                last_received: None,
                live: false,
            });
        service.live |= container.state == Some(crate::model::containers::ContainerState::Running);
    }
    services
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        container_id::ContainerId,
        containers::{ContainerState, ContainerSummary},
    };

    fn container(service: &str, state: ContainerState) -> ContainerSummary {
        ContainerSummary {
            id: ContainerId::parse("aaaaaaaaaaaa").unwrap(),
            full_id: format!("{service}-{state:?}"),
            name: service.into(),
            service: service.into(),
            image: String::new(),
            image_sha: String::new(),
            ports: Vec::new(),
            labels: Vec::new(),
            state: Some(state),
            started_at: None,
        }
    }

    #[test]
    fn merges_container_liveness_into_stored_groups() {
        let groups = merge_services(
            BTreeMap::from([("api".to_string(), 42)]),
            vec![
                container("api", ContainerState::Exited),
                container("api", ContainerState::Running),
                container("worker", ContainerState::Exited),
            ],
        );

        assert_eq!(
            groups,
            BTreeMap::from([
                (
                    "api".into(),
                    ServiceStatus {
                        last_received: Some(42),
                        live: true,
                    },
                ),
                (
                    "worker".into(),
                    ServiceStatus {
                        last_received: None,
                        live: false,
                    },
                ),
            ])
        );
    }
}

pub async fn query(
    State(state): State<AppState>,
    Path(service): Path<String>,
    Query(query): Query<LogQuery>,
) -> AppResult<Json<LogPage>> {
    Ok(Json(state.logs.query(&service, query.filter()?).await?))
}
