use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::Deserialize;
use std::collections::BTreeMap;

use crate::error::{AppError, AppResult};
use crate::model::{LogFilter, LogGroupStatus, LogGroupSummary, LogLevel, LogPage, LogStream};
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

fn parse_levels(value: Option<String>) -> AppResult<Vec<LogLevel>> {
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

fn parse_streams(value: Option<String>) -> AppResult<Vec<LogStream>> {
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
) -> AppResult<Json<BTreeMap<String, LogGroupStatus>>> {
    let stored = state.logs.list_groups().await?;
    let containers = state.docker.containers_info()?;
    Ok(Json(merge_log_groups(stored, containers)))
}

fn merge_log_groups(
    stored: Vec<LogGroupSummary>,
    containers: Vec<crate::model::ContainerSummary>,
) -> BTreeMap<String, LogGroupStatus> {
    let mut groups = stored
        .into_iter()
        .map(|group| {
            (
                group.log_group,
                LogGroupStatus {
                    last_received: group.last_received,
                    live: false,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for container in containers {
        let group = groups
            .entry(container.log_group.clone())
            .or_insert_with(|| LogGroupStatus {
                last_received: None,
                live: false,
            });
        group.live |= container.state == Some(crate::model::ContainerState::Running);
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContainerState, ContainerSummary};

    fn container(log_group: &str, state: ContainerState) -> ContainerSummary {
        ContainerSummary {
            id: format!("{log_group}-{state:?}"),
            name: log_group.into(),
            log_group: log_group.into(),
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
        let groups = merge_log_groups(
            vec![LogGroupSummary {
                log_group: "api".into(),
                last_received: Some(42),
            }],
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
                    LogGroupStatus {
                        last_received: Some(42),
                        live: true,
                    },
                ),
                (
                    "worker".into(),
                    LogGroupStatus {
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
    Path(log_group): Path<String>,
    Query(query): Query<LogQuery>,
) -> AppResult<Json<LogPage>> {
    Ok(Json(state.logs.query(&log_group, query.filter()?).await?))
}
