use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::Deserialize;

use crate::error::{AppError, AppResult};
use crate::model::{
    ContainerGroupMetrics, ContainerMetricsByLogGroup, HostSample, MetricsResolution,
    MetricsSnapshot, TimeRange,
};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct MetricsQuery {
    pub from: i64,
    pub to: i64,
    pub resolution: String,
}

impl MetricsQuery {
    fn parse(self) -> AppResult<(TimeRange, MetricsResolution)> {
        if self.from > self.to {
            return Err(AppError::BadRequest(
                "from must be before or equal to to".into(),
            ));
        }
        let resolution = match self.resolution.as_str() {
            "10s" => MetricsResolution::TenSeconds,
            "1m" => MetricsResolution::OneMinute,
            "5m" => MetricsResolution::FiveMinutes,
            "1h" => MetricsResolution::OneHour,
            value => {
                return Err(AppError::BadRequest(format!("invalid resolution: {value}")));
            }
        };
        Ok((
            TimeRange {
                from: self.from,
                to: self.to,
            },
            resolution,
        ))
    }
}

pub async fn host(
    State(state): State<AppState>,
    Query(query): Query<MetricsQuery>,
) -> AppResult<Json<Vec<HostSample>>> {
    let (range, resolution) = query.parse()?;
    Ok(Json(state.metrics.query_host(range, resolution).await?))
}

pub async fn container(
    State(state): State<AppState>,
    Path(log_group): Path<String>,
    Query(query): Query<MetricsQuery>,
) -> AppResult<Json<ContainerGroupMetrics>> {
    let (range, resolution) = query.parse()?;
    Ok(Json(
        state
            .metrics
            .query_container(&log_group, range, resolution)
            .await?,
    ))
}

pub async fn containers_history(
    State(state): State<AppState>,
    Query(query): Query<MetricsQuery>,
) -> AppResult<Json<ContainerMetricsByLogGroup>> {
    let (range, resolution) = query.parse()?;
    Ok(Json(
        state.metrics.query_containers(range, resolution).await?,
    ))
}

pub async fn current(State(state): State<AppState>) -> Json<MetricsSnapshot> {
    Json(state.snapshot.current())
}
