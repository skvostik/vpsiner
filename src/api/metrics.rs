use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::Deserialize;

use crate::error::{AppError, AppResult};
use crate::model::{
    ContainerGroupMetrics, ContainerMetricsByService, HostPoint, MetricsResolution,
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
    pub(crate) fn parse(self) -> AppResult<(TimeRange, MetricsResolution)> {
        if self.from > self.to {
            return Err(AppError::BadRequest(
                "from must be before or equal to to".into(),
            ));
        }
        Ok((
            TimeRange {
                from: self.from,
                to: self.to,
            },
            parse_resolution(&self.resolution)?,
        ))
    }
}

pub(crate) fn parse_resolution(value: &str) -> AppResult<MetricsResolution> {
    match value {
        "10s" => Ok(MetricsResolution::TenSeconds),
        "1m" => Ok(MetricsResolution::OneMinute),
        "5m" => Ok(MetricsResolution::FiveMinutes),
        "1h" => Ok(MetricsResolution::OneHour),
        value => Err(AppError::BadRequest(format!("invalid resolution: {value}"))),
    }
}

pub async fn host(
    State(state): State<AppState>,
    Query(query): Query<MetricsQuery>,
) -> AppResult<Json<Vec<HostPoint>>> {
    let (range, resolution) = query.parse()?;
    Ok(Json(state.metrics.query_host(range, resolution).await?))
}

pub async fn container(
    State(state): State<AppState>,
    Path(service): Path<String>,
    Query(query): Query<MetricsQuery>,
) -> AppResult<Json<ContainerGroupMetrics>> {
    let (range, resolution) = query.parse()?;
    Ok(Json(
        state
            .metrics
            .query_container(&service, range, resolution)
            .await?,
    ))
}

pub async fn containers_history(
    State(state): State<AppState>,
    Query(query): Query<MetricsQuery>,
) -> AppResult<Json<ContainerMetricsByService>> {
    let (range, resolution) = query.parse()?;
    Ok(Json(
        state.metrics.query_containers(range, resolution).await?,
    ))
}

pub async fn current(State(state): State<AppState>) -> Json<MetricsSnapshot> {
    Json(state.snapshot.current())
}
