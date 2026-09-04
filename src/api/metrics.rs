use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::Deserialize;

use crate::error::{AppError, AppResult};
use crate::model::metrics::{
    ContainerGroupMetrics, ContainerMetricsByService, HostPoint, MetricsResolution,
    MetricsResponse, MetricsSnapshot,
};
use crate::model::time::TimeRange;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct MetricsQuery {
    pub from: i64,
    pub to: i64,
}

impl MetricsQuery {
    pub(crate) fn parse(self) -> AppResult<TimeRange> {
        if self.from > self.to {
            return Err(AppError::BadRequest(
                "from must be before or equal to to".into(),
            ));
        }
        Ok(TimeRange {
            from: self.from,
            to: self.to,
        })
    }
}

pub async fn host(
    State(state): State<AppState>,
    Query(query): Query<MetricsQuery>,
) -> AppResult<Json<MetricsResponse<Vec<HostPoint>>>> {
    let range = query.parse()?;
    let resolution = MetricsResolution::for_range(range);
    Ok(Json(MetricsResponse {
        resolution: resolution.as_str().to_string(),
        data: state.metrics.query_host(range, resolution).await?,
    }))
}

pub async fn container(
    State(state): State<AppState>,
    Path(service): Path<String>,
    Query(query): Query<MetricsQuery>,
) -> AppResult<Json<MetricsResponse<ContainerGroupMetrics>>> {
    let range = query.parse()?;
    let resolution = MetricsResolution::for_range(range);
    let mut by_service = state.metrics.query_containers(range, resolution).await?;
    Ok(Json(MetricsResponse {
        resolution: resolution.as_str().to_string(),
        data: by_service.remove(&service).unwrap_or_default(),
    }))
}

pub async fn containers_history(
    State(state): State<AppState>,
    Query(query): Query<MetricsQuery>,
) -> AppResult<Json<MetricsResponse<ContainerMetricsByService>>> {
    let range = query.parse()?;
    let resolution = MetricsResolution::for_range(range);
    let by_service = state.metrics.query_containers(range, resolution).await?;
    Ok(Json(MetricsResponse {
        resolution: resolution.as_str().to_string(),
        data: by_service
            .into_iter()
            .map(|(service, group)| (service, group.sum))
            .collect(),
    }))
}

pub async fn current(State(state): State<AppState>) -> Json<MetricsSnapshot> {
    Json(state.snapshot.current())
}
