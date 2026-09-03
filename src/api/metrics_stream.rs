//! SSE versions of the time-series metrics endpoints. Unlike `stream.rs`'s snapshot/diff
//! endpoints, these are parameterized per connection (`from`, and `service`
//! for the per-service endpoint) and driven by `BucketWatcher`, not a data-change signal: a new
//! point only exists once wall-clock time crosses the next completed bucket for that resolution.
//! Each endpoint watches only its own sample source, so the two collectors never wake each other.
//! There is no `to` — a live stream always runs up to the server's own "now" and keeps going.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::error::{AppError, AppResult};
use crate::metrics::bucket_watcher::MetricsSource;
use crate::metrics::store::MetricsStore;
use crate::model::{
    ContainerGroupMetricsAppend, GroupPoint, MetricsResolution, MetricsResponse, TimeRange,
    TimestampMs,
};
use crate::state::AppState;

fn now_ms() -> TimestampMs {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Deserialize)]
pub struct MetricsStreamQuery {
    pub from: i64,
}

impl MetricsStreamQuery {
    fn parse(self) -> AppResult<(TimeRange, MetricsResolution)> {
        let to = now_ms();
        if self.from > to {
            return Err(AppError::BadRequest(
                "from must not be in the future".into(),
            ));
        }
        Ok((
            TimeRange {
                from: self.from,
                to,
            },
            MetricsResolution::for_range(TimeRange {
                from: self.from,
                to,
            }),
        ))
    }
}

enum HostStreamState {
    Initial(
        Arc<dyn MetricsStore>,
        watch::Receiver<TimestampMs>,
        TimeRange,
        MetricsResolution,
    ),
    Waiting(
        Arc<dyn MetricsStore>,
        watch::Receiver<TimestampMs>,
        TimestampMs,
        MetricsResolution,
    ),
}

pub async fn host(
    State(state): State<AppState>,
    Query(query): Query<MetricsStreamQuery>,
) -> AppResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let (range, resolution) = query.parse()?;
    let metrics = state.metrics.clone();
    let rx = state
        .bucket_watcher
        .subscribe(MetricsSource::Host, resolution);

    let stream = stream::unfold(
        HostStreamState::Initial(metrics, rx, range, resolution),
        |current_state| async move {
            match current_state {
                HostStreamState::Initial(metrics, rx, range, resolution) => {
                    let points = metrics
                        .query_host(range, resolution)
                        .await
                        .unwrap_or_default();
                    let last_sent = points.last().map(|p| p.ts).unwrap_or(range.from);
                    let event = to_event(
                        Some("snapshot"),
                        &MetricsResponse {
                            resolution: resolution.as_str().to_string(),
                            data: points,
                        },
                    );
                    Some((
                        event,
                        HostStreamState::Waiting(metrics, rx, last_sent, resolution),
                    ))
                }
                HostStreamState::Waiting(metrics, mut rx, mut last_sent, resolution) => loop {
                    if rx.changed().await.is_err() {
                        return None;
                    }
                    let bucket_end = *rx.borrow_and_update();
                    if bucket_end <= last_sent {
                        continue;
                    }
                    let range = TimeRange {
                        from: last_sent,
                        to: bucket_end,
                    };
                    let points = metrics
                        .query_host(range, resolution)
                        .await
                        .unwrap_or_default();
                    let Some(point) = points.into_iter().next_back() else {
                        continue;
                    };
                    last_sent = point.ts;
                    let event = to_event(Some("append"), &point);
                    return Some((
                        event,
                        HostStreamState::Waiting(metrics, rx, last_sent, resolution),
                    ));
                },
            }
        },
    );

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

enum ContainersStreamState {
    Initial(
        Arc<dyn MetricsStore>,
        watch::Receiver<TimestampMs>,
        TimeRange,
        MetricsResolution,
    ),
    Waiting(
        Arc<dyn MetricsStore>,
        watch::Receiver<TimestampMs>,
        TimestampMs,
        MetricsResolution,
    ),
}

pub async fn containers(
    State(state): State<AppState>,
    Query(query): Query<MetricsStreamQuery>,
) -> AppResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let (range, resolution) = query.parse()?;
    let metrics = state.metrics.clone();
    let rx = state
        .bucket_watcher
        .subscribe(MetricsSource::Containers, resolution);

    let stream = stream::unfold(
        ContainersStreamState::Initial(metrics, rx, range, resolution),
        |current_state| async move {
            match current_state {
                ContainersStreamState::Initial(metrics, rx, range, resolution) => {
                    let by_service = metrics
                        .query_containers(range, resolution)
                        .await
                        .unwrap_or_default();
                    let series: HashMap<String, Vec<GroupPoint>> = by_service
                        .into_iter()
                        .map(|(service, group)| (service, group.sum))
                        .collect();
                    let last_sent = latest_ts_by_service(&series).unwrap_or(range.from);
                    let event = to_event(
                        Some("snapshot"),
                        &MetricsResponse {
                            resolution: resolution.as_str().to_string(),
                            data: series,
                        },
                    );
                    Some((
                        event,
                        ContainersStreamState::Waiting(metrics, rx, last_sent, resolution),
                    ))
                }
                ContainersStreamState::Waiting(metrics, mut rx, mut last_sent, resolution) => {
                    loop {
                        if rx.changed().await.is_err() {
                            return None;
                        }
                        let bucket_end = *rx.borrow_and_update();
                        if bucket_end <= last_sent {
                            continue;
                        }
                        let range = TimeRange {
                            from: last_sent,
                            to: bucket_end,
                        };
                        let by_service = metrics
                            .query_containers(range, resolution)
                            .await
                            .unwrap_or_default();
                        let series: HashMap<String, Vec<GroupPoint>> = by_service
                            .into_iter()
                            .map(|(service, group)| (service, group.sum))
                            .collect();
                        let Some(latest_ts) = latest_ts_by_service(&series) else {
                            continue;
                        };
                        let cross_section = cross_section_by_service(series);
                        last_sent = latest_ts;
                        let event = to_event(Some("append"), &cross_section);
                        return Some((
                            event,
                            ContainersStreamState::Waiting(metrics, rx, last_sent, resolution),
                        ));
                    }
                }
            }
        },
    );

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

enum ContainerStreamState {
    Initial(
        Arc<dyn MetricsStore>,
        watch::Receiver<TimestampMs>,
        String,
        TimeRange,
        MetricsResolution,
    ),
    Waiting(
        Arc<dyn MetricsStore>,
        watch::Receiver<TimestampMs>,
        String,
        TimestampMs,
        MetricsResolution,
    ),
}

pub async fn container(
    State(state): State<AppState>,
    Path(service): Path<String>,
    Query(query): Query<MetricsStreamQuery>,
) -> AppResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let (range, resolution) = query.parse()?;
    let metrics = state.metrics.clone();
    let rx = state
        .bucket_watcher
        .subscribe(MetricsSource::Containers, resolution);

    let stream = stream::unfold(
        ContainerStreamState::Initial(metrics, rx, service, range, resolution),
        |current_state| async move {
            match current_state {
                ContainerStreamState::Initial(metrics, rx, service, range, resolution) => {
                    let mut by_service = metrics
                        .query_containers(range, resolution)
                        .await
                        .unwrap_or_default();
                    let group_metrics = by_service.remove(&service).unwrap_or_default();
                    let last_sent = latest_ts_in_service(&group_metrics).unwrap_or(range.from);
                    let event = to_event(
                        Some("snapshot"),
                        &MetricsResponse {
                            resolution: resolution.as_str().to_string(),
                            data: group_metrics,
                        },
                    );
                    Some((
                        event,
                        ContainerStreamState::Waiting(metrics, rx, service, last_sent, resolution),
                    ))
                }
                ContainerStreamState::Waiting(
                    metrics,
                    mut rx,
                    service,
                    mut last_sent,
                    resolution,
                ) => loop {
                    if rx.changed().await.is_err() {
                        return None;
                    }
                    let bucket_end = *rx.borrow_and_update();
                    if bucket_end <= last_sent {
                        continue;
                    }
                    let range = TimeRange {
                        from: last_sent,
                        to: bucket_end,
                    };
                    let mut by_service = metrics
                        .query_containers(range, resolution)
                        .await
                        .unwrap_or_default();
                    let group_metrics = by_service.remove(&service).unwrap_or_default();
                    let Some(latest_ts) = latest_ts_in_service(&group_metrics) else {
                        continue;
                    };
                    let append = ContainerGroupMetricsAppend {
                        sum: group_metrics.sum.into_iter().next_back(),
                        containers: group_metrics
                            .containers
                            .into_iter()
                            .filter_map(|(cid, mut points)| points.pop().map(|point| (cid, point)))
                            .collect(),
                    };
                    last_sent = latest_ts;
                    let event = to_event(Some("append"), &append);
                    return Some((
                        event,
                        ContainerStreamState::Waiting(metrics, rx, service, last_sent, resolution),
                    ));
                },
            }
        },
    );

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn latest_ts_by_service(series: &HashMap<String, Vec<GroupPoint>>) -> Option<TimestampMs> {
    series
        .values()
        .filter_map(|points| points.last().map(|point| point.ts))
        .max()
}

fn cross_section_by_service(
    series: HashMap<String, Vec<GroupPoint>>,
) -> HashMap<String, GroupPoint> {
    series
        .into_iter()
        .filter_map(|(service, mut points)| points.pop().map(|point| (service, point)))
        .collect()
}

fn latest_ts_in_service(
    group_metrics: &crate::model::ContainerGroupMetrics,
) -> Option<TimestampMs> {
    let sum_ts = group_metrics.sum.last().map(|point| point.ts);
    let containers_ts = group_metrics
        .containers
        .values()
        .filter_map(|points| points.last().map(|point| point.ts))
        .max();
    sum_ts.into_iter().chain(containers_ts).max()
}

fn to_event<T: Serialize>(name: Option<&'static str>, payload: &T) -> Result<Event, Infallible> {
    let event = Event::default()
        .json_data(payload)
        .expect("stream payload always serializes");
    Ok(match name {
        Some(name) => event.event(name),
        None => event,
    })
}
