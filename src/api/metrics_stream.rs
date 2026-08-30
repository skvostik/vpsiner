//! SSE versions of the time-series metrics endpoints. Unlike `stream.rs`'s snapshot/diff
//! endpoints, these are parameterized per connection (`from`/`resolution`, and `log_group`
//! for the per-group endpoint) and driven by `BucketWatcher`, not a data-change signal: a new
//! point only exists once wall-clock time crosses the next completed bucket for that resolution.
//! There is no `to` — a live stream always runs up to the server's own "now" and keeps going.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::api::metrics::parse_resolution;
use crate::error::{AppError, AppResult};
use crate::metrics::store::MetricsStore;
use crate::model::{
    ContainerGroupMetricsAppend, GroupPoint, MetricsResolution, TimeRange, TimestampMs,
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
    pub resolution: String,
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
            parse_resolution(&self.resolution)?,
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
    let rx = state.bucket_watcher.subscribe(resolution);

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
                    let event = to_event(Some("snapshot"), &points);
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
    let rx = state.bucket_watcher.subscribe(resolution);

    let stream = stream::unfold(
        ContainersStreamState::Initial(metrics, rx, range, resolution),
        |current_state| async move {
            match current_state {
                ContainersStreamState::Initial(metrics, rx, range, resolution) => {
                    let series = metrics
                        .query_containers(range, resolution)
                        .await
                        .unwrap_or_default();
                    let last_sent = latest_ts_by_log_group(&series).unwrap_or(range.from);
                    let event = to_event(Some("snapshot"), &series);
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
                        let series = metrics
                            .query_containers(range, resolution)
                            .await
                            .unwrap_or_default();
                        let cross_section = cross_section_by_log_group(series);
                        if cross_section.is_empty() {
                            continue;
                        }
                        last_sent = bucket_end;
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
    Path(log_group): Path<String>,
    Query(query): Query<MetricsStreamQuery>,
) -> AppResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let (range, resolution) = query.parse()?;
    let metrics = state.metrics.clone();
    let rx = state.bucket_watcher.subscribe(resolution);

    let stream = stream::unfold(
        ContainerStreamState::Initial(metrics, rx, log_group, range, resolution),
        |current_state| async move {
            match current_state {
                ContainerStreamState::Initial(metrics, rx, log_group, range, resolution) => {
                    let group_metrics = metrics
                        .query_container(&log_group, range, resolution)
                        .await
                        .unwrap_or(crate::model::ContainerGroupMetrics {
                            sum: Vec::new(),
                            containers: HashMap::new(),
                        });
                    let last_sent = latest_ts_in_group(&group_metrics).unwrap_or(range.from);
                    let event = to_event(Some("snapshot"), &group_metrics);
                    Some((
                        event,
                        ContainerStreamState::Waiting(
                            metrics, rx, log_group, last_sent, resolution,
                        ),
                    ))
                }
                ContainerStreamState::Waiting(
                    metrics,
                    mut rx,
                    log_group,
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
                    let group_metrics = metrics
                        .query_container(&log_group, range, resolution)
                        .await
                        .unwrap_or(crate::model::ContainerGroupMetrics {
                            sum: Vec::new(),
                            containers: HashMap::new(),
                        });
                    let append = ContainerGroupMetricsAppend {
                        sum: group_metrics.sum.into_iter().next_back(),
                        containers: group_metrics
                            .containers
                            .into_iter()
                            .filter_map(|(cid, mut points)| points.pop().map(|point| (cid, point)))
                            .collect(),
                    };
                    if append.sum.is_none() && append.containers.is_empty() {
                        continue;
                    }
                    last_sent = bucket_end;
                    let event = to_event(Some("append"), &append);
                    return Some((
                        event,
                        ContainerStreamState::Waiting(
                            metrics, rx, log_group, last_sent, resolution,
                        ),
                    ));
                },
            }
        },
    );

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn latest_ts_by_log_group(series: &HashMap<String, Vec<GroupPoint>>) -> Option<TimestampMs> {
    series
        .values()
        .filter_map(|points| points.last().map(|point| point.ts))
        .max()
}

fn cross_section_by_log_group(
    series: HashMap<String, Vec<GroupPoint>>,
) -> HashMap<String, GroupPoint> {
    series
        .into_iter()
        .filter_map(|(log_group, mut points)| points.pop().map(|point| (log_group, point)))
        .collect()
}

fn latest_ts_in_group(group_metrics: &crate::model::ContainerGroupMetrics) -> Option<TimestampMs> {
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
