//! SSE versions of the logs endpoints: a diff-based groups list, and filter-aware forward
//! tailing for a single service. Both react to `LogFlushWatcher` instead of polling; the
//! groups list also reacts to `DockerService::subscribe_containers_info()` (slice 2) since a
//! group's `live` flag depends on container state, not log flushes.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::api::logs::{merge_services, parse_levels, parse_streams};
use crate::error::AppResult;
use crate::logs::store::LogStore;
use crate::model::{LogFilter, LogTailAppend, ServiceDiff, ServiceStatus};
use crate::state::AppState;

enum GroupsStreamState {
    Initial(AppState, watch::Receiver<u64>, watch::Receiver<u64>),
    Waiting(
        AppState,
        watch::Receiver<u64>,
        watch::Receiver<u64>,
        BTreeMap<String, ServiceStatus>,
    ),
}

pub async fn groups(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let flush_rx = state.log_flush_watcher.subscribe_any();
    let containers_rx = state.docker.subscribe_containers_info();

    let stream = stream::unfold(
        GroupsStreamState::Initial(state, flush_rx, containers_rx),
        |current_state| async move {
            match current_state {
                GroupsStreamState::Initial(state, flush_rx, containers_rx) => {
                    let current = current_groups(&state).await;
                    let event = to_event(Some("snapshot"), &current);
                    Some((
                        event,
                        GroupsStreamState::Waiting(state, flush_rx, containers_rx, current),
                    ))
                }
                GroupsStreamState::Waiting(
                    state,
                    mut flush_rx,
                    mut containers_rx,
                    mut last_sent,
                ) => {
                    loop {
                        let changed = tokio::select! {
                            result = flush_rx.changed() => result,
                            result = containers_rx.changed() => result,
                        };
                        if changed.is_err() {
                            return None;
                        }
                        let current = current_groups(&state).await;
                        let diff = diff_services(&last_sent, &current);
                        if diff.added.is_empty()
                            && diff.updated.is_empty()
                            && diff.removed.is_empty()
                        {
                            // Nothing actually changed (e.g. a periodic refresh tick); skip the event.
                            continue;
                        }
                        last_sent = current;
                        let event = to_event(Some("diff"), &diff);
                        return Some((
                            event,
                            GroupsStreamState::Waiting(state, flush_rx, containers_rx, last_sent),
                        ));
                    }
                }
            }
        },
    );

    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn current_groups(state: &AppState) -> BTreeMap<String, ServiceStatus> {
    let stored = state
        .metadata
        .list_last_received()
        .await
        .unwrap_or_default();
    let containers = state.docker.containers_info().unwrap_or_default();
    merge_services(stored, containers)
}

fn diff_services(
    last_sent: &BTreeMap<String, ServiceStatus>,
    current: &BTreeMap<String, ServiceStatus>,
) -> ServiceDiff {
    let mut diff = ServiceDiff::default();

    for (service, status) in current {
        match last_sent.get(service) {
            None => {
                diff.added.insert(service.clone(), status.clone());
            }
            Some(previous) if previous != status => {
                diff.updated.insert(service.clone(), status.clone());
            }
            Some(_) => {}
        }
    }

    for service in last_sent.keys() {
        if !current.contains_key(service) {
            diff.removed.push(service.clone());
        }
    }

    diff
}

#[derive(Debug, Deserialize)]
pub struct LogTailQuery {
    pub q: Option<String>,
    pub level: Option<String>,
    pub stream: Option<String>,
    pub after: Option<String>,
}

impl LogTailQuery {
    fn filter(self) -> AppResult<LogFilter> {
        Ok(LogFilter {
            from: None,
            to: None,
            query: self.q,
            levels: parse_levels(self.level)?,
            streams: parse_streams(self.stream)?,
            limit: None,
            before: None,
            after: self.after,
        })
    }
}

struct TailState {
    logs: Arc<dyn LogStore>,
    rx: watch::Receiver<u64>,
    service: String,
    filter: LogFilter,
    /// Skips waiting for a flush notification once, so lines flushed between the client's last
    /// REST page load and this connection opening aren't missed.
    first: bool,
}

pub async fn tail(
    State(state): State<AppState>,
    Path(service): Path<String>,
    Query(query): Query<LogTailQuery>,
) -> AppResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let filter = query.filter()?;
    let rx = state.log_flush_watcher.subscribe(&service);
    let initial_state = TailState {
        logs: state.logs.clone(),
        rx,
        service,
        filter,
        first: true,
    };

    let stream = stream::unfold(initial_state, |mut state| async move {
        loop {
            if state.first {
                state.first = false;
            } else if state.rx.changed().await.is_err() {
                return None;
            } else {
                state.rx.borrow_and_update();
            }

            let page = match state.logs.query(&state.service, state.filter.clone()).await {
                Ok(page) => page,
                Err(_) => continue,
            };
            if page.items.is_empty() {
                continue;
            }
            state.filter.after = page.newer_cursor.clone();
            let append = LogTailAppend {
                items: page.items,
                newer_cursor: page.newer_cursor,
            };
            let event = to_event(Some("append"), &append);
            return Some((event, state));
        }
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
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
