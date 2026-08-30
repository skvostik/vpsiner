use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::{self, Stream};
use serde::Serialize;
use tokio::sync::watch;

use crate::docker::DockerService;
use crate::metrics::snapshot::MetricsSnapshotState;
use crate::model::{ContainerDiff, ContainerSummary};
use crate::state::AppState;

/// host and container samples are recorded by independent tasks, so a debounce window collapses
/// the two revision bumps of one logical update cycle into a single emitted event.
const COALESCE_WINDOW: Duration = Duration::from_millis(100);

enum StreamState {
    Initial(Arc<MetricsSnapshotState>, watch::Receiver<u64>),
    Waiting(Arc<MetricsSnapshotState>, watch::Receiver<u64>),
}

pub async fn current(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let snapshot = state.snapshot.clone();
    let rx = snapshot.subscribe();
    let stream = stream::unfold(
        StreamState::Initial(snapshot, rx),
        |current_state| async move {
            match current_state {
                StreamState::Initial(snapshot, rx) => {
                    let event = to_event(None, &snapshot.current());
                    Some((event, StreamState::Waiting(snapshot, rx)))
                }
                StreamState::Waiting(snapshot, mut rx) => {
                    if rx.changed().await.is_err() {
                        return None;
                    }
                    tokio::time::sleep(COALESCE_WINDOW).await;
                    rx.borrow_and_update();
                    let event = to_event(None, &snapshot.current());
                    Some((event, StreamState::Waiting(snapshot, rx)))
                }
            }
        },
    );

    Sse::new(stream).keep_alive(KeepAlive::default())
}

enum ContainersStreamState {
    Initial(Arc<dyn DockerService>, watch::Receiver<u64>),
    Waiting(
        Arc<dyn DockerService>,
        watch::Receiver<u64>,
        HashMap<String, ContainerSummary>,
    ),
}

pub async fn containers(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let docker = state.docker.clone();
    let rx = docker.subscribe_containers_info();
    let stream = stream::unfold(
        ContainersStreamState::Initial(docker, rx),
        |current_state| async move {
            match current_state {
                ContainersStreamState::Initial(docker, rx) => {
                    let containers = docker.containers_info().unwrap_or_default();
                    let last_sent = index_by_id(&containers);
                    let event = to_event(Some("snapshot"), &containers);
                    Some((event, ContainersStreamState::Waiting(docker, rx, last_sent)))
                }
                ContainersStreamState::Waiting(docker, mut rx, mut last_sent) => {
                    loop {
                        if rx.changed().await.is_err() {
                            return None;
                        }
                        rx.borrow_and_update();
                        let containers = docker.containers_info().unwrap_or_default();
                        let diff = diff_containers(&last_sent, &containers);
                        if diff.added.is_empty()
                            && diff.updated.is_empty()
                            && diff.removed.is_empty()
                        {
                            // Nothing actually changed (e.g. a periodic refresh tick); skip the event.
                            continue;
                        }
                        last_sent = index_by_id(&containers);
                        let event = to_event(Some("diff"), &diff);
                        return Some((
                            event,
                            ContainersStreamState::Waiting(docker, rx, last_sent),
                        ));
                    }
                }
            }
        },
    );

    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn index_by_id(containers: &[ContainerSummary]) -> HashMap<String, ContainerSummary> {
    containers
        .iter()
        .map(|c| (c.id.clone(), c.clone()))
        .collect()
}

fn diff_containers(
    last_sent: &HashMap<String, ContainerSummary>,
    current: &[ContainerSummary],
) -> ContainerDiff {
    let mut diff = ContainerDiff::default();
    let mut seen = HashSet::with_capacity(current.len());

    for container in current {
        seen.insert(container.id.as_str());
        match last_sent.get(&container.id) {
            None => diff.added.push(container.clone()),
            Some(previous) if previous != container => diff.updated.push(container.clone()),
            Some(_) => {}
        }
    }

    for id in last_sent.keys() {
        if !seen.contains(id.as_str()) {
            diff.removed.push(id.clone());
        }
    }

    diff
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
