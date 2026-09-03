use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::{self, Stream};
use serde::Serialize;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::docker::DockerService;
use crate::metrics::snapshot::MetricsSnapshotState;
use crate::model::{ContainerDiff, ContainerSummary};
use crate::state::AppState;

enum StreamState {
    Initial(
        Arc<MetricsSnapshotState>,
        watch::Receiver<u64>,
        watch::Receiver<u64>,
        CancellationToken,
    ),
    Waiting(
        Arc<MetricsSnapshotState>,
        watch::Receiver<u64>,
        watch::Receiver<u64>,
        CancellationToken,
    ),
}

pub async fn current(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let snapshot = state.snapshot.clone();
    let host_rx = snapshot.subscribe_host();
    let containers_rx = snapshot.subscribe_containers();
    let shutdown = state.shutdown.clone();
    let stream = stream::unfold(
        StreamState::Initial(snapshot, host_rx, containers_rx, shutdown),
        |current_state| async move {
            match current_state {
                StreamState::Initial(snapshot, host_rx, containers_rx, shutdown) => {
                    let event = to_event(Some("snapshot"), &snapshot.current());
                    Some((
                        event,
                        StreamState::Waiting(snapshot, host_rx, containers_rx, shutdown),
                    ))
                }
                StreamState::Waiting(snapshot, mut host_rx, mut containers_rx, shutdown) => {
                    let event = tokio::select! {
                        _ = shutdown.cancelled() => return None,
                        changed = host_rx.changed() => {
                            changed.ok()?;
                            to_event(Some("host"), &snapshot.current_host())
                        }
                        changed = containers_rx.changed() => {
                            changed.ok()?;
                            to_event(Some("containers"), &snapshot.current_containers())
                        }
                    };
                    Some((
                        event,
                        StreamState::Waiting(snapshot, host_rx, containers_rx, shutdown),
                    ))
                }
            }
        },
    );

    Sse::new(stream).keep_alive(KeepAlive::default())
}

enum ContainersStreamState {
    Initial(
        Arc<dyn DockerService>,
        watch::Receiver<u64>,
        CancellationToken,
    ),
    Waiting(
        Arc<dyn DockerService>,
        watch::Receiver<u64>,
        HashMap<String, ContainerSummary>,
        CancellationToken,
    ),
}

pub async fn containers(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let docker = state.docker.clone();
    let rx = docker.subscribe_containers_info();
    let shutdown = state.shutdown.clone();
    let stream = stream::unfold(
        ContainersStreamState::Initial(docker, rx, shutdown),
        |current_state| async move {
            match current_state {
                ContainersStreamState::Initial(docker, rx, shutdown) => {
                    let containers = docker.containers_info().unwrap_or_default();
                    let last_sent = index_by_id(&containers);
                    let event = to_event(Some("snapshot"), &containers);
                    Some((
                        event,
                        ContainersStreamState::Waiting(docker, rx, last_sent, shutdown),
                    ))
                }
                ContainersStreamState::Waiting(docker, mut rx, mut last_sent, shutdown) => {
                    loop {
                        tokio::select! {
                            _ = shutdown.cancelled() => return None,
                            changed = rx.changed() => {
                                if changed.is_err() {
                                    return None;
                                }
                            }
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
                            ContainersStreamState::Waiting(docker, rx, last_sent, shutdown),
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
