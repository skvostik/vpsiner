use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::{self, Stream};
use tokio::sync::watch;

use crate::metrics::snapshot::MetricsSnapshotState;
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
                    let event = to_event(&snapshot);
                    Some((event, StreamState::Waiting(snapshot, rx)))
                }
                StreamState::Waiting(snapshot, mut rx) => {
                    if rx.changed().await.is_err() {
                        return None;
                    }
                    tokio::time::sleep(COALESCE_WINDOW).await;
                    rx.borrow_and_update();
                    let event = to_event(&snapshot);
                    Some((event, StreamState::Waiting(snapshot, rx)))
                }
            }
        },
    );

    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn to_event(snapshot: &MetricsSnapshotState) -> Result<Event, Infallible> {
    Ok(Event::default()
        .json_data(snapshot.current())
        .expect("MetricsSnapshot always serializes"))
}
