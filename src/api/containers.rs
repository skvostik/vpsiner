use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};

use crate::error::AppResult;
use crate::model::ContainerSummary;
use crate::state::AppState;

async fn run_action(state: &AppState, id: &str, action: &'static str) -> AppResult<StatusCode> {
    match action {
        "start" => {
            state.docker.start_container(id).await?;
            Ok(StatusCode::NO_CONTENT)
        }
        "stop" => {
            state.docker.stop_container(id).await?;
            Ok(StatusCode::NO_CONTENT)
        }
        "restart" => {
            state.docker.restart_container(id).await?;
            Ok(StatusCode::NO_CONTENT)
        }
        _ => unreachable!("container action routes pass known action names"),
    }
}

pub async fn list(State(state): State<AppState>) -> AppResult<Json<Vec<ContainerSummary>>> {
    Ok(Json(state.docker.containers()))
}

pub async fn start(State(state): State<AppState>, Path(id): Path<String>) -> AppResult<StatusCode> {
    run_action(&state, &id, "start").await
}

pub async fn stop(State(state): State<AppState>, Path(id): Path<String>) -> AppResult<StatusCode> {
    run_action(&state, &id, "stop").await
}

pub async fn restart(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> AppResult<StatusCode> {
    run_action(&state, &id, "restart").await
}
