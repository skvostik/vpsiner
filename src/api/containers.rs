use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};

use crate::error::{AppError, AppResult};
use crate::model::{container_id::ContainerId, containers::ContainerSummary};
use crate::state::AppState;

async fn run_action(state: &AppState, id: &str, action: &'static str) -> AppResult<StatusCode> {
    let id = ContainerId::parse(id)
        .ok_or_else(|| AppError::BadRequest("invalid container id".into()))?;
    match action {
        "start" => {
            state.docker.start_container(id).await?;
        }
        "stop" => {
            state.docker.stop_container(id).await?;
        }
        "restart" => {
            state.docker.restart_container(id).await?;
        }
        _ => unreachable!("container action routes pass known action names"),
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn list(State(state): State<AppState>) -> AppResult<Json<Vec<ContainerSummary>>> {
    Ok(Json(state.docker.containers_info()?))
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
