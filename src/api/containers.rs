use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use std::sync::atomic::Ordering;

use crate::error::{AppError, AppResult};
use crate::model::{ContainerState, ContainerSummary};
use crate::state::AppState;

enum ActionPlan {
    Noop,
    Start,
    Stop,
    Restart,
}

fn plan_for(state: ContainerState, action: &'static str) -> AppResult<ActionPlan> {
    match (state, action) {
        (ContainerState::Removing | ContainerState::Dead, _) => Err(AppError::Conflict(format!(
            "cannot {action} container while state is {state:?}"
        ))),
        (ContainerState::Restarting, "stop") => Ok(ActionPlan::Stop),
        (ContainerState::Restarting, _) => Err(AppError::Conflict(format!(
            "cannot {action} container while state is {state:?}"
        ))),
        (ContainerState::Running, "start") => Ok(ActionPlan::Noop),
        (ContainerState::Created | ContainerState::Exited, "stop") => Ok(ActionPlan::Noop),
        (ContainerState::Created | ContainerState::Exited | ContainerState::Paused, "start") => {
            Ok(ActionPlan::Start)
        }
        (ContainerState::Running | ContainerState::Paused, "stop") => Ok(ActionPlan::Stop),
        (ContainerState::Created | ContainerState::Exited, "restart") => Ok(ActionPlan::Start),
        (ContainerState::Running | ContainerState::Paused, "restart") => Ok(ActionPlan::Restart),
        _ => Err(AppError::BadRequest(format!(
            "unsupported action: {action}"
        ))),
    }
}

async fn run_action(state: &AppState, id: &str, action: &'static str) -> AppResult<StatusCode> {
    if !state.docker_controls_available.load(Ordering::Relaxed) {
        return Err(AppError::Forbidden(
            "container controls are disabled or unavailable on this backend".into(),
        ));
    }

    let current = state.docker.container_state(id).await?;
    match plan_for(current, action)? {
        ActionPlan::Noop => Ok(StatusCode::NO_CONTENT),
        ActionPlan::Start => {
            state.docker.start_container(id).await?;
            Ok(StatusCode::NO_CONTENT)
        }
        ActionPlan::Stop => {
            state.docker.stop_container(id).await?;
            Ok(StatusCode::NO_CONTENT)
        }
        ActionPlan::Restart => {
            state.docker.restart_container(id).await?;
            Ok(StatusCode::NO_CONTENT)
        }
    }
}

pub async fn list(State(state): State<AppState>) -> AppResult<Json<Vec<ContainerSummary>>> {
    Ok(Json(state.docker.list_containers().await?))
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
