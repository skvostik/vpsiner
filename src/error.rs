use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[allow(dead_code)] // not every variant has a producer yet
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("docker error: {0}")]
    Docker(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("host metrics error: {0}")]
    Host(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid request: {0}")]
    BadRequest(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("not implemented: {0}")]
    Unimplemented(&'static str),
}

pub type AppResult<T> = Result<T, AppError>;

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::Forbidden(_) => StatusCode::FORBIDDEN,
            AppError::Unimplemented(_) => StatusCode::NOT_IMPLEMENTED,
            AppError::Docker(_) => StatusCode::BAD_GATEWAY,
            AppError::Storage(_) | AppError::Host(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        if status.is_server_error() {
            tracing::error!("{}", self);
        }

        (
            status,
            Json(ErrorBody {
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}
