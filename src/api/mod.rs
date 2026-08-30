pub mod containers;
pub mod logs;
pub mod metrics;
pub mod metrics_stream;
pub mod stream;

use axum::{Json, Router, extract::State, routing::get, routing::post};
use serde::Serialize;

use crate::state::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub service: &'static str,
    pub version: &'static str,
    pub port: u16,
    pub sample_interval_ms: u64,
    pub retention_weeks: u32,
    pub docker_controls_available: bool,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/containers", get(containers::list))
        .route("/containers/{id}/start", post(containers::start))
        .route("/containers/{id}/stop", post(containers::stop))
        .route("/containers/{id}/restart", post(containers::restart))
        .route("/metrics/host", get(metrics::host))
        .route("/metrics/current", get(metrics::current))
        .route("/metrics/containers/{log_group}", get(metrics::container))
        .route("/metrics/containers", get(metrics::containers_history))
        .route("/logs", get(logs::list_groups))
        .route("/logs/{log_group}", get(logs::query))
        .nest(
            "/stream",
            Router::new()
                .route("/metrics/current", get(stream::current))
                .route("/metrics/host", get(metrics_stream::host))
                .route("/metrics/containers", get(metrics_stream::containers))
                .route(
                    "/metrics/containers/{log_group}",
                    get(metrics_stream::container),
                )
                .route("/containers", get(stream::containers)),
        )
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        service: "vpsiner",
        version: env!("CARGO_PKG_VERSION"),
        port: state.config.port,
        sample_interval_ms: state.config.collect_interval.as_millis() as u64,
        retention_weeks: state.config.retention_weeks,
        docker_controls_available: state.docker.controls_available(),
    })
}
