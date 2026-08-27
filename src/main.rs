mod api;
mod config;
mod docker;
mod error;
mod logs;
mod metrics;
mod model;
mod retention;
mod state;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tower_http::services::{ServeDir, ServeFile};

use crate::config::Config;
use crate::docker::BollardDocker;
use crate::logs::metadata::SqliteLogMetadataStore;
use crate::logs::store::SqliteLogStore;
use crate::metrics::host::SysinfoHost;
use crate::metrics::store::SqliteMetricsStore;
use crate::state::AppState;

#[tokio::main]
async fn main() {
    let log_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(log_filter)
        .with_target(false)
        .init();

    let config = Config::from_env();
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));

    tracing::info!("starting vpsiner on http://{}", addr);
    tracing::info!(
        retention_weeks = config.retention_weeks,
        "configured data retention"
    );
    if let Some(static_dir) = &config.static_dir {
        tracing::info!("static assets directory: {}", static_dir.display());
    } else {
        tracing::info!("static file serving disabled");
    }

    // Composition root: concrete implementations are chosen here and nowhere else.
    let metadata = Arc::new(SqliteLogMetadataStore::new(
        config.data_path.join("metadata.db"),
    ));
    let state = AppState::new(
        config.clone(),
        Arc::new(BollardDocker::new(
            &config.docker_host,
            config.docker_timeout_secs,
            config.docker_request_timeout_secs,
            config.collect_interval,
            config.docker_probe_interval,
            config.docker_request_concurrency,
            config.docker_retry_delay,
            config.log_channel_capacity,
            config.samples_channel_capacity,
            config.docker_events_channel_capacity,
            config.docker_debounce,
            metadata.clone(),
            config.retention_weeks,
        )),
        Arc::new(SqliteMetricsStore::new(config.data_path.join("metrics.db"))),
        Arc::new(SqliteLogStore::new(config.data_path.join("logs"))),
        metadata,
        Arc::new(SysinfoHost::default()),
    );

    tracing::info!(
        retention_weeks = config.retention_weeks,
        "retention cleanup worker started"
    );
    retention::cleanup_once(&state.metrics, &state.logs, config.retention_weeks).await;
    tokio::spawn(retention::run(
        state.metrics.clone(),
        state.logs.clone(),
        config.retention_weeks,
    ));

    tokio::spawn(metrics::collector::run(
        state.host.clone(),
        state.metrics.clone(),
        state.logs.clone(),
        config.collect_interval,
    ));
    tokio::spawn(metrics::collector::run_containers(
        state.docker.clone(),
        state.metrics.clone(),
        config.collect_interval,
    ));
    tokio::spawn(logs::run_ingestion(
        state.docker.clone(),
        state.logs.clone(),
        state.metadata.clone(),
        config.log_flush_debounce,
    ));

    let app = build_router(state, &config);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind TCP listener");

    axum::serve(listener, app)
        .await
        .expect("server failed to start");
}

fn build_router(state: AppState, config: &Config) -> Router {
    let router = Router::new().nest("/api", api::router());

    if let Some(static_dir) = &config.static_dir {
        // Unmatched paths fall back to index.html so the Vue router can handle deep links.
        let spa = ServeDir::new(static_dir)
            .append_index_html_on_directories(true)
            .fallback(ServeFile::new(static_dir.join("index.html")));
        router.fallback_service(spa).with_state(state)
    } else {
        router.with_state(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::docker::MockDockerService;
    use crate::error::AppError;
    use crate::logs::metadata::MockLogMetadataStore;
    use crate::logs::store::MockLogStore;
    use crate::metrics::host::MockHostMetricsSource;
    use crate::metrics::store::MockMetricsStore;
    use crate::model::{ContainerState, ContainerSummary};

    fn test_config() -> Config {
        Config {
            docker_host: "tcp://127.0.0.1:2375".into(),
            docker_timeout_secs: 60,
            docker_request_timeout_secs: 5,
            data_path: "/tmp/vpsiner-test".into(),
            static_dir: None,
            port: 3000,
            retention_weeks: 12,
            collect_interval: std::time::Duration::from_secs(10),
            log_flush_debounce: std::time::Duration::from_millis(500),
            docker_controls_mode: crate::config::DockerControlsMode::Disabled,
            docker_probe_interval: std::time::Duration::from_secs(60),
            docker_retry_delay: std::time::Duration::from_secs(5),
            docker_request_concurrency: 8,
            docker_debounce: std::time::Duration::from_millis(1_000),
            log_channel_capacity: 10_000,
            samples_channel_capacity: 32,
            docker_events_channel_capacity: 256,
        }
    }

    fn state_with_docker(docker: MockDockerService) -> (AppState, Config) {
        let config = test_config();
        let state = AppState::new(
            config.clone(),
            Arc::new(docker),
            Arc::new(MockMetricsStore::new()),
            Arc::new(MockLogStore::new()),
            Arc::new(MockLogMetadataStore::new()),
            Arc::new(MockHostMetricsSource::new()),
        );
        (state, config)
    }

    #[tokio::test]
    async fn lists_containers_from_the_injected_docker_service() {
        let mut docker = MockDockerService::new();
        docker.expect_containers_info().times(1).returning(|| {
            Ok(vec![ContainerSummary {
                id: "abc123".into(),
                name: "web".into(),
                log_group: "shop-web".into(),
                image: "nginx:latest".into(),
                image_sha: String::new(),
                ports: Vec::new(),
                labels: Vec::new(),
                state: Some(ContainerState::Running),
                started_at: Some(1_700_000_000_000),
            }])
        });

        let (state, config) = state_with_docker(docker);
        let response = build_router(state, &config)
            .oneshot(
                Request::builder()
                    .uri("/api/containers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let containers: Vec<ContainerSummary> = serde_json::from_slice(&body).unwrap();
        assert_eq!(containers[0].log_group, "shop-web");
    }

    #[tokio::test]
    async fn maps_docker_failures_to_bad_gateway() {
        let mut docker = MockDockerService::new();
        docker
            .expect_start_container()
            .withf(|id| id == "abc123")
            .returning(|_| Err(AppError::Docker("socket unreachable".into())));

        let (state, config) = state_with_docker(docker);
        let response = build_router(state, &config)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/containers/abc123/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn rejects_container_actions_when_controls_are_unavailable() {
        let mut docker = MockDockerService::new();
        docker
            .expect_start_container()
            .withf(|id| id == "abc123")
            .returning(|_| {
                Err(AppError::Forbidden(
                    "container controls are disabled or unavailable on this backend".into(),
                ))
            });
        let (state, config) = state_with_docker(docker);

        let response = build_router(state, &config)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/containers/abc123/start")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn health_reports_the_backend_version() {
        let mut docker = MockDockerService::new();
        docker.expect_controls_available().returning(|| false);
        let (state, config) = state_with_docker(docker);
        let response = build_router(state, &config)
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let health: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(health["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(health["retention_weeks"], 12);
    }
}
