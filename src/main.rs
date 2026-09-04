mod api;
mod config;
mod db_version;
mod docker;
mod error;
mod logs;
mod metrics;
mod model;
mod retention;
mod state;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use tower_http::services::{ServeDir, ServeFile};

use crate::config::Config;
use crate::docker::BollardDocker;
use crate::logs::metadata::SqliteLogMetadataStore;
use crate::logs::store::SqliteLogStore;
use crate::metrics::host::SysinfoHost;
use crate::metrics::store::SqliteMetricsStore;
use crate::state::AppState;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime.enable_all();

    if let Some(worker_threads) = configured_worker_threads() {
        runtime.worker_threads(worker_threads);
    }

    runtime
        .build()
        .expect("failed to build Tokio runtime")
        .block_on(async_main());
}

fn configured_worker_threads() -> Option<usize> {
    let value = std::env::var("VPSINER_WORKER_THREADS").ok()?;
    let parsed = value.parse::<usize>().unwrap_or_else(|_| {
        panic!("VPSINER_WORKER_THREADS must be a positive integer, got: {value}")
    });

    assert!(
        parsed > 0,
        "VPSINER_WORKER_THREADS must be a positive integer, got: {value}"
    );

    Some(parsed)
}

async fn async_main() {
    let log_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(log_filter)
        .with_target(false)
        .without_time()
        .init();

    tracing::info!(pid = std::process::id(), "vpsiner process started");

    tracing::info!(
        num_workers = tokio::runtime::Handle::current().metrics().num_workers(),
        "tokio runtime worker-thread allocation"
    );

    let config = Config::from_env();
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));

    tracing::info!("starting vpsiner on http://{}", addr);
    tracing::info!(
        retention_weeks = config.retention_weeks,
        "configured data retention"
    );
    tracing::info!("config directory: {}", config.config_path.display());
    if let Some(static_dir) = &config.static_dir {
        tracing::info!("static assets directory: {}", static_dir.display());
    } else {
        tracing::info!("static file serving disabled");
    }

    if let Err(message) = db_version::ensure_compatible(&config.data_path).await {
        tracing::error!("{message}");
        std::process::exit(1);
    }

    // Composition root: concrete implementations are chosen here and nowhere else.
    let metadata = Arc::new(
        SqliteLogMetadataStore::connect(
            db_version::metadata_dir(&config.data_path).join("metadata.db"),
            config.sqlite_cache_size_kb,
            config.sqlite_busy_timeout,
        )
        .await
        .expect("failed to open metadata database"),
    );
    let metrics = Arc::new(
        SqliteMetricsStore::connect(
            db_version::metrics_dir(&config.data_path).join("metrics.db"),
            config.sqlite_cache_size_kb,
            config.sqlite_busy_timeout,
            config.downsample_max_gap_pct,
        )
        .await
        .expect("failed to open metrics database"),
    );
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
            config.docker_controls_mode,
            metadata.clone(),
            config.retention_weeks,
        )),
        metrics,
        Arc::new(SqliteLogStore::new(
            db_version::logs_dir(&config.data_path),
            config.sqlite_cache_size_kb,
            config.sqlite_busy_timeout,
            config.sqlite_keep_alive,
        )),
        metadata,
        Arc::new(SysinfoHost::default()),
    );

    tracing::info!(
        retention_weeks = config.retention_weeks,
        "retention cleanup worker started"
    );
    retention::cleanup_once(&state.metrics, &state.logs, config.retention_weeks).await;
    let retention_task = tokio::spawn(retention::run(
        state.metrics.clone(),
        state.logs.clone(),
        config.retention_weeks,
    ));

    let host_metrics_task = tokio::spawn(metrics::collector::run_host(
        state.host.clone(),
        state.metrics.clone(),
        state.logs.clone(),
        state.snapshot.clone(),
        state.bucket_watcher.clone(),
        config.collect_interval,
    ));
    let container_metrics_task = tokio::spawn(metrics::collector::run_containers(
        state.docker.clone(),
        state.metrics.clone(),
        state.snapshot.clone(),
        state.bucket_watcher.clone(),
        config.collect_interval,
    ));
    let log_ingestion_task = tokio::spawn(logs::run_ingestion(
        state.docker.clone(),
        state.logs.clone(),
        state.metadata.clone(),
        state.log_flush_watcher.clone(),
        config.log_flush_debounce,
        config.log_flush_keep_alive,
    ));

    let metadata_store = state.metadata.clone();
    let metrics_store = state.metrics.clone();
    let logs_store = state.logs.clone();
    let shutdown = state.shutdown.clone();
    let app = build_router(state, &config);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind TCP listener");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown))
        .await
        .expect("server failed to start");

    tracing::info!("stopping background workers");
    retention_task.abort();
    host_metrics_task.abort();
    container_metrics_task.abort();
    log_ingestion_task.abort();

    for (name, task) in [
        ("retention", retention_task),
        ("host metrics", host_metrics_task),
        ("container metrics", container_metrics_task),
        ("log ingestion", log_ingestion_task),
    ] {
        match task.await {
            Ok(_) => tracing::info!(task = name, "background worker stopped"),
            Err(err) if err.is_cancelled() => {
                tracing::debug!(task = name, "background worker cancelled")
            }
            Err(err) => {
                tracing::warn!(task = name, error = %err, "background worker exited with error")
            }
        }
    }

    close_with_timeout("logs store", logs_store.close()).await;
    close_with_timeout("metrics store", metrics_store.close()).await;
    close_with_timeout("metadata store", metadata_store.close()).await;
    tracing::info!("shutdown complete");
}

async fn close_with_timeout(name: &str, close_future: impl std::future::Future<Output = ()>) {
    const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
    match tokio::time::timeout(CLOSE_TIMEOUT, close_future).await {
        Ok(()) => tracing::info!(target = name, "store closed"),
        Err(_) => tracing::warn!(target = name, timeout = ?CLOSE_TIMEOUT, "store close timed out"),
    }
}

async fn shutdown_signal(shutdown: tokio_util::sync::CancellationToken) {
    let interrupt = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for interrupt signal");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to listen for terminate signal")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = interrupt => {}
        _ = terminate => {}
    }

    tracing::info!("shutdown signal received");
    // Wakes up every SSE stream so their connections close instead of blocking graceful shutdown.
    shutdown.cancel();
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
            config_path: "/tmp/vpsiner-test/config".into(),
            static_dir: None,
            port: 3000,
            retention_weeks: 12,
            collect_interval: std::time::Duration::from_secs(10),
            log_flush_debounce: std::time::Duration::from_millis(500),
            log_flush_keep_alive: std::time::Duration::from_secs(60),
            docker_controls_mode: crate::config::DockerControlsMode::Disabled,
            docker_probe_interval: std::time::Duration::from_secs(60),
            docker_retry_delay: std::time::Duration::from_secs(5),
            docker_request_concurrency: 8,
            docker_debounce: std::time::Duration::from_millis(1_000),
            log_channel_capacity: 10_000,
            samples_channel_capacity: 32,
            docker_events_channel_capacity: 256,
            sqlite_cache_size_kb: 1_024,
            sqlite_busy_timeout: std::time::Duration::from_secs(5),
            sqlite_keep_alive: std::time::Duration::from_secs(300),
            downsample_max_gap_pct: 40,
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
                service: "shop-web".into(),
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
        assert_eq!(containers[0].service, "shop-web");
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

    #[tokio::test]
    async fn configuration_reports_the_actual_tokio_worker_count() {
        let docker = MockDockerService::new();
        let (state, config) = state_with_docker(docker);
        let response = build_router(state, &config)
            .oneshot(
                Request::builder()
                    .uri("/api/config/computed")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let values: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            values[0]["value"],
            tokio::runtime::Handle::current()
                .metrics()
                .num_workers()
                .to_string()
        );
    }

    #[tokio::test]
    async fn ui_config_returns_default_when_file_does_not_exist() {
        let docker = MockDockerService::new();
        let (state, config) = state_with_docker(docker);
        let response = build_router(state, &config)
            .oneshot(
                Request::builder()
                    .uri("/api/config/ui")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let ui_config: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(ui_config["name"], "VPSiner");
        assert_eq!(ui_config["eyebrow"], "Simply Observed");
        let links = ui_config.get("links").and_then(|l| l.as_array()).unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0]["icon"], "Github");
        assert_eq!(links[0]["label"], "GitHub");
        assert_eq!(links[0]["url"], "https://github.com/skvostik/vpsiner");
    }

    #[tokio::test]
    async fn ui_config_serves_custom_file_when_present() {
        let temp_dir = std::env::temp_dir().join(format!("vpsiner-test-{}", std::process::id()));
        tokio::fs::create_dir_all(&temp_dir).await.unwrap();
        let ui_json_path = temp_dir.join("ui.json");
        let custom_json = r#"{"name":"Homelab","eyebrow":"Dashboard","links":[{"icon":"Server","label":"Custom Server","url":"https://example.com"}]}"#;
        tokio::fs::write(&ui_json_path, custom_json).await.unwrap();

        let mut config = test_config();
        config.config_path = temp_dir.clone();
        let state = AppState::new(
            config.clone(),
            Arc::new(MockDockerService::new()),
            Arc::new(MockMetricsStore::new()),
            Arc::new(MockLogStore::new()),
            Arc::new(MockLogMetadataStore::new()),
            Arc::new(MockHostMetricsSource::new()),
        );

        let response = build_router(state, &config)
            .oneshot(
                Request::builder()
                    .uri("/api/config/ui")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let ui_config: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(ui_config["name"], "Homelab");
        assert_eq!(ui_config["eyebrow"], "Dashboard");
        let links = ui_config.get("links").and_then(|l| l.as_array()).unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0]["icon"], "Server");
        assert_eq!(links[0]["label"], "Custom Server");
        assert_eq!(links[0]["url"], "https://example.com");

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}
