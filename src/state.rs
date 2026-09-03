use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::docker::DockerService;
use crate::logs::flush_watcher::LogFlushWatcher;
use crate::logs::metadata::LogMetadataStore;
use crate::logs::store::LogStore;
use crate::metrics::bucket_watcher::BucketWatcher;
use crate::metrics::host::HostMetricsSource;
use crate::metrics::snapshot::MetricsSnapshotState;
use crate::metrics::store::MetricsStore;

/// Injection point for every external dependency. Cloning is cheap — only `Arc`s are copied.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub docker: Arc<dyn DockerService>,
    pub metrics: Arc<dyn MetricsStore>,
    pub logs: Arc<dyn LogStore>,
    pub metadata: Arc<dyn LogMetadataStore>,
    pub host: Arc<dyn HostMetricsSource>,
    pub snapshot: Arc<MetricsSnapshotState>,
    pub bucket_watcher: Arc<BucketWatcher>,
    pub log_flush_watcher: Arc<LogFlushWatcher>,
    /// Cancelled on shutdown so SSE streams stop holding their connection open.
    pub shutdown: CancellationToken,
}

impl AppState {
    pub fn new(
        config: Config,
        docker: Arc<dyn DockerService>,
        metrics: Arc<dyn MetricsStore>,
        logs: Arc<dyn LogStore>,
        metadata: Arc<dyn LogMetadataStore>,
        host: Arc<dyn HostMetricsSource>,
    ) -> Self {
        let snapshot = Arc::new(MetricsSnapshotState::new(config.collect_interval));
        let bucket_watcher = Arc::new(BucketWatcher::new());
        let log_flush_watcher = Arc::new(LogFlushWatcher::new());
        Self {
            config: Arc::new(config),
            docker,
            metrics,
            logs,
            metadata,
            host,
            snapshot,
            bucket_watcher,
            log_flush_watcher,
            shutdown: CancellationToken::new(),
        }
    }
}
