use std::sync::Arc;

use crate::config::Config;
use crate::docker::DockerService;
use crate::logs::metadata::LogMetadataStore;
use crate::logs::store::LogStore;
use crate::metrics::host::HostMetricsSource;
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
        Self {
            config: Arc::new(config),
            docker,
            metrics,
            logs,
            metadata,
            host,
        }
    }
}
