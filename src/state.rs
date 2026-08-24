use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use tokio::sync::RwLock;

use crate::config::Config;
use crate::docker::DockerService;
use crate::logs::store::LogStore;
use crate::metrics::host::HostMetricsSource;
use crate::metrics::store::MetricsStore;
use crate::model::ContainerSummary;

/// Live container registry, keyed by `log_group`.
pub type ContainerRegistry = HashMap<String, ContainerSummary>;

/// Injection point for every external dependency. Cloning is cheap — only `Arc`s are copied.
#[allow(dead_code)] // not every dependency has a consumer yet
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub docker: Arc<dyn DockerService>,
    pub metrics: Arc<dyn MetricsStore>,
    pub logs: Arc<dyn LogStore>,
    pub host: Arc<dyn HostMetricsSource>,
    pub containers: Arc<RwLock<ContainerRegistry>>,
    /// Updated at startup (and periodically in `Auto` mode) by `docker::run_write_probe`.
    pub docker_controls_available: Arc<AtomicBool>,
}

impl AppState {
    pub fn new(
        config: Config,
        docker: Arc<dyn DockerService>,
        metrics: Arc<dyn MetricsStore>,
        logs: Arc<dyn LogStore>,
        host: Arc<dyn HostMetricsSource>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            docker,
            metrics,
            logs,
            host,
            containers: Arc::new(RwLock::new(ContainerRegistry::new())),
            docker_controls_available: Arc::new(AtomicBool::new(false)),
        }
    }
}
