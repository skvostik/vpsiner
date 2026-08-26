use crate::docker::mapping::unbounded_receiver_stream;
use crate::docker::raw::{list_all_containers_details, list_running_containers};
use crate::error::{AppError, AppResult};
use crate::model::{ContainerSummary, container_log_id, container_short_id};
use bollard::Docker;
use futures_util::stream::BoxStream;
use std::time::Duration;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ObservedContainer {
    pub id: String,
    pub name: String,
    pub log_group: String,
}

impl ObservedContainer {
    pub fn short_id(&self) -> &str {
        container_short_id(&self.id)
    }
    pub fn log_id(&self) -> String {
        container_log_id(&self.log_group, &self.short_id())
    }
}

pub(super) trait ContainerRegistry: Send + Sync + 'static {
    /// Returns a list of all containers known to the registry in all states. The registry
    /// uses a best-effort approach to keep the list up-to-date while using minimum possible resources.
    /// This should not be relied upon for critical operations,
    /// but rather to display information in UI.
    fn containers_info(&self) -> AppResult<Vec<ContainerSummary>>;
    fn container_info(&self, id: &str) -> AppResult<Option<ContainerSummary>>;

    /// Returns a list of all observed containers in the registry. This
    /// list must contain up-to-date information for critical operations.
    fn observed_containers(&self) -> AppResult<Vec<ObservedContainer>>;
    fn observed(&self, id: &str) -> AppResult<Option<ObservedContainer>>;

    /// Sends stream of containers as they are added to the list of observed containers.
    /// Events about removals of observed containers are not sent through this stream.
    fn take_start_observing_stream(&self) -> AppResult<BoxStream<'static, ObservedContainer>>;
}

pub(super) struct BollardContainerRegistry {
    docker: Docker,
    containers_info: Arc<RwLock<Vec<ContainerSummary>>>,
    observed_containers: Arc<RwLock<HashMap<String, ObservedContainer>>>,
    start_observing_events_rx: Mutex<Option<mpsc::UnboundedReceiver<ObservedContainer>>>,
    start_observing_events_tx: mpsc::UnboundedSender<ObservedContainer>,
}

impl ContainerRegistry for BollardContainerRegistry {
    fn containers_info(&self) -> AppResult<Vec<ContainerSummary>> {
        Ok(self
            .containers_info
            .read()
            .map_err(|_| AppError::Docker("failed to acquire containers read lock".into()))?
            .clone())
    }

    fn container_info(&self, id: &str) -> AppResult<Option<ContainerSummary>> {
        Ok(self.containers_info()?.into_iter().find(|c| c.id == id))
    }

    fn observed_containers(&self) -> AppResult<Vec<ObservedContainer>> {
        Ok(self
            .observed_containers
            .read()
            .map_err(|_| {
                AppError::Docker("failed to acquire observed_containers read lock".into())
            })?
            .values()
            .cloned()
            .collect())
    }

    fn observed(&self, id: &str) -> AppResult<Option<ObservedContainer>> {
        Ok(self
            .observed_containers
            .read()
            .map_err(|_| AppError::Docker("failed to acquire containers read lock".into()))?
            .get(id)
            .cloned())
    }

    fn take_start_observing_stream(&self) -> AppResult<BoxStream<'static, ObservedContainer>> {
        let mut guard = self
            .start_observing_events_rx
            .lock()
            .map_err(|_| AppError::Docker("failed to acquire observing receiver lock".into()))?;
        match guard.take() {
            Some(rx) => Ok(unbounded_receiver_stream(rx)),
            None => Err(AppError::Docker("observing receiver already taken".into())),
        }
    }
}

impl BollardContainerRegistry {
    pub fn new(docker: Docker) -> Self {
        let (start_observing_events_tx, start_observing_events_rx) =
            mpsc::unbounded_channel::<ObservedContainer>();

        let containers_info = Arc::new(RwLock::new(Vec::new()));
        let observed_containers = Arc::new(RwLock::new(HashMap::new()));

        spawn_update_observed_containers_worker(
            docker.clone(),
            observed_containers.clone(),
            start_observing_events_tx.clone(),
            Duration::from_secs(30),
        );

        spawn_update_containers_info_worker(
            docker.clone(),
            containers_info.clone(),
            Duration::from_secs(60),
        );

        Self {
            docker,
            containers_info: containers_info,
            observed_containers: observed_containers,
            start_observing_events_rx: Mutex::new(Some(start_observing_events_rx)),
            start_observing_events_tx,
        }
    }

    async fn update_observed_containers(&self) -> AppResult<()> {
        update_observed_containers(
            &self.docker,
            &self.observed_containers,
            &self.start_observing_events_tx,
        )
        .await
    }

    async fn start_observing(&self, container_id: &str) -> AppResult<()> {
        match self.observed(container_id)? {
            Some(container) => {
                tracing::debug!(container = %&container.log_id(), "already observing");
                return Ok(());
            }
            None => {}
        }
        self.update_observed_containers().await?;
        Ok(())
    }

    async fn stop_observing(&self, container_id: &str) -> AppResult<()> {
        match self.observed(container_id)? {
            Some(_container) => {}
            None => {
                tracing::debug!(container = %container_short_id(container_id), "not observing");
                return Ok(());
            }
        }
        self.update_observed_containers().await?;
        Ok(())
    }

    async fn update_containers_info(&self) -> AppResult<()> {
        update_containers_info(&self.docker, &self.containers_info).await
    }
}

async fn update_containers_info(
    docker: &Docker,
    containers_info: &Arc<RwLock<Vec<ContainerSummary>>>,
) -> AppResult<()> {
    tracing::debug!("updating containers info");
    let containers = list_all_containers_details(docker).await?;
    let mut current = containers_info
        .write()
        .map_err(|err| AppError::Docker(format!("container_info registry lock poisoned: {err}")))?;
    *current = containers;
    Ok(())
}

async fn update_observed_containers(
    docker: &Docker,
    observed_containers: &Arc<RwLock<HashMap<String, ObservedContainer>>>,
    start_observing_events_tx: &mpsc::UnboundedSender<ObservedContainer>,
) -> AppResult<()> {
    tracing::debug!("updating observed containers");

    let mut started_observing = Vec::<ObservedContainer>::new();

    let running_containers = list_running_containers(docker)
        .await?
        .into_iter()
        .map(|c| (c.id.clone(), c))
        .collect::<HashMap<String, ObservedContainer>>();

    // observed containers lock scope
    {
        let mut observed_containers = observed_containers
            .write()
            .map_err(|err| AppError::Docker(format!("observed_containers lock poisoned: {err}")))?;

        observed_containers.retain(|id, observed_container| {
            match running_containers.contains_key(id) {
                true => true,
                false => {
                    tracing::info!(container = %observed_container.log_id(), "stop observing");
                    false
                }
            }
        });

        let containers_to_start_observing = running_containers
            .into_iter()
            .filter(|(cid, _)| !observed_containers.contains_key(cid))
            .collect::<Vec<_>>();

        for (cid, container) in containers_to_start_observing {
            tracing::info!(container = %container.log_id(), "start observing");
            observed_containers.insert(cid, container.clone());
            started_observing.push(container.clone());
        }
    }

    for container in started_observing {
        let container_log_id = container.log_id();
        if let Err(_) = start_observing_events_tx.send(container) {
            tracing::error!(container= %container_log_id, "failed to send start observing event");
        };
    }

    Ok(())
}

fn spawn_update_containers_info_worker(
    docker: Docker,
    containers_info: Arc<RwLock<Vec<ContainerSummary>>>,
    interval: Duration,
) {
    tracing::info!(sample_interval = ?interval, "starting update_containers_info worker");
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;

            if let Err(e) = update_containers_info(&docker, &containers_info).await {
                tracing::error!(error = %e, "failed to update containers info");
            }
        }
    });
}

fn spawn_update_observed_containers_worker(
    docker: Docker,
    observed_containers: Arc<RwLock<HashMap<String, ObservedContainer>>>,
    start_observing_events_tx: mpsc::UnboundedSender<ObservedContainer>,
    interval: Duration,
) {
    tracing::info!(sample_interval = ?interval, "starting update_observed_containers worker");
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;

            if let Err(e) = update_observed_containers(
                &docker,
                &observed_containers,
                &start_observing_events_tx,
            )
            .await
            {
                tracing::error!(error = %e, "failed to update containers info");
            }
        }
    });
}
