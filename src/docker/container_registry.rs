use crate::docker::mapping::receiver_stream;
use crate::docker::raw::{list_all_containers_details, list_running_containers};
use crate::error::{AppError, AppResult};
use crate::model::{ContainerSummary, container_log_id, container_short_id};
use bollard::{Docker, query_parameters::EventsOptionsBuilder};
use futures_util::{StreamExt, stream::BoxStream};
use std::time::Duration;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock, Weak},
};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ObservedContainer {
    pub id: String,
    pub name: String,
    pub log_group: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ContainerObserveAction {
    Start,
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ContainerObserveEvent {
    pub action: ContainerObserveAction,
    pub container: ObservedContainer,
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

    /// Sends stream of containers as they are added or removed to the list of observed containers.
    fn take_observe_events_stream(&self) -> AppResult<BoxStream<'static, ContainerObserveEvent>>;
}

pub(super) struct BollardContainerRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    docker: Docker,
    request_concurrency: usize,
    request_timeout: Duration,
    containers_info: Arc<RwLock<Vec<ContainerSummary>>>,
    observed_containers: Arc<RwLock<HashMap<String, ObservedContainer>>>,
    observe_events_rx: Mutex<Option<mpsc::Receiver<ContainerObserveEvent>>>,
    observe_events_tx: mpsc::Sender<ContainerObserveEvent>,
    observed_update_tx: mpsc::Sender<()>,
    containers_info_update_tx: mpsc::Sender<()>,
}

impl ContainerRegistry for BollardContainerRegistry {
    fn containers_info(&self) -> AppResult<Vec<ContainerSummary>> {
        self.inner.containers_info()
    }

    fn container_info(&self, id: &str) -> AppResult<Option<ContainerSummary>> {
        self.inner.container_info(id)
    }

    fn observed_containers(&self) -> AppResult<Vec<ObservedContainer>> {
        self.inner.observed_containers()
    }

    fn take_observe_events_stream(&self) -> AppResult<BoxStream<'static, ContainerObserveEvent>> {
        self.inner.take_observe_events_stream()
    }
}

impl Inner {
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

    fn take_observe_events_stream(&self) -> AppResult<BoxStream<'static, ContainerObserveEvent>> {
        let mut guard = self
            .observe_events_rx
            .lock()
            .map_err(|_| AppError::Docker("failed to acquire observing receiver lock".into()))?;
        match guard.take() {
            Some(rx) => Ok(receiver_stream(rx)),
            None => Err(AppError::Docker("observing receiver already taken".into())),
        }
    }
}

impl BollardContainerRegistry {
    pub fn new(
        docker: Docker,
        request_concurrency: usize,
        request_timeout: Duration,
        probe_interval: Duration,
        retry_delay: Duration,
        events_channel_capacity: usize,
        debounce: Duration,
    ) -> Self {
        let (observe_events_tx, observe_events_rx) =
            mpsc::channel::<ContainerObserveEvent>(events_channel_capacity);
        let (observed_update_tx, observed_update_rx) = mpsc::channel::<()>(1);
        let (containers_info_update_tx, containers_info_update_rx) = mpsc::channel::<()>(1);

        let containers_info = Arc::new(RwLock::new(Vec::new()));
        let observed_containers = Arc::new(RwLock::new(HashMap::new()));

        let inner = Arc::new(Inner {
            docker,
            request_concurrency,
            request_timeout,
            containers_info,
            observed_containers,
            observe_events_rx: Mutex::new(Some(observe_events_rx)),
            observe_events_tx,
            observed_update_tx,
            containers_info_update_tx,
        });

        spawn_containers_observer(Arc::downgrade(&inner), probe_interval, retry_delay);
        spawn_update_containers_info_worker(Arc::downgrade(&inner), probe_interval);
        spawn_observed_update_worker(Arc::downgrade(&inner), observed_update_rx, debounce);
        spawn_containers_info_update_worker(
            Arc::downgrade(&inner),
            containers_info_update_rx,
            debounce,
        );

        Self { inner }
    }
}

impl Inner {
    fn request_observed_update(&self) -> AppResult<()> {
        if let Err(error) = self.observed_update_tx.try_send(()) {
            if !matches!(error, mpsc::error::TrySendError::Full(_)) {
                return Err(AppError::Docker(format!(
                    "failed to schedule observed containers update: {error}"
                )));
            }
        }
        Ok(())
    }

    async fn update_observed_containers_now(&self) -> AppResult<()> {
        tracing::debug!("updating observed containers");

        let mut started_observing = Vec::<ObservedContainer>::new();
        let mut stopped_observing = Vec::<ObservedContainer>::new();

        let running_containers = list_running_containers(&self.docker, self.request_timeout)
            .await?
            .into_iter()
            .map(|c| (c.id.clone(), c))
            .collect::<HashMap<String, ObservedContainer>>();

        // observed containers lock scope
        {
            let mut observed_containers = self.observed_containers.write().map_err(|err| {
                AppError::Docker(format!("observed_containers lock poisoned: {err}"))
            })?;

            observed_containers.retain(|id, observed_container| {
                match running_containers.contains_key(id) {
                    true => true,
                    false => {
                        tracing::info!(container = %observed_container.log_id(), "stop observing");
                        stopped_observing.push(observed_container.clone());
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
            if let Err(err) = self.observe_events_tx.try_send(ContainerObserveEvent {
                container,
                action: ContainerObserveAction::Start,
            }) {
                tracing::warn!(container = %container_log_id, error = %err, "failed to send start observing event");
            };
        }

        for container in stopped_observing {
            let container_log_id = container.log_id();
            if let Err(err) = self.observe_events_tx.try_send(ContainerObserveEvent {
                container,
                action: ContainerObserveAction::Stop,
            }) {
                tracing::warn!(container = %container_log_id, error = %err, "failed to send stop observing event");
            };
        }

        // Request an update to the containers info after processing observed containers
        self.request_containers_info_update()?;

        Ok(())
    }

    fn request_containers_info_update(&self) -> AppResult<()> {
        if let Err(error) = self.containers_info_update_tx.try_send(()) {
            if !matches!(error, mpsc::error::TrySendError::Full(_)) {
                return Err(AppError::Docker(format!(
                    "failed to schedule containers info update: {error}"
                )));
            }
        }
        Ok(())
    }

    async fn update_containers_info_now(&self) -> AppResult<()> {
        tracing::debug!("updating containers info");
        let containers = list_all_containers_details(
            &self.docker,
            self.request_concurrency,
            self.request_timeout,
        )
        .await?;
        let mut current = self.containers_info.write().map_err(|err| {
            AppError::Docker(format!("container_info registry lock poisoned: {err}"))
        })?;
        *current = containers;
        Ok(())
    }
}

fn spawn_update_containers_info_worker(registry: Weak<Inner>, interval: Duration) {
    tracing::info!(sample_interval = ?interval, "starting update_containers_info worker");
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            let Some(registry) = registry.upgrade() else {
                tracing::debug!(
                    "stopping update_containers_info worker because registry was dropped"
                );
                return;
            };

            ticker.tick().await;

            if let Err(e) = registry.request_containers_info_update() {
                tracing::error!(error = %e, "failed to schedule containers info update");
            }
        }
    });
}

fn spawn_containers_info_update_worker(
    registry: Weak<Inner>,
    mut requests: mpsc::Receiver<()>,
    debounce: Duration,
) {
    tokio::spawn(async move {
        while requests.recv().await.is_some() {
            tokio::time::sleep(debounce).await;

            while requests.try_recv().is_ok() {}

            let Some(registry_ref) = registry.upgrade() else {
                tracing::debug!(
                    "stopping containers info update worker because registry was dropped"
                );
                return;
            };

            if let Err(error) = registry_ref.update_containers_info_now().await {
                tracing::warn!(error = %error, "failed to update containers info");
            }
        }

        tracing::debug!("stopping containers info update worker because request channel closed");
    });
}

fn spawn_observed_update_worker(
    registry: Weak<Inner>,
    mut requests: mpsc::Receiver<()>,
    debounce: Duration,
) {
    tokio::spawn(async move {
        while requests.recv().await.is_some() {
            tokio::time::sleep(debounce).await;

            // Drain any additional requests that arrived while we were sleeping.
            //They will be coalesced into the next update.
            while requests.try_recv().is_ok() {}

            let Some(registry_ref) = registry.upgrade() else {
                tracing::debug!(
                    "stopping observed containers update worker because registry was dropped"
                );
                return;
            };

            if let Err(error) = registry_ref.update_observed_containers_now().await {
                tracing::warn!(error = %error, "failed to update observed containers");
            }

            // Keep a request that arrived while the Docker update was running. It will trigger
            // another coalesced refresh after the current one completes.
        }

        tracing::debug!(
            "stopping observed containers update worker because request channel closed"
        );
    });
}

fn spawn_containers_observer(registry: Weak<Inner>, interval: Duration, retry_delay: Duration) {
    tracing::info!(sample_interval = ?interval, "starting containers observer");
    tokio::spawn(async move {
        loop {
            let Some(_) = registry.upgrade() else {
                tracing::debug!("stopping containers observer because registry was dropped");
                return;
            };

            if let Err(e) = run_containers_observer_cycle(registry.clone(), interval).await {
                tracing::warn!(error = %e, "containers observer cycle ended unexpectedly");
            }

            tokio::time::sleep(retry_delay).await;
        }
    });
}

async fn run_containers_observer_cycle(registry: Weak<Inner>, interval: Duration) -> AppResult<()> {
    let Some(registry_ref) = registry.upgrade() else {
        return Ok(());
    };

    registry_ref.request_observed_update()?;
    let docker = registry_ref.docker.clone();
    drop(registry_ref);

    let mut ticker = tokio::time::interval(interval);
    let mut filters = HashMap::new();
    filters.insert("type".to_string(), vec!["container".to_string()]);
    let options = EventsOptionsBuilder::default().filters(&filters).build();
    let mut events = docker.events(Some(options));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let Some(registry_ref) = registry.upgrade() else {
                    return Ok(());
                };

                if let Err(e) = registry_ref.request_observed_update() {
                    tracing::warn!(error = %e, "failed to refresh observed containers on interval");
                }
            }
            event = events.next() => {
                match event {
                    Some(Ok(event)) => {
                        let event_action = event.action.as_deref().unwrap_or("unknown");
                        let container_id = event
                            .actor
                            .as_ref()
                            .and_then(|actor| actor.id.as_deref())
                            .unwrap_or("unknown");

                        tracing::debug!(
                            action = %event_action,
                            container_id = %container_short_id(container_id),
                            "docker container event received"
                        );

                        let Some(registry_ref) = registry.upgrade() else {
                                    return Ok(());
                                };

                        if let Err(e) = registry_ref.request_observed_update() {
                            tracing::warn!(error = %e, "failed to refresh observed containers on event");
                        }

                    }
                    Some(Err(err)) => {
                        return Err(AppError::Docker(format!("docker container events stream error: {err}")));
                    }
                    None => {
                        return Err(AppError::Docker("docker container events stream ended".into()));
                    }
                }
            }
        }
    }
}
