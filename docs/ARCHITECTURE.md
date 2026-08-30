# Architecture overview

- [Architecture overview](#architecture-overview)
  - [Components](#components)
  - [DockerService](#dockerservice)
  - [ContainerRegistry](#containerregistry)
  - [LogBuffer](#logbuffer)

## Components

This diagram shows the high-level backend wiring. It focuses on which components communicate with each other and where data is read, buffered, persisted, and exposed through API endpoints.

```mermaid
flowchart TB
    subgraph Left[Workers]
        direction TB
        LogIngest[log ingestion]
        MetricsCollector[metrics collector]
        Retention[cleanup worker]
    end

    subgraph Middle[Services]
        direction TB
        Docker[DockerService]
        Registry[ContainerRegistry]
        Metadata[metadata store]
        LogBuffer[LogBuffer]
        LogStore[log store]
        MetricsStore[metrics store]
    end

    subgraph Right[API endpoints]
        direction TB
        Health[GET /health]
        Containers[GET / POST /containers]
        ContainersStream[GET /stream/containers]
        Logs[GET /logs*]
        LogsStream[GET /stream/logs*]
        Metrics[GET /metrics/*]
        MetricsStream[GET /stream/metrics/current]
    end

    LogIngest -->|read| Docker
    LogIngest -->|push| LogBuffer
    LogBuffer -->|write| Metadata
    LogBuffer -->|write| LogStore

    Docker -->|use| Registry
    Docker -->|read| Metadata

    Retention -->|delete| MetricsStore
    Retention -->|delete| LogStore

    Docker --> Health
    Docker --- Containers
    Docker --- ContainersStream
    LogStore --> Logs
    Metadata --> Logs
    MetricsStore --> Metrics
    MetricsCollector -.->|push on change| MetricsStream
    LogBuffer -.->|push on flush| LogsStream

    MetricsCollector -->|read| Docker
    MetricsCollector -->|write| MetricsStore

    classDef worker fill:#fef3c7,stroke:#f59e0b,stroke-width:1px,color:#111827;
    classDef service fill:#e8f1ff,stroke:#3b82f6,stroke-width:1px,color:#111827;
    classDef store fill:#ecfdf5,stroke:#10b981,stroke-width:1px,color:#111827;
    classDef api fill:#f3e8ff,stroke:#8b5cf6,stroke-width:1px,color:#111827;

    class MetricsCollector,LogIngest,Retention worker;
    class Docker,Registry,LogBuffer service;
    class MetricsStore,LogStore,Metadata store;
    class Health,Containers,ContainersStream,Metrics,MetricsStream,Logs,LogsStream api;
```

- Metrics collector: periodically reads host and container metrics and writes time-series samples to the metrics store.
- Log ingestion: consumes the live Docker log stream and forwards lines into the buffering pipeline.
- Cleanup worker: periodically removes data older than the configured retention window.
- DockerService: the Docker-facing abstraction that provides container lists/details, log streams, samples, and control actions.
- ContainerRegistry: maintains discovered containers and their observed runtime state for DockerService.
- LogBuffer: batches/debounces log lines before persistence, then writes log rows and metadata updates; also bumps a `LogFlushWatcher` signal per group (plus a global one) on every successful flush, driving `GET /stream/logs` and `GET /stream/logs/{log_group}` instead of polling.
- Metrics store: persists host and container metrics for dashboards and historical queries.
- Log store: persists log entries and supports grouped log retrieval.
- Metadata store: stores log checkpoints/positions and related indexing metadata used by ingestion and Docker resume logic.
- API module: exposes the HTTP endpoints and composes responses from Docker state and persisted data.
- `GET /stream/metrics/current` and `GET /stream/containers`: Server-Sent Events push equivalents of `GET /metrics/current` and `GET /containers`. Both are driven by a `tokio::sync::watch` revision counter bumped by the underlying in-memory cache (the metrics snapshot, and the ContainerRegistry's `containers_info` cache respectively) whenever it changes, instead of clients polling on a timer.

This is the clean backend wiring: the API reads from Docker and the stores, the metrics collector reads from Sysinfo and Docker and writes metrics, and the log ingestion pipeline reads from Docker and writes logs/metadata.


## DockerService

This section describes the internal orchestration inside DockerService. Its main responsibility is to bridge Docker runtime signals to internal streams and background workers used by metrics and log pipelines.

```mermaid
flowchart TB
    Docker[DockerService]
    Registry[ContainerRegistry]
    Metadata[metadata store]
    LogsOut[logs stream]
    SamplesOut[container samples stream]

    subgraph Bg[Workers]
        direction TB
        Probe[write probe worker]
        SampleObs[sample observer worker]
        Observe[log observer worker]
    end

    subgraph PerContainer[Per running container]
        direction TB
        LogTask[log task]
    end

    Docker -->|use| Registry
    Docker -->|read checkpoint| Metadata

    Docker -->|spawn| Probe
    Docker -->|spawn| Observe
    Docker -->|spawn| SampleObs

    Observe -->|watch running containers| Registry
    Observe -->|spawn per running container| LogTask

    LogTask -->|read checkpoint| Metadata
    LogTask -->|tail docker logs| LogsOut

    SampleObs -->|read container stats| Registry
    SampleObs -->|emit batches| SamplesOut
```

- One log worker task is created per currently running container.
- When the set of running containers changes, DockerService reconciles workers: starts new ones for newly running containers and drops finished ones.

## ContainerRegistry

This section describes how ContainerRegistry keeps container state current. It combines periodic refresh and Docker event triggers, then exposes both cached views and observe events for DockerService workers.

```mermaid
flowchart TB
    subgraph Inputs[Docker signals]
        direction TB
        Tick[periodic tick]
        Events[docker events stream]
    end

    subgraph Core[ContainerRegistry]
        direction TB
        Observer[containers observer]
        ObservedUpd[observed update worker]
        InfoTick[containers-info tick worker]
        InfoUpd[containers-info update worker]
        ObservedSet[observed containers set]
        InfoCache[containers info cache]
    end

    ObserveEvents[observe events stream]

    Tick -->|trigger| Observer
    Events -->|trigger| Observer
    Observer -->|schedule| ObservedUpd
    ObservedUpd -->|refresh running set| ObservedSet
    ObservedUpd -->|emit start/stop| ObserveEvents

    Tick -->|schedule| InfoTick
    InfoTick -->|schedule| InfoUpd
    ObservedUpd -->|schedule| InfoUpd
    InfoUpd -->|refresh all containers| InfoCache
```

- Maintains two views: a fast observed-running set and a broader containers-info cache.
- Coalesces refresh requests with debounce workers to avoid redundant Docker calls.
- Emits start/stop observe events consumed by DockerService log worker orchestration.
- Bumps a `watch::Sender<u64>` revision counter whenever `containers_info` is refreshed; `GET /stream/containers` subscribes to it to know when to recompute and push a diff, instead of polling.

## LogBuffer

This section explains LogBuffer's two primary responsibilities. First, it debounces and batches log writes per group so write load stays stable. Second, it uses checkpoint metadata to drive deduplication and safe backfilling behavior across restarts.

```mermaid
flowchart TB
    In[log lines from ingestion]
    Metadata[metadata store]
    LogStore[log store]

    subgraph BufferCore[LogBuffer]
        direction TB
        Seed[startup checkpoint preload]
        Groups[group map by log_group]
        Dedup[per-container checkpoint dedup]
    end

    subgraph PerGroup[Per log_group]
        direction TB
        Queue[buffered lines]
        Flush[debounced flush worker]
    end

    In -->|push| Groups
    Seed ---|list checkpoints| Metadata
    Seed -->|seed state| Groups
    Groups -->|route by group| Dedup
    Dedup -->|accept| Queue
    Queue -->|schedule| Flush
    Flush -->|record received checkpoint| Metadata
    Flush -->|append| LogStore
```

- One flush worker exists per log group, so bursts in one group do not block other groups.
- Startup preloads checkpoints from metadata so dedup and backfill resume logic survive restarts.
- Dedup drops strictly older lines and exact boundary duplicates per container.
- Checkpoint writes are the mechanism that enables safe backfilling without duplicating already persisted lines.
- Flush is debounced and coalesces repeated flush requests before writing.
- On a successful flush, `LogFlushWatcher::notify(log_group)` bumps that group's `watch` channel and a global one; `GET /stream/logs/{log_group}` subscribes per-group to push newly-flushed lines forward, and `GET /stream/logs` subscribes to the global signal (plus `DockerService::subscribe_containers_info()`, since a group's `live` flag depends on container state, not flushes) to push a diff of the groups list.


