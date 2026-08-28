# Architecture overview

- [Architecture overview](#architecture-overview)
  - [Components](#components)
  - [DockerService](#dockerservice)
  - [ContainerRegistry](#containerregistry)
  - [LogBuffer](#logbuffer)

## Components

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
        Logs[GET /logs*]
        Metrics[GET /metrics/*]
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
    LogStore --> Logs
    Metadata --> Logs
    MetricsStore --> Metrics

    MetricsCollector -->|read| Docker
    MetricsCollector -->|write| MetricsStore

    classDef worker fill:#fef3c7,stroke:#f59e0b,stroke-width:1px,color:#111827;
    classDef service fill:#e8f1ff,stroke:#3b82f6,stroke-width:1px,color:#111827;
    classDef store fill:#ecfdf5,stroke:#10b981,stroke-width:1px,color:#111827;
    classDef api fill:#f3e8ff,stroke:#8b5cf6,stroke-width:1px,color:#111827;

    class MetricsCollector,LogIngest,Retention worker;
    class Docker,Registry,LogBuffer service;
    class MetricsStore,LogStore,Metadata store;
    class Health,Containers,Metrics,Logs api;
```

- Metrics collector: periodically reads host and container metrics and writes time-series samples to the metrics store.
- Log ingestion: consumes the live Docker log stream and forwards lines into the buffering pipeline.
- Cleanup worker: periodically removes data older than the configured retention window.
- DockerService: the Docker-facing abstraction that provides container lists/details, log streams, samples, and control actions.
- ContainerRegistry: maintains discovered containers and their observed runtime state for DockerService.
- LogBuffer: batches/debounces log lines before persistence, then writes log rows and metadata updates.
- Metrics store: persists host and container metrics for dashboards and historical queries.
- Log store: persists log entries and supports grouped log retrieval.
- Metadata store: stores log checkpoints/positions and related indexing metadata used by ingestion and Docker resume logic.
- API module: exposes the HTTP endpoints and composes responses from Docker state and persisted data.

This is the clean backend wiring: the API reads from Docker and the stores, the metrics collector reads from Sysinfo and Docker and writes metrics, and the log ingestion pipeline reads from Docker and writes logs/metadata.


## DockerService

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

## LogBuffer

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
- Startup preloads checkpoints from metadata so dedup survives restarts.
- Dedup drops strictly older lines and exact boundary duplicates per container.
- Flush is debounced and coalesces repeated flush requests before writing.


