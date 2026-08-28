# Architecture overview

```mermaid
flowchart LR
    subgraph Left[Workers]
        direction TB
        MetricsCollector[metrics collector]
        LogIngest[log ingestion]
        Retention[cleanup worker]
    end

    subgraph Middle[Services]
        direction TB
        Docker[DockerService]
        Registry[ContainerRegistry]
        Metadata[metadata store]
        LogBuffer[log buffer]
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

    class MetricsCollector,LogIngest,LogBuffer,Retention worker;
    class Docker,Registry service;
    class MetricsStore,LogStore,Metadata store;
    class Health,Containers,Metrics,Logs api;
```


## Component responsibilities

- API module: exposes the HTTP endpoints and composes responses from Docker state and persisted data.
- DockerService: the Docker-facing abstraction that provides container lists/details, log streams, samples, and control actions.
- ContainerRegistry: maintains discovered containers and their observed runtime state for DockerService.
- Metrics collector: periodically reads host and container metrics and writes time-series samples to the metrics store.
- Log ingestion: consumes the live Docker log stream and forwards lines into the buffering pipeline.
- Log buffer: batches/debounces log lines before persistence, then writes log rows and metadata updates.
- Metrics store: persists host and container metrics for dashboards and historical queries.
- Log store: persists log entries and supports grouped log retrieval.
- Metadata store: stores log checkpoints/positions and related indexing metadata used by ingestion and Docker resume logic.
- Cleanup worker: periodically removes data older than the configured retention window.

This is the clean backend wiring: the API reads from Docker and the stores, the metrics collector reads from Sysinfo and Docker and writes metrics, and the log ingestion pipeline reads from Docker and writes logs/metadata.
