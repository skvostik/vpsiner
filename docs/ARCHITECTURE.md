# Architecture overview

```mermaid
flowchart LR
    subgraph Left[Workers]
        direction TB
        MetricsCollector[metrics collector]
        LogIngest[log ingestion]
        Retention[cleanup worker]
    end

    subgraph Middle[Services + Stores]
        direction TB
        Docker[DockerService]
        MetricsStore[SQLite metrics store]
        LogStore[SQLite log store]
        Metadata[SQLite metadata store]
    end

    subgraph Right[API endpoints]
        direction TB
        Health[GET /health]
        Containers[GET / POST /containers]
        Metrics[GET /metrics/*]
        Logs[GET /logs*]
    end

    MetricsCollector -->|reads container stats| Docker
    MetricsCollector -->|writes samples| MetricsStore

    LogIngest -->|reads live logs| Docker
    LogIngest -->|writes log rows| LogStore
    LogIngest -->|writes metadata| Metadata

    Retention -->|deletes expired metrics| MetricsStore
    Retention -->|deletes expired logs| LogStore

    Docker -->|serves health info| Health
    Docker -->|serves container state| Containers
    Docker -->|serves start/stop/restart actions| Containers
    MetricsStore -->|serves history + snapshots| Metrics
    LogStore -->|serves log groups + entries| Logs
    Metadata -->|serves log metadata| Logs

    classDef worker fill:#fef3c7,stroke:#f59e0b,stroke-width:1px,color:#111827;
    classDef service fill:#e8f1ff,stroke:#3b82f6,stroke-width:1px,color:#111827;
    classDef store fill:#ecfdf5,stroke:#10b981,stroke-width:1px,color:#111827;
    classDef api fill:#f3e8ff,stroke:#8b5cf6,stroke-width:1px,color:#111827;

    class MetricsCollector,LogIngest,Retention worker;
    class Docker service;
    class MetricsStore,LogStore,Metadata store;
    class Health,Containers,Metrics,Logs api;
```


## Component responsibilities

- API module: handles health checks, container actions, metrics queries, and log queries by reading from the Docker service and the SQLite stores.
- DockerService: the Docker-facing abstraction; it exposes container data, logs, and control operations, hiding the underlying Bollard implementation.
- Metrics collector: reads host telemetry from Sysinfo and per-container samples from Docker, then writes aggregated metric snapshots to the metrics store.
- Log ingestion: consumes live Docker log streams, batches them through the log buffer, and persists both log entries and their metadata.
- SQLite metrics store: persists time-series host and container metrics used by the dashboard and historical charts.
- SQLite log store: stores log payloads and queryable log data grouped by container or log group.
- SQLite metadata store: keeps the metadata needed to index and organize log data efficiently.
- Storage retention cleanup worker: deletes expired data so the metrics and log databases remain bounded over time.

This is the clean backend wiring: the API reads from Docker and the stores, the metrics collector reads from Sysinfo and Docker and writes metrics, and the log ingestion pipeline reads from Docker and writes logs/metadata.
