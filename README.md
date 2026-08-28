# VPSiner

VPSiner is a small, self-hosted dashboard for monitoring and operating a single
Docker host. It collects host and container metrics, keeps container logs in one
place, and can start, stop, or restart containers from its web interface.

It is built for home labs and small servers: one container, one Docker host, no
external services. In a typical home-lab setup, observed memory consumption is
under 50 MB. Actual usage depends on the number of containers and log volume.

- [VPSiner](#vpsiner)
  - [Features](#features)
    - [Planned features](#planned-features)
    - [Not planned](#not-planned)
  - [Screenshots](#screenshots)
  - [Quick start](#quick-start)
    - [Add some test logs](#add-some-test-logs)
  - [Configuration](#configuration)
    - [Common settings](#common-settings)
    - [Advanced settings](#advanced-settings)
    - [Log groups](#log-groups)
    - [Container controls](#container-controls)
    - [Data persistence](#data-persistence)
  - [Development](#development)
  - [Contributing](#contributing)

## Features

- One image that works out of the box with low resource consumption
- Host CPU, memory, storage, network, and disk metrics
- Container metrics with history and per-container details
- Centralized logs with text, time, stream, and log-level filters
- Log backfilling from Docker's available history
- Logs and metrics history persisted across container restarts and recreation
- Optional start, stop, and restart controls
- SQLite storage with configurable retention
- Responsive web interface included in the image

### Planned features

The following improvements are planned:

- Resource optimization, including compute power, memory, frontend and backend
  network communication, and storage size
- Log explorer UX enhancements, including parsing log fields, filtering by
  container instance, and visualizing log volumes
- Dead-simple alerting and alert history, starting with alerts when a container
  crashes
- UI customization, including custom links in the menu so VPSiner can serve as
  a main server dashboard with convenient links to other running services

### Not planned

The following are intentionally out of scope:

- Authorization; use an authentication proxy to choose and configure the auth
  mechanism that fits your environment
- Collecting data from multiple hosts
- Metrics, traces, or other advanced observability features

## Screenshots

![Screen recording of the main features of the VPSiner tool](https://github.com/user-attachments/assets/8c130004-cb7c-42ca-8538-625214222968)

## Quick start

Run VPSiner with access to the local Docker socket and a persistent data
directory:

```bash
docker run -d \
  --name vpsiner \
  --restart unless-stopped \
  -p 3000:3000 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v vpsiner-data:/data \
  ghcr.io/skvostik/vpsiner:latest
```

Open [http://localhost:3000](http://localhost:3000).

Mounting the Docker socket is the simplest way to get started, but it gives the
container powerful access to Docker. For a long-running setup, use a Docker
socket proxy and set `VPSINER_DOCKER_HOST` to its HTTP endpoint.

See the [Docker socket Compose example](examples/docker-socket/docker-compose.yml)
for a sample setup.

VPSiner has no authentication or authorization, and none is planned. Do not
publish it directly to the internet. If it is reachable outside a trusted
network, put it behind an authentication-enabled reverse proxy. A Docker socket
proxy and an authentication proxy protect different boundaries; public setups
should use both.

### Add some test logs

No other containers running yet? Start few instances of
[Funny Logger](https://github.com/skvostik/funny-logger) and place it in a
`demo-group` log group:

```bash
docker run -d \
  --label vpsiner.log_group=demo-group \
  ghcr.io/skvostik/funny-logger:latest
```

The container and its output will appear in VPSiner shortly afterward.

Alternatively, from the repository root, start the test load example:

```bash
docker compose -f examples/test-load/docker-compose.yml up -d
```

## Configuration

Configuration is read from environment variables when VPSiner starts. The
defaults work for the quick-start command above.

### Common settings

| Variable                  | Default in the image          | Description                                                                    |
| ------------------------- | ----------------------------- | ------------------------------------------------------------------------------ |
| `VPSINER_DOCKER_HOST`     | `unix:///var/run/docker.sock` | Docker socket or socket-proxy endpoint, for example `http://docker-proxy:2375` |
| `VPSINER_RETENTION_WEEKS` | `4`                           | Number of weeks of metrics and logs to retain                                  |
| `VPSINER_DOCKER_CONTROLS` | `auto`                        | Container controls mode: `auto`, `enabled`, or `disabled`                      |
| `TOKIO_WORKER_THREADS`    | unset                         | Overrides Tokio runtime worker-thread count; by default Tokio uses available CPU parallelism |

### Advanced settings

These normally do not need to be changed.

| Variable                                 | Default         | Description                                                                                                      |
| ---------------------------------------- | --------------- | ---------------------------------------------------------------------------------------------------------------- |
| `VPSINER_DATA_PATH`                      | `/data`         | Directory containing metrics and log databases                                                                   |
| `VPSINER_PORT`                           | `3000`          | HTTP listen port inside the container                                                                            |
| `VPSINER_COLLECT_INTERVAL_SECS`          | `10`            | Host and container metrics collection interval                                                                   |
| `VPSINER_LOG_FLUSH_DEBOUNCE_MS`          | `500`           | Delay used to coalesce buffered log lines per log group before writing them to storage                           |
| `VPSINER_LOG_CHANNEL_CAPACITY`           | `10000`         | Maximum number of log lines buffered before backpressure                                                         |
| `VPSINER_SAMPLES_CHANNEL_CAPACITY`       | `32`            | Maximum number of container sample batches buffered before backpressure                                          |
| `VPSINER_DOCKER_PROBE_INTERVAL_SECS`     | `60`            | Interval for Docker write-capability probing, log observer fallback reconciliation, and registry refresh workers |
| `VPSINER_DOCKER_RETRY_SECS`              | `5`             | Delay before retrying the Docker container event observer after its stream ends or fails                         |
| `VPSINER_DOCKER_REQUEST_CONCURRENCY`     | `8`             | Maximum number of concurrent Docker inspect and stats requests                                                   |
| `VPSINER_DOCKER_EVENTS_CHANNEL_CAPACITY` | `256`           | Maximum number of container observe events buffered before new events are dropped                                |
| `VPSINER_DOCKER_DEBOUNCE_MS`             | `1000`          | Delay used to coalesce container observation and container info refresh requests                                 |
| `VPSINER_DOCKER_TIMEOUT_SECS`            | `60`            | Internal timeout for Docker API requests                                                                         |
| `VPSINER_DOCKER_REQUEST_TIMEOUT_SECS`    | `5`             | Timeout for fetch requests                                                                                       |
| `VPSINER_STATIC_DIR`                     | Bundled UI path | Directory from which the backend serves the frontend                                                             |
| `RUST_LOG`                               | `info`          | Backend log filter, such as `debug` or `vpsiner=debug`                                                           |

### Log groups

`log_group` is the stable identity for a logical service. It groups both the
container's logs and metrics, and links their history when a container is
recreated and gets a new container ID.

VPSiner chooses the `log_group` value in this order:

1. The container's `vpsiner.log_group` label.
2. Docker Compose project and service labels, producing `{project}-{service}`.
3. The container name.

Docker Compose labels are added automatically, so most Compose deployments need
no configuration. Use an explicit label when several containers should share a
group:

```yaml
services:
  worker:
    image: example/worker
    labels:
      vpsiner.log_group: jobs
```

### Container controls

VPSiner exposes only start, stop, and restart actions. It does not remove
containers or edit their configuration.

In the default `auto` mode, VPSiner safely probes whether its Docker connection
allows the required `POST` requests. If a socket proxy blocks them, the buttons
are hidden and the backend rejects direct control requests as well. Set
`VPSINER_DOCKER_CONTROLS=disabled` for a monitoring-only installation, or allow
the start, stop, and restart endpoints in your socket proxy to enable controls.

### Data persistence

VPSiner stores host and container metrics in `/data/metrics.db` and logs in
weekly SQLite databases under `/data/logs/`. Mount `/data` to a named volume or
host directory, as shown in the quick start, to preserve history when the
container is replaced.

Metrics and logs older than `VPSINER_RETENTION_WEEKS` are removed automatically.
Back up the data directory if historical monitoring data matters to you.

## Development

The backend is written in Rust using Axum, Tokio, Bollard, and SQLite. The
frontend uses Vue 3, TypeScript, Vite, Naive UI, and ECharts.

For local development, start the backend from the repository root:

```bash
cargo run
```

Then start the frontend in another terminal:

```bash
cd frontend
npm ci
npm run dev
```

Open [http://localhost:5100](http://localhost:5100). The Vite development server
forwards `/api` requests to the backend on port `3000`. The backend needs access
to Docker through the local socket or `VPSINER_DOCKER_HOST`.

To build the same complete image used for releases:

```bash
docker build -t vpsiner .
```

The REST API is documented in [docs/API.md](docs/API.md).
Backend wiring and component design are documented in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Contributing

VPSiner is available under the [MIT License](LICENSE). Feedback and contributions
are welcome. Please use [GitHub Issues](https://github.com/skvostik/vpsiner/issues)
for feature requests and bug reports.

Please report security issues privately to [info@skvostik.cz](mailto:info@skvostik.cz)
instead of opening a public issue.
