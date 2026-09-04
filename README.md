# VPSiner

VPSiner is a small, self-hosted dashboard for monitoring and operating a single
Docker host. It persists logs and keeps them linked across container restarts and
recreations. It also collects host and container metrics, and can start, stop, or
restart containers from its web interface.

It is designed for home labs and small servers: one container, one Docker host, no
external services. With a dozen containers running, it typically uses about
50 MB of memory.

![Screen recording of the main features of the VPSiner tool](https://github.com/user-attachments/assets/8c130004-cb7c-42ca-8538-625214222968)

## Table of contents

- [VPSiner](#vpsiner)
  - [Table of contents](#table-of-contents)
  - [Features](#features)
    - [Planned features](#planned-features)
    - [Not planned](#not-planned)
  - [Quick start](#quick-start)
    - [Add some test logs](#add-some-test-logs)
  - [Configuration](#configuration)
    - [Common settings](#common-settings)
    - [Services](#services)
    - [Container controls](#container-controls)
    - [UI customization and custom links](#ui-customization-and-custom-links)
    - [Data persistence](#data-persistence)
    - [Database schema versions](#database-schema-versions)
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
- UI customization with custom sidebar links and branding

### Planned features

The following improvements are planned:

- Resource optimization, including compute power, memory, frontend and backend
  network communication, and storage size
- Log explorer UX enhancements, including parsing log fields, filtering by
  container instance, and visualizing log volumes
- Dead-simple alerting and alert history, starting with alerts when a container
  crashes

### Not planned

The following are intentionally out of scope:

- Authorization; use an authentication proxy to choose and configure the auth
  mechanism that fits your environment
- Collecting data from multiple hosts
- Metrics, traces, or other advanced observability features


## Quick start

Run VPSiner with access to the local Docker socket and a persistent data
directory:

```bash
docker pull ghcr.io/skvostik/vpsiner:latest
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

See the [Docker Compose example](examples/vpsiner/docker-compose.yml)
for a sample setup.

VPSiner has no authentication or authorization, and none is planned. Do not
publish it directly to the internet. If it is reachable outside a trusted
network, put it behind an authentication-enabled reverse proxy. A Docker socket
proxy and an authentication proxy protect different boundaries; public setups
should use both.

### Add some test logs

No other containers running yet? Start few instances of
[Funny Logger](https://github.com/skvostik/funny-logger) and place it in a
`demo-service` service:

```bash
docker run -d \
  --label vpsiner.service=demo-service \
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

| Variable                  | Default                       | Description                                                                    |
| ------------------------- | ----------------------------- | ------------------------------------------------------------------------------ |
| `VPSINER_DOCKER_HOST`     | `unix:///var/run/docker.sock` | Docker socket or socket-proxy endpoint, for example `http://docker-proxy:2375` |
| `VPSINER_RETENTION_WEEKS` | `4`                           | Number of weeks of metrics and logs to retain                                  |
| `VPSINER_DOCKER_CONTROLS` | `auto`                        | Container controls mode: `auto`, `enabled`, or `disabled`                      |
| `VPSINER_PORT`            | `3000`                        | HTTP listen port inside the container                                          |
| `RUST_LOG`                | `info`                        | Backend log filter, such as `debug` or `vpsiner=debug`                         |

There are further advanced variables for tuning intervals, buffer sizes and
timeouts. The **Configuration** page in the app lists every supported variable
with its description, default, and the value the running instance resolved.

### Services

`service` is the stable identity for a logical service. It groups both the
container's logs and metrics, and links their history when a container is
recreated and gets a new container ID.

VPSiner chooses the `service` value in this order:

1. The container's `vpsiner.service` label.
2. Docker Compose project and service labels, producing `{project}-{service}`.
   Note that a VPSiner service is therefore usually *coarser* than a Compose
   service: it is the project and Compose service combined.
3. The container name.

Docker Compose labels are added automatically, so most Compose deployments need
no configuration. Use an explicit label when several containers should share a
service:

```yaml
services:
  worker:
    image: example/worker
    labels:
      vpsiner.service: jobs
```

### Container controls

VPSiner exposes only start, stop, and restart actions. It does not remove
containers or edit their configuration.

In the default `auto` mode, VPSiner safely probes whether its Docker connection
allows the required `POST` requests. If a socket proxy blocks them, the buttons
are hidden and the backend rejects direct control requests as well. Set
`VPSINER_DOCKER_CONTROLS=disabled` for a monitoring-only installation, or allow
the start, stop, and restart endpoints in your socket proxy to enable controls.

### UI customization and custom links

You can customize the dashboard branding (name, eyebrow subtitle, and browser page title)
and configure custom navigation links in the sidebar menu so VPSiner can serve as a
main server dashboard with convenient links to your other services.

Mount a configuration directory to `/config` (or point `VPSINER_CONFIG_PATH` to
your config directory) containing a `ui.json` file:

```json
{
  "name": "VPSiner",
  "eyebrow": "Simply Observed",
  "links": [
    {
      "icon": "Github",
      "label": "GitHub",
      "url": "https://github.com/skvostik/vpsiner"
    },
    {
      "icon": "Server",
      "label": "Portainer",
      "url": "https://portainer.example.com"
    },
    {
      "icon": "HardDrive",
      "label": "Nextcloud",
      "url": "https://nextcloud.example.com"
    }
  ]
}
```

- `name`: Custom title/brand name shown in the sidebar header and browser `<title>` (default: `"VPSiner"`).
- `eyebrow`: Eyebrow subtitle text above the title in the sidebar (default: `"Simply Observed"`).
- `icon`: Any Lucide icon name (e.g. `Server`, `HardDrive`, `Globe`, `Activity`, `Terminal`, `Database`).
- `label`: Label displayed in the sidebar navigation.
- `url`: Destination URL (opened in a new tab).

If `ui.json` is not present, VPSiner defaults to `"VPSiner"`, `"Simply Observed"`, and a link to the VPSiner GitHub repository.

### Data persistence

VPSiner stores host and container metrics in `/data/metrics/metrics.db`, log
ingestion checkpoints in `/data/metadata/metadata.db`, and logs in weekly SQLite
databases under `/data/logs/`. Mount `/data` to a named volume or host
directory, as shown in the quick start, to preserve history when the container
is replaced.

Metrics and logs older than `VPSINER_RETENTION_WEEKS` are removed automatically.
Back up the data directory if historical monitoring data matters to you.

### Database schema versions

Some releases change the database schema in an incompatible way. VPSiner tracks
this in `/data/versions.json` and refuses to start when the databases on disk do
not match the ones the release expects, telling you exactly what it found.

To move on, restart once with `VPSINER_FORCE_DB_MIGRATION=1`. This **permanently
deletes** the incompatible databases and starts them empty; databases that still
match keep their history. The error message names the exact folders, so you can
also just delete them yourself instead of setting the variable. Back up `/data`
first if you need the old contents.

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

For a quick production-like local smoke test (including Docker socket proxy), run:

```bash
docker compose -f examples/vpsiner/docker-compose.yml up --build -d
```

Then open [http://localhost:3000](http://localhost:3000). Stop it with:

```bash
docker compose -f examples/vpsiner/docker-compose.yml down
```

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
