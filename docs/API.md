# API overview

This project exposes a REST API under the `/api` prefix.

General conventions:
- all routes start with `/api`
- timestamps are in Unix epoch milliseconds (`i64`)
- `from` must be less than or equal to `to`
- `POST` actions return `204 No Content` on success
- invalid requests return `400 Bad Request` unless another status is specified

### Metrics point semantics

Every metrics endpoint returns the same record shape: gauges carrying the sampled value, and `*_rate` fields carrying **bytes per second**. Cumulative counters are never returned; the server derives all rates.

Rate rules, applying to every endpoint:
- a rate is computed from consecutive raw samples as observed byte delta divided by observed elapsed time
- an interval whose counter decreased is treated as a counter reset (container or host restart) and is **excluded entirely** — it contributes to neither the delta nor the elapsed time
- a rate is `0` when nothing valid could be measured, for example a single sample with no predecessor, or an interval that is entirely a reset
- rates are summed when aggregating across containers; this is well-defined even when containers start and stop within the requested range

### Metrics bucket semantics

Applies to the three time-series endpoints (`/api/metrics/host`, `/api/metrics/containers/{service}`, `/api/metrics/containers`). It does **not** apply to `/api/metrics/current`, which returns latest values rather than buckets.

- The server selects `resolution` from the requested time range and returns it in the response. Clients do not send `resolution`.
- Resolution selection uses the range span (`to - from`): up to 30 minutes returns `10s`, up to 3 hours returns `1m`, up to 24 hours returns `5m`, and longer ranges return `1h`. Each cutoff allows 60 seconds of tolerance, so live streams opened from client-side rolling windows do not fall into a coarser resolution because of small clock or request delays. There is no un-downsampled/"raw" mode — `10s` is simply the finest fixed bucket size and follows the same rules as the others.
- Samples are grouped into half-open, epoch-aligned buckets: `(bucket_end - resolution, bucket_end]`, where `bucket_end = ceil(ts_ms / bucket_size_ms) * bucket_size_ms`. Bucket boundaries are anchored to the Unix epoch, not to the request's `from`. For example, `10s` buckets end on `:00, :10, :20, :30, :40`, or `:50` seconds of each minute; `1m` buckets end on each minute; `1h` buckets end on each hour.
- Returned `ts` is the bucket end.
- Only fully elapsed buckets (`ts <= now` at request time) are returned. An open trailing bucket is omitted, even if `to` extends past it.
- Empty buckets are omitted.
- Gauge metrics (e.g. `cpu_pct`, `mem_used`, `mem_total`, `storage_used`, `storage_total`, `metrics_size`, `logs_size`, `mem_limit`) are averaged across the samples within a bucket.
- Rate metrics (e.g. `net_rx_rate`, `net_tx_rate`, `disk_read_rate`, `disk_write_rate`, `blk_read_rate`, `blk_write_rate`) are derived once, when a `10s` bucket is written: the counter is interpolated at both bucket boundaries and the slope between them is the bucket's rate. Coarser resolutions average those stored `10s` rates.
- Rate metrics are `null` when the underlying counters could not produce a rate for the bucket — for example the first bucket after a container starts, or a bucket spanning a counter reset. A `null` rate means "unknown", not "zero".

## 1) Health check

### GET `/api/health`

Returns API availability and runtime metadata.

Parameters: none

Example response:
```json
{
  "ok": true,
  "app": "vpsiner",
  "version": "0.1.0",
  "port": 8080,
  "sample_interval_ms": 10000,
  "retention_weeks": 12,
  "docker_controls_available": true
}
```

`app` is the application name.

`sample_interval_ms` is the metrics sampling interval; clients should not poll metrics endpoints more often than this.

`version` is the application version string.

`retention_weeks` is the configured metrics and logs retention window in weeks.

`docker_controls_available` indicates whether the `/api/containers/{id}/start|stop|restart` endpoints are currently available. Clients should poll `/api/health` and show/hide container lifecycle controls based on this flag. This is enforced server-side: while `docker_controls_available` is `false`, action endpoints return `403 Forbidden`.

---

## 2) Configuration

### GET `/api/config/ui`

Returns frontend UI configuration, including custom branding (name and eyebrow) and custom sidebar navigation links.

Reads `ui.json` from the configured config directory (`VPSINER_CONFIG_PATH`, default `config` / `/config` in Docker). If `ui.json` does not exist or cannot be read, returns default configuration linking to the VPSiner GitHub repository.

Parameters: none

Example response:
```json
{
  "name": "VPSiner",
  "eyebrow": "Simply Observed",
  "links": [
    {
      "icon": "Github",
      "label": "GitHub",
      "url": "https://github.com/skvostik/vpsiner"
    }
  ]
}
```

Fields:
- `name`: Brand name shown in the sidebar header and browser page title (default: `"VPSiner"`)
- `eyebrow`: Eyebrow subtitle text shown above the brand name in the sidebar (default: `"Simply Observed"`)
- `links`: Array of custom link objects:
  - `icon`: Name of the Lucide icon (e.g., `Github`, `Server`, `Globe`, `Activity`, `HardDrive`, `Terminal`)
  - `label`: Display text for the menu item
  - `url`: Destination URL (opened in a new tab)

### GET `/api/config/settings`

Returns every environment variable VPSiner supports, together with the value the running instance resolved and the built-in default. Read-only; there is no endpoint to change settings, they are applied at startup from the process environment.

Parameters: none

Example response:
```json
[
  {
    "name": "VPSINER_DOCKER_HOST",
    "value": "http://docker-proxy:2375",
    "default": "unix:///var/run/docker.sock",
    "description": "Docker socket or socket-proxy endpoint, for example http://docker-proxy:2375",
    "category": "common",
    "overridden": true
  },
  {
    "name": "VPSINER_WORKER_THREADS",
    "value": "",
    "default": "",
    "description": "Overrides Tokio runtime worker-thread count; by default Tokio uses available CPU parallelism",
    "category": "common",
    "overridden": false
  }
]
```

Fields:
- `name`: Environment variable name
- `value`: Effective value in use, formatted in the same unit the variable accepts. Empty string when the setting is unset and has no default
- `default`: Built-in default. Empty string when the variable has no default
- `description`: Short human-readable description, matching the tables in the README
- `category`: `"common"` for frequently adjusted settings, `"advanced"` for tuning knobs
- `overridden`: `true` when the variable is present in the process environment, regardless of whether its value differs from the default

Ordering is stable and groups `common` entries before `advanced` ones, matching the order used in the README.

### GET `/api/config/computed`

Returns live values measured from the running backend. These values are read-only and may differ from their configured inputs; for example, Tokio chooses the available CPU parallelism when `VPSINER_WORKER_THREADS` is unset.

Parameters: none

Example response:
```json
[
  {
    "name": "tokio_worker_threads",
    "value": "4",
    "description": "Actual number of worker threads allocated to the Tokio runtime"
  }
]
```

Fields:
- `name`: Stable computed-value identifier
- `value`: Current value, formatted as a string
- `description`: Short human-readable explanation

`tokio_worker_threads` is the actual worker-thread allocation reported by the active Tokio runtime. It is not necessarily the same as the `VPSINER_WORKER_THREADS` setting: when that environment variable is unset, Tokio selects the allocation automatically.

---

## 3) List containers

### GET `/api/containers`

Returns containers and their `service` values.

`service` resolution precedence:
- if container label `vpsiner.service` exists and is non-empty, it is used
- otherwise, if both Docker Compose labels `com.docker.compose.project` and `com.docker.compose.service` exist and are non-empty, `service` is `{project}-{service}`
- otherwise, `service` falls back to the container name without a leading slash

Note that a VPSiner `service` is usually coarser than a Docker Compose service, because it combines the Compose project and service names as `{project}-{service}`.

Parameters: none

Example response:
```json
[
  {
    "id": "8af7d6c1273d",
    "name": "web-1",
    "service": "project-web",
    "image": "nginx:latest",
    "image_sha": "sha256:abcd...",
    "ports": ["80:80", "443:443"],
    "labels": ["com.docker.compose.project=project", "com.docker.compose.service=web"],
    "state": "running",
    "started_at": null
  },
  {
    "id": "5aa2bca1f0db",
    "name": "db-1",
    "service": "project-db",
    "image": "mysql:8",
    "image_sha": "sha256:efgh...",
    "ports": ["3306:3306"],
    "labels": ["com.docker.compose.project=project", "com.docker.compose.service=db"],
    "state": "exited",
    "started_at": null
  }
]
```

Allowed `state` values:
- `created`
- `restarting`
- `running`
- `removing`
- `paused`
- `exited`
- `dead`

Containers no longer available are not listed; there is no `removed` state.

`started_at` is nullable. For running containers it is the best available Docker start timestamp in Unix milliseconds. Clients must tolerate `null` for any state.

---

## 4) Container management

### POST `/api/containers/{id}/start`
### POST `/api/containers/{id}/stop`
### POST `/api/containers/{id}/restart`

Starts, stops, or restarts a container by full container ID.

The API intentionally exposes only these three non-destructive control actions. It does not expose destructive actions such as remove, and it does not expose pause as a separate action.

Action semantics:
- `start` means "make this container running" when possible
- `stop` means "make this container not running" when possible
- `restart` means "stop and start this container" when possible

State transition behavior:
- from `created`: `start` starts the container; `stop` is a no-op success; `restart` starts the container
- from `exited`: `start` starts the container; `stop` is a no-op success; `restart` starts the container
- from `running`: `start` is a no-op success; `stop` stops the container; `restart` restarts the container
- from `paused`: `start` submits a Docker start request; `stop` stops the container; `restart` submits a Docker restart request
- from `restarting`: `stop` submits a Docker stop request; `start` and `restart` return `409 Conflict`
- from `removing`: the container is being removed; control actions return `409 Conflict`
- from `dead`: control actions return `409 Conflict`

Idempotency:
- `start` on `running` returns `204 No Content`
- `stop` on `created` or `exited` returns `204 No Content`
- transition/broken states return `409 Conflict` when the requested action cannot be applied safely

Availability:
- returns `403 Forbidden` when `docker_controls_available` (see `GET /api/health`) is `false`

Parameters:
- `id` (path) – full container ID

Example: `POST /api/containers/8af7d6c1273d/start`

Result:
- HTTP `204 No Content` on success
- HTTP `4xx/5xx` on failure

---

## 5) Host metrics

### GET `/api/metrics/host?from={ts}&to={ts}`

Returns a time series of host metrics.

Metric semantics:
- `cpu_pct` is total host CPU utilization during the sample interval, expressed as a percentage of total logical CPU capacity
- `cpu_pct` uses a `0..100` scale, where `100` means all logical CPUs are fully utilized
- `mem_used`, `mem_total`, `storage_used`, `storage_total`, `metrics_size`, and `logs_size` are byte values
- `metrics_size` is the on-disk size of Vpsiner's metrics database at sample time
- `logs_size` is the combined on-disk size of Vpsiner's log databases at sample time
- `net_rx_rate`, `net_tx_rate`, `disk_read_rate`, and `disk_write_rate` are bytes per second or `null`; see "Metrics point semantics" above

Ordering:
- samples MUST be returned in ascending timestamp order: `ts ASC`

Downsampling: see "Metrics bucket semantics" above.

Default values:
- `from` and `to` are required

Query parameters:
- `from` (required): start time in ms
- `to` (required): end time in ms

Example:
```http
GET /api/metrics/host?from=1720000000000&to=1720003600000
```

Example response:
```json
{
  "resolution": "10s",
  "data": [
    {
      "ts": 1720000000000,
      "cpu_pct": 21.4,
      "mem_used": 2147483648,
      "mem_total": 8589934592,
      "storage_used": 104857600,
      "storage_total": 536870912,
      "metrics_size": 5242880,
      "logs_size": 73400320,
      "net_rx_rate": 15432.0,
      "net_tx_rate": 9650.0,
      "disk_read_rate": 2000.0,
      "disk_write_rate": 1500.0
    }
  ]
}
```

---

## 6) Current metrics snapshot

### GET `/api/metrics/current`

Returns the latest known value for the host, for every currently sampled container, and for every `service`. This is the endpoint to use for list and overview UIs that need one current number per entity; it is not a time series and cannot be used to plot history.

The records are the same `HostPoint`, `ContainerPoint`, and `GroupPoint` types the time-series endpoints return, so clients can share one model. See "Metrics point semantics" above for the rate rules.

Parameters: none. This endpoint takes no `from`, `to`, or `resolution`, and is not subject to the bucket semantics described above — values reflect the most recent raw sample, and rates are computed from the two most recent samples.

Gauge fields carry the raw value of the latest sample.

Timestamps:
- every record carries its own `ts`, the timestamp of the sample it was derived from
- there is no response-level timestamp; host and container samples are collected independently and their `ts` values are not synchronized

Staleness:
- records whose `ts` is older than three times `sample_interval_ms` (see `GET /api/health`) are omitted
- `host` is `null` when no recent host sample exists, including before the first sample after startup
- `containers` and `services` are empty objects when no container has been sampled recently, for example when no containers are running
- clients MUST treat `null` `host` and empty objects as valid successful responses
- a container that stops disappears from `containers` within the staleness window

Aggregation:
- keys under `containers` are container IDs
- `services` is derived by summing the entries in `containers` that share a `service`
- a `services` entry's `ts` is the greatest `ts` among its containers

Polling:
- clients SHOULD NOT poll more often than `sample_interval_ms` from `GET /api/health`

Example:
```http
GET /api/metrics/current
```

Example response:
```json
{
  "host": {
    "ts": 1720003600000,
    "cpu_pct": 21.4,
    "mem_used": 2147483648,
    "mem_total": 8589934592,
    "storage_used": 104857600,
    "storage_total": 536870912,
    "metrics_size": 5242880,
    "logs_size": 73400320,
    "net_rx_rate": 15432.0,
    "net_tx_rate": 9650.0,
    "disk_read_rate": 2000.0,
    "disk_write_rate": 1500.0
  },
  "containers": {
    "8af7d6c1273d": {
      "ts": 1720003600000,
      "service": "project-web",
      "cpu_pct": 12.8,
      "mem_used": 402653184,
      "mem_limit": 1073741824,
      "net_rx_rate": 10500.0,
      "net_tx_rate": 7300.0,
      "blk_read_rate": 5200.0,
      "blk_write_rate": 4100.0
    },
    "91bc832df407": {
      "ts": 1720003600000,
      "service": "project-web",
      "cpu_pct": 7.6,
      "mem_used": 301989888,
      "mem_limit": 1073741824,
      "net_rx_rate": 7300.0,
      "net_tx_rate": 5300.0,
      "blk_read_rate": 3600.0,
      "blk_write_rate": 3300.0
    }
  },
  "services": {
    "project-web": {
      "ts": 1720003600000,
      "cpu_pct": 20.4,
      "mem_used": 704643072,
      "mem_limit": 2147483648,
      "net_rx_rate": 17800.0,
      "net_tx_rate": 12600.0,
      "blk_read_rate": 8800.0,
      "blk_write_rate": 7400.0
    }
  }
}
```

Empty response example, valid when nothing has been sampled recently:
```json
{
  "host": null,
  "containers": {},
  "services": {}
}
```

---

## 7) Metrics for a specific service

### GET `/api/metrics/containers/{service}?from={ts}&to={ts}`

Returns container metrics for a single `service`.

The response contains:
- `sum`: one time series aggregated across all container IDs in this `service` at each timestamp
- `containers`: individual time series keyed by container ID, for viewing each sampled container separately

`sum` aggregates the metric values for all container samples in the `service` that share a timestamp.

Metric semantics:
- `cpu_pct` is the container CPU utilization during the sample interval, expressed as a percentage of total host logical CPU capacity
- a container using one full logical CPU on a host with `N` logical CPUs contributes approximately `100 / N` to `cpu_pct`
- aggregated `cpu_pct` values are summed by timestamp, so service CPU usage is comparable to host CPU usage and normally does not exceed `100`, aside from sampling jitter
- `mem_used` and `mem_limit` are byte values
- `net_rx_rate`, `net_tx_rate`, `blk_read_rate`, and `blk_write_rate` are bytes per second or `null`; see "Metrics point semantics" above

Aggregation rules:
- `sum` is computed by grouping the per-container buckets by exact `ts` and summing all numeric fields for that timestamp
- a summed rate counts only the containers whose rate is known for that timestamp, and is `null` only when every container's rate is `null`
- a `sum` data point at timestamp `ts` includes only container buckets with that timestamp
- missing container samples are not interpolated
- no synthetic zero-valued container samples are generated
- because rates rather than counters are summed, a container entering or leaving the service mid-range does not produce a spike in `sum`
- `containers` includes container IDs with metric samples for the requested `service` and time range

Ordering:
- `sum` samples MUST be returned in ascending timestamp order: `ts ASC`
- each array under `containers` MUST be returned in ascending timestamp order: `ts ASC`

Downsampling: see "Metrics bucket semantics" above. `sum` is calculated from each container's downsampled series by bucket timestamp.

Default values:
- `from` and `to` are required

Parameters:
- `service` (path): `service` from `/api/containers`
- `from` (query): start time in ms
- `to` (query): end time in ms

Example:
```http
GET /api/metrics/containers/project-web?from=1720000000000&to=1720003600000
```

Example response:
```json
{
  "resolution": "10s",
  "data": {
    "sum": [
      {
        "ts": 1720000000000,
        "cpu_pct": 20.4,
        "mem_used": 704643072,
        "mem_limit": 2147483648,
        "net_rx_rate": 17500.0,
        "net_tx_rate": 12600.0,
        "blk_read_rate": 8800.0,
        "blk_write_rate": 7400.0
      }
    ],
    "containers": {
      "8af7d6c1273d": [
        {
          "ts": 1720000000000,
          "service": "project-web",
          "cpu_pct": 12.8,
          "mem_used": 402653184,
          "mem_limit": 1073741824,
          "net_rx_rate": 10500.0,
          "net_tx_rate": 7300.0,
          "blk_read_rate": 5200.0,
          "blk_write_rate": 4100.0
        }
      ]
    }
  }
}
```

Notes:
- the keys under `containers` are container IDs
- `sum` data points do not include `service`, because the service is given by the request path
- `containers` data points include `service` so they match the records returned by `/api/metrics/current`

---

## 8) Aggregate metrics for all container services

### GET `/api/metrics/containers?from={ts}&to={ts}`

Returns aggregate container metrics for all `service` values during the given interval.

The response is keyed by `service`. Each value is the aggregated time series for that service, using the same summing rules as the `sum` field from `/api/metrics/containers/{service}`.

Metric semantics:
- `cpu_pct` uses the same host-normalized scale as `HostPoint.cpu_pct`
- aggregated `cpu_pct` values are summed by timestamp and are comparable to host CPU usage
- `mem_used` and `mem_limit` are byte values; `*_rate` fields are bytes per second

Aggregation rules:
- each `service` time series is computed by grouping the per-container buckets by exact `service` and `ts`, then summing all numeric fields
- a service data point at timestamp `ts` includes only container buckets with that timestamp
- missing container samples are not interpolated
- no synthetic zero-valued container samples are generated
- because rates rather than counters are summed, a container entering or leaving a service mid-range does not produce a spike

Ordering:
- each `service` time series MUST be returned in ascending timestamp order: `ts ASC`

Downsampling: see "Metrics bucket semantics" above. Each service series is calculated from downsampled container series by bucket timestamp.

Default values:
- `from` and `to` are required

Query parameters:
- `from` (required): start time in ms
- `to` (required): end time in ms

Example:
```http
GET /api/metrics/containers?from=1720000000000&to=1720003600000
```

Example response:
```json
{
  "resolution": "10s",
  "data": {
    "project-web": [
      {
        "ts": 1720000000000,
        "cpu_pct": 20.4,
        "mem_used": 704643072,
        "mem_limit": 2147483648,
        "net_rx_rate": 17500.0,
        "net_tx_rate": 12600.0,
        "blk_read_rate": 8800.0,
        "blk_write_rate": 7400.0
      }
    ],
    "project-db": [
      {
        "ts": 1720000000000,
        "cpu_pct": 5.2,
        "mem_used": 671088640,
        "mem_limit": 1073741824,
        "net_rx_rate": 3300.0,
        "net_tx_rate": 2200.0,
        "blk_read_rate": 7800.0,
        "blk_write_rate": 9300.0
      }
    ]
  }
}
```

---

## 9) List services

### GET `/api/logs`

Returns known `service` values with the timestamp of the newest log line and current live status. Services known only from Docker are included even when no logs have been stored yet.

Parameters: none

Ordering:
- object keys are emitted in ascending `service` order for stable responses

Notes:
- `last_received` is `null` when the service has no log lines
- `last_received` is the greatest `ts` in the service
- `live` is `true` when at least one container in the service is currently running

Example response:
```json
{
  "project-db": { "last_received": 1720003600000, "live": true },
  "project-web": { "last_received": 1720003550000, "live": false },
  "system-nginx": { "last_received": null, "live": false }
}
```

---

## 10) Query logs

### GET `/api/logs/{service}?from={ts}&to={ts}&q={text}&level={lvl}&stream={s}&limit={n}&before={token}&after={token}`

Returns paginated logs for the given service.

Each log line includes its `service`, source container ID, and a single text field (`line`) with ANSI/VT100 color escape sequences removed. `q` is sanitized on the backend and matched case-insensitively against `line`. The backend does not parse any other structured fields out of the text — that is left up to clients.

`q` matches arbitrary substrings, including across punctuation, but only substrings of 3 or more characters can match; shorter tokens never match any row. Search queries in `q` are parsed into whitespace-separated tokens or quoted phrases (e.g. `"some=value with spaces"`), treated as literal substrings, and combined with `OR`. Missing trailing quotes are automatically closed.

Ordering:
- results MUST be returned in ascending time order: oldest entries first
- entries with the same timestamp use a stable opaque order
- an initial request without `before` or `after` returns the newest matching page, but the items in that page are still ordered oldest-to-newest

Pagination:
- `before` fetches logs older than the cursor anchor; clients prepend returned `items` to an existing list
- `after` fetches logs newer than the cursor anchor; clients append returned `items` to an existing list
- `before` and `after` are mutually exclusive; providing both returns `400 Bad Request`
- cursors are opaque ordering anchors that identify a stable position in the log ordering
- `older_cursor` is the anchor for requesting logs older than the oldest returned item; it is non-null when `items` is non-empty
- `newer_cursor` is the anchor for requesting logs newer than the newest returned item; it is non-null when `items` is non-empty
- `has_older` indicates whether there are more matching logs older than `older_cursor` at response time
- `has_newer` indicates whether there are more matching logs newer than `newer_cursor` at response time
- if an `after` request returns no new items, `newer_cursor` repeats the submitted `after` cursor so clients can continue polling with the response cursor; `older_cursor` is `null`, `has_older` is `false`, and `has_newer` is `false`
- cursors do not encode filter values, but `has_older`/`has_newer` are only accurate when the filter (`from`, `to`, `q`, `level`, `stream`) matches the request that produced the cursor; clients MUST discard `before`/`after` cursors and issue a fresh unpaginated request when changing filters
- cursors are intended for the same `service`; clients MUST NOT reuse a cursor from one `service` with another `service`

Default values for this endpoint:
- `level`: no filtering
- `stream`: no filtering
- `limit`: `100`
- `from` / `to`: no default bounds
- `q` / `before` / `after`: no default values

Parameters:
- `service` (path): service
- `from` (optional): start filter time in ms; default: none (no lower bound)
- `to` (optional): end filter time in ms; default: none (no upper bound)
- `q` (optional): text search filter against `line`; whitespace-separated tokens or quoted phrases matched with `OR`; default: none
- `level` (optional): comma-separated log levels; allowed values: `debug`, `info`, `warn`, `error`; default: no filtering
- `stream` (optional): comma-separated streams; allowed values: `stdout`, `stderr`; default: no filtering
- `limit` (optional): maximum number of items in the response; default: `100` (bounded to `1..100`)
- `before` (optional): opaque cursor for fetching older logs before that cursor boundary; default: none
- `after` (optional): opaque cursor for fetching newer logs after that cursor boundary; default: none

Example:
```http
GET /api/logs/project-web?from=1720000000000&to=1720003600000&level=error,info&stream=stdout&limit=50&q=timeout
```

Example response:
```json
{
  "items": [
    {
      "ts": 1720000234567,
      "service": "project-web",
      "cid": "8af7d6c1273d",
      "stream": "stdout",
      "level": "info",
      "line": "[2024-07-04T12:33:54Z] INFO Request completed in 42ms request_id=abc duration_ms=42"
    },
    {
      "ts": 1720000241000,
      "service": "project-web",
      "cid": "91bc832df407",
      "stream": "stderr",
      "level": "error",
      "line": "[2024-07-04T12:34:01Z] ERROR Timeout while connecting to upstream"
    }
  ],
  "older_cursor": "eyJ0cyI6MTcyMDAwMDIzNDU2Nywid2VlayI6IjIwMjQtVzI3IiwiaWQiOjEyMzQ1fQ==",
  "newer_cursor": "eyJ0cyI6MTcyMDAwMDI0MTAwMCwid2VlayI6IjIwMjQtVzI3IiwiaWQiOjEyMzQ2fQ==",
  "has_older": true,
  "has_newer": false
}
```

Notes:
- `items` contains log entries with `service`, `cid`, `stream`, `level`, and `line` values
- if `has_older` is `true`, the request can be repeated with `before=older_cursor` to fetch the next older page
- `newer_cursor` can be used with `after=newer_cursor` for live polling even when `has_newer` is `false`; the response may contain an empty `items` array when no newer logs exist yet
- if an `after` request returns no new items, `newer_cursor` repeats the submitted `after` cursor
- if an initial request returns no items, `older_cursor` and `newer_cursor` are `null`, and both `has_older` and `has_newer` are `false`
- invalid `level` or `stream` values return `400 Bad Request`
- invalid or malformed cursors return `400 Bad Request`

---

## 11) Server-Sent Events (streaming)

Every `/api/stream/*` endpoint returns `text/event-stream` and is a push-based alternative to a corresponding plain `GET` endpoint documented above, grouped here together rather than alongside their REST counterparts. Shared conventions across all of them:
- clients SHOULD rely on the browser's native `EventSource` reconnect behavior rather than implementing their own retry loop
- connections send periodic keep-alive comments to detect dead connections and avoid idle timeouts
- reconnecting always restarts with a fresh baseline event before further incremental events resume — the server does not remember a disconnected client's state
- named SSE `event:` fields distinguish payload kinds (e.g. `snapshot` vs `diff`/`append`) instead of overloading a single unnamed `message` event

### GET `/api/stream/metrics/current`

Push equivalent of `GET /api/metrics/current` for clients that want live updates without polling. Host and container metrics are collected by independent tasks at their own rates, so each is pushed as its own named event rather than as one combined payload. Staleness rules are unchanged and applied per half.

Parameters: none.

Events:
- `snapshot` — sent once, immediately on connect. Payload is a full `MetricsSnapshot`, identical in shape to `GET /api/metrics/current`.
- `host` — sent whenever host metrics are recorded. Payload is a `HostPoint`, or `null` if the latest sample is stale.
- `containers` — sent whenever a container sample batch is recorded. Payload is the `containers` and `services` maps.

Clients merge `host` and `containers` events into the baseline `snapshot` to maintain a full `MetricsSnapshot`.

Example:
```http
GET /api/stream/metrics/current
Accept: text/event-stream
```

Example events:
```
event: snapshot
data: {"host":{"ts":1720003600000,"cpu_pct":21.4,"mem_used":2147483648,"mem_total":8589934592,"storage_used":104857600,"storage_total":536870912,"metrics_size":5242880,"logs_size":73400320,"net_rx_rate":15432.0,"net_tx_rate":9650.0,"disk_read_rate":2000.0,"disk_write_rate":1500.0},"containers":{},"services":{}}

event: host
data: {"ts":1720003610000,"cpu_pct":24.9,"mem_used":2147483648,"mem_total":8589934592,"storage_used":104857600,"storage_total":536870912,"metrics_size":5242880,"logs_size":73400320,"net_rx_rate":15432.0,"net_tx_rate":9650.0,"disk_read_rate":2000.0,"disk_write_rate":1500.0}

event: containers
data: {"containers":{},"services":{}}

```

### GET `/api/stream/containers`

Diff-based push equivalent of `GET /api/containers`. Emits two kinds of named events instead of repeatedly sending the full list:

- `snapshot` — sent once, immediately on connect. Payload is the full array, identical in shape to `GET /api/containers`'s response.
- `diff` — sent whenever the container list actually changes after that. Payload:
  ```json
  {
    "added": [ /* ContainerSummary, container ids not seen before */ ],
    "updated": [ /* ContainerSummary, whole record for any id whose fields changed */ ],
    "removed": [ /* container id strings no longer present */ ]
  }
  ```

Parameters: none.

Behavior:
- `updated` entries are whole replacement records, not per-field patches — clients should overwrite their local copy of that container id wholesale
- no event is emitted at all when a periodic refresh detects no actual change; clients should not expect a steady heartbeat of data (aside from keep-alive comments) while nothing changes
- each connection tracks its own diff baseline independently

Example:
```http
GET /api/stream/containers
Accept: text/event-stream
```

Example events:
```
event: snapshot
data: [{"id":"8af7d6c1273d","name":"web-1","service":"project-web","image":"nginx:latest","image_sha":"sha256:abcd...","ports":["80:80"],"labels":[],"state":"running","started_at":1720003600000}]

event: diff
data: {"added":[],"updated":[{"id":"8af7d6c1273d","name":"web-1","service":"project-web","image":"nginx:latest","image_sha":"sha256:abcd...","ports":["80:80"],"labels":[],"state":"exited","started_at":1720003600000}],"removed":[]}

```

### GET `/api/stream/metrics/host?from={ts}`

Push equivalent of `GET /api/metrics/host`, parameterized by `from` — each connection tracks its own range and cursor. There is no `to`: a live stream always runs up to the server's own "now" at connect time and keeps appending indefinitely afterward, so a client-supplied `to` would be redundant and a possible source of clock-skew bugs.

Behavior:
- on connect, computes `resolution` from `from` to the server's current time, then emits one `snapshot` event with the exact same payload as `GET /api/metrics/host?from={ts}&to={now}` would return
- afterward, emits one `append` event per newly-completed bucket at the selected `resolution` — payload is a single `HostPoint` (not an array), the new bucket's cross-section
- a bucket with no host sample in it produces no event at all
- `resolution` bounds how often `append` can occur: at most once per bucket boundary for that resolution (e.g. up to once every 10 seconds at `10s`, once per hour at `1h`)
- intended for **live/rolling windows only** (see "Metrics bucket semantics" above); for a fixed historical range, use the plain `GET /api/metrics/host` endpoint instead — the client is responsible for evicting points that fall outside its own sliding window as time advances, since the server does not re-enforce `from` after the initial snapshot

Parameters:
- `from` (required): start time in ms

Example:
```http
GET /api/stream/metrics/host?from=1720000000000
Accept: text/event-stream
```

Example events:
```
event: snapshot
data: {"resolution":"10s","data":[{"ts":1720000000000,"cpu_pct":21.4,"mem_used":2147483648,"mem_total":8589934592,"storage_used":104857600,"storage_total":536870912,"metrics_size":5242880,"logs_size":73400320,"net_rx_rate":15432.0,"net_tx_rate":9650.0,"disk_read_rate":2000.0,"disk_write_rate":1500.0}]}

event: append
data: {"ts":1720000010000,"cpu_pct":22.1,"mem_used":2148000000,"mem_total":8589934592,"storage_used":104857600,"storage_total":536870912,"metrics_size":5242880,"logs_size":73400320,"net_rx_rate":15900.0,"net_tx_rate":9700.0,"disk_read_rate":2100.0,"disk_write_rate":1600.0}

```

### GET `/api/stream/metrics/containers?from={ts}`

Push equivalent of `GET /api/metrics/containers` (aggregate per `service`); same parameterization (no `to`, see above) and bucket-append behavior as `/api/stream/metrics/host`.

Behavior:
- `snapshot` payload shape matches `GET /api/metrics/containers` exactly (`{ resolution, data }`, where `data` is keyed by `service`, each an array of `GroupPoint`)
- `append` payload is an object keyed by `service`, but each value is a single `GroupPoint` (the new bucket's cross-section), not an array — only `service`s with data in that bucket are included
- a `service` that first appears mid-window can show up as a new key in a later `append` event; clients should treat first sight of a key as starting a new series

Parameters:
- `from` (required): start time in ms

Example events:
```
event: snapshot
data: {"resolution":"10s","data":{"project-web":[{"ts":1720000000000,"cpu_pct":20.4,"mem_used":704643072,"mem_limit":2147483648,"net_rx_rate":17500.0,"net_tx_rate":12600.0,"blk_read_rate":8800.0,"blk_write_rate":7400.0}]}}

event: append
data: {"project-web":{"ts":1720000010000,"cpu_pct":21.0,"mem_used":705000000,"mem_limit":2147483648,"net_rx_rate":17800.0,"net_tx_rate":12700.0,"blk_read_rate":8900.0,"blk_write_rate":7500.0}}

```

### GET `/api/stream/metrics/containers/{service}?from={ts}`

Push equivalent of `GET /api/metrics/containers/{service}`; same parameterization (no `to`, see above) and bucket-append behavior as the other metrics streams above.

Behavior:
- `snapshot` payload shape matches `GET /api/metrics/containers/{service}` exactly (`{ resolution, data }`, where `data` is `{ sum: GroupPoint[], containers: { [id]: ContainerPoint[] } }`)
- `append` payload is `{ sum: GroupPoint | null, containers: { [id]: ContainerPoint } }` — a single cross-section for the newly-completed bucket; an append with `sum: null` and empty `containers` is never sent (skipped instead)
- a container that starts mid-window appears as a new key under `containers` in a later `append` event

Parameters:
- `service` (path): `service` from `/api/containers`
- `from` (required): start time in ms

Example:
```http
GET /api/stream/metrics/containers/project-web?from=1720000000000
Accept: text/event-stream
```

Example events:
```
event: snapshot
data: {"resolution":"10s","data":{"sum":[{"ts":1720000000000,"cpu_pct":20.4,"mem_used":704643072,"mem_limit":2147483648,"net_rx_rate":17500.0,"net_tx_rate":12600.0,"blk_read_rate":8800.0,"blk_write_rate":7400.0}],"containers":{"8af7d6c1273d":[{"ts":1720000000000,"service":"project-web","cpu_pct":12.8,"mem_used":402653184,"mem_limit":1073741824,"net_rx_rate":10500.0,"net_tx_rate":7300.0,"blk_read_rate":5200.0,"blk_write_rate":4100.0}]}}}

event: append
data: {"sum":{"ts":1720000010000,"cpu_pct":21.0,"mem_used":705000000,"mem_limit":2147483648,"net_rx_rate":17800.0,"net_tx_rate":12700.0,"blk_read_rate":8900.0,"blk_write_rate":7500.0},"containers":{"8af7d6c1273d":{"ts":1720000010000,"service":"project-web","cpu_pct":13.0,"mem_used":403000000,"mem_limit":1073741824,"net_rx_rate":10600.0,"net_tx_rate":7350.0,"blk_read_rate":5250.0,"blk_write_rate":4150.0}}}

```

### GET `/api/stream/logs`

Diff-based push equivalent of `GET /api/logs`. Reacts to two independent change sources: a new log line being flushed (moves `last_received`) and a container starting/stopping (flips `live`). Emits the same two named events as `/api/stream/containers`:

- `snapshot` — sent once, immediately on connect. Payload identical in shape to `GET /api/logs`'s response.
- `diff` — sent whenever any service's status actually changes. Payload:
  ```json
  {
    "added": { "service": "ServiceStatus" },
    "updated": { "service": "ServiceStatus" },
    "removed": [ "service" ]
  }
  ```

Parameters: none.

Behavior:
- `updated` entries are whole replacement records for that `service`, not per-field patches
- no event is emitted at all when nothing actually changed
- each connection tracks its own diff baseline independently

Example:
```http
GET /api/stream/logs
Accept: text/event-stream
```

Example events:
```
event: snapshot
data: {"project-web":{"last_received":1720003550000,"live":false},"system-nginx":{"last_received":null,"live":false}}

event: diff
data: {"added":{},"updated":{"project-web":{"last_received":1720003600000,"live":true}},"removed":[]}

```

### GET `/api/stream/logs/{service}?q={text}&level={lvl}&stream={s}&after={token}`

Filter-aware, forward-only push equivalent of the tailing use of `GET /api/logs/{service}` (polling with `after`). This endpoint only replaces tailing — the REST endpoint keeps its full bidirectional cursor-pagination role for initial page loads and scrolling up/down through history.

Behavior:
- no `snapshot` event on connect — the connection performs an immediate first check against the given `after` cursor (self-healing any gap between a client's last REST page load and this connection opening), then waits for further log flushes
- `append` is emitted once per matching batch of newly-flushed lines, never on an empty result:
  ```json
  {
    "items": [ "LogLine" ],
    "newer_cursor": "string | null"
  }
  ```
- `newer_cursor` lets clients keep their own pagination cursor consistent with what the stream has already delivered, the same way `newer_cursor` works on `GET /api/logs/{service}`
- filters (`q`, `level`, `stream`) apply exactly as they do on the REST endpoint; there is no `from`, `to`, `before`, or `limit` — not meaningful for a forward-only tail

Parameters:
- `service` (path): service
- `q` (optional): text search filter against `line`; default: none
- `level` (optional): comma-separated log levels; default: no filtering
- `stream` (optional): comma-separated streams; default: no filtering
- `after` (optional): opaque cursor to resume from; default: none (tail-only from connect time)

Example:
```http
GET /api/stream/logs/project-web?after=eyJ0cyI6MTcyMDAwMDI0MTAwMCwid2VlayI6IjIwMjQtVzI3IiwiaWQiOjEyMzQ2fQ%3D%3D
Accept: text/event-stream
```

Example event:
```
event: append
data: {"items":[{"ts":1720003600000,"service":"project-web","cid":"8af7d6c1273d","stream":"stdout","level":"info","line":"[2024-07-04T12:33:54Z] INFO Request completed in 42ms"}],"newer_cursor":"eyJ0cyI6MTcyMDAwMzYwMDAwMCwid2VlayI6IjIwMjQtVzI3IiwiaWQiOjEyMzQ3fQ=="}

```

---

## 12) Types and contracts

### `HealthResponse`
```json
{
  "ok": true,
  "app": "string",
  "version": "string",
  "port": 8080,
  "sample_interval_ms": 10000,
  "retention_weeks": 12,
  "docker_controls_available": true
}
```

### `UiConfig`
```json
{
  "name": "string",
  "eyebrow": "string",
  "links": [
    {
      "icon": "string",
      "label": "string",
      "url": "string"
    }
  ]
}
```

### `ContainerSummary`
```json
{
  "id": "string",
  "name": "string",
  "service": "string",
  "image": "string",
  "image_sha": "string",
  "ports": ["string"],
  "labels": ["string"],
  "state": "created | restarting | running | removing | paused | exited | dead",
  "started_at": null
}
```

### `ContainerDiff`
```json
{
  "added": ["ContainerSummary"],
  "updated": ["ContainerSummary"],
  "removed": ["string"]
}
```

### `HostPoint`
```json
{
  "ts": 1234567890123,
  "cpu_pct": 12.5,
  "mem_used": 123456789,
  "mem_total": 456789123,
  "storage_used": 123456789,
  "storage_total": 456789123,
  "metrics_size": 5242880,
  "logs_size": 73400320,
  "net_rx_rate": 1000.0,
  "net_tx_rate": 2000.0,
  "disk_read_rate": 3000.0,
  "disk_write_rate": 4000.0
}
```

### `ContainerPoint`
```json
{
  "ts": 1234567890123,
  "service": "string",
  "cpu_pct": 12.5,
  "mem_used": 123456789,
  "mem_limit": 456789123,
  "net_rx_rate": 1000.0,
  "net_tx_rate": 2000.0,
  "blk_read_rate": 3000.0,
  "blk_write_rate": 4000.0
}
```

### `GroupPoint`
```json
{
  "ts": 1234567890123,
  "cpu_pct": 12.5,
  "mem_used": 123456789,
  "mem_limit": 456789123,
  "net_rx_rate": 1000.0,
  "net_tx_rate": 2000.0,
  "blk_read_rate": 3000.0,
  "blk_write_rate": 4000.0
}
```

### `ContainerGroupMetrics`
```json
{
  "sum": ["GroupPoint"],
  "containers": { "container_id": ["ContainerPoint"] }
}
```

### `ContainerGroupMetricsAppend`
```json
{
  "sum": "GroupPoint | null",
  "containers": { "container_id": "ContainerPoint" }
}
```

### `ContainerMetricsByService`
```json
{
  "service": ["GroupPoint"]
}
```

### `MetricsResponse<T>`
```json
{
  "resolution": "10s | 1m | 5m | 1h",
  "data": "T"
}
```

### `ServiceStatus`
```json
{
  "last_received": "number | null",
  "live": true
}
```

### `ServiceDiff`
```json
{
  "added": { "service": "ServiceStatus" },
  "updated": { "service": "ServiceStatus" },
  "removed": ["string"]
}
```

### `LogTailAppend`
```json
{
  "items": ["LogLine"],
  "newer_cursor": "string | null"
}
```

### `MetricsSnapshot`
```json
{
  "host": "HostPoint | null",
  "containers": { "container_id": "ContainerPoint" },
  "services": { "service": "GroupPoint" }
}
```

### `LogLine`
```json
{
  "ts": 1234567890123,
  "service": "string",
  "cid": "string",
  "stream": "stdout | stderr",
  "level": "debug | info | warn | error | null",
  "line": "string"
}
```

### `LogPage`
```json
{
  "items": [
    {
      "ts": 1234567890123,
      "service": "string",
      "cid": "string",
      "stream": "stdout",
      "level": "info",
      "line": "string"
    }
  ],
  "older_cursor": "string | null",
  "newer_cursor": "string | null",
  "has_older": true,
  "has_newer": false
}
```

---

## 13) Route summary

| Endpoint                                   | Method | Description                                                             |
| ------------------------------------------ | ------ | ----------------------------------------------------------------------- |
| `/api/health`                              | GET    | Health check                                                            |
| `/api/config/ui`                           | GET    | UI configuration and custom links                                       |
| `/api/config/settings`                     | GET    | Supported environment variables with effective and default values       |
| `/api/config/computed`                     | GET    | Runtime-computed backend values                                         |
| `/api/containers`                          | GET    | List containers and details                                             |
| `/api/containers/{id}/start`               | POST   | Start container                                                         |
| `/api/containers/{id}/stop`                | POST   | Stop container                                                          |
| `/api/containers/{id}/restart`             | POST   | Restart container                                                       |
| `/api/metrics/host`                        | GET    | Host metrics                                                            |
| `/api/metrics/current`                     | GET    | Latest host, per container and per service values with rates            |
| `/api/metrics/containers/{service}`        | GET    | Container metrics (sum + per container id)                              |
| `/api/metrics/containers`                  | GET    | Aggregate container metrics (sum per service)                           |
| `/api/logs`                                | GET    | List services                                                           |
| `/api/logs/{service}`                      | GET    | Query logs                                                              |
| `/api/stream/metrics/current`              | GET    | SSE push equivalent of `/api/metrics/current`                           |
| `/api/stream/containers`                   | GET    | SSE diff-based push equivalent of `/api/containers`                     |
| `/api/stream/metrics/host`                 | GET    | SSE append-based push equivalent of `/api/metrics/host`                 |
| `/api/stream/metrics/containers`           | GET    | SSE append-based push equivalent of `/api/metrics/containers`           |
| `/api/stream/metrics/containers/{service}` | GET    | SSE append-based push equivalent of `/api/metrics/containers/{service}` |
| `/api/stream/logs`                         | GET    | SSE diff-based push equivalent of `/api/logs`                           |
| `/api/stream/logs/{service}`               | GET    | SSE forward-tailing push equivalent of polling `/api/logs/{service}`    |
