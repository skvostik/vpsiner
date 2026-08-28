# API overview

This project exposes a REST API under the `/api` prefix.

General conventions:
- all routes start with `/api`
- timestamps are in Unix epoch milliseconds (`i64`)
- `from` must be less than or equal to `to`
- `POST` actions return `204 No Content` on success
- invalid requests return `400 Bad Request` unless another status is specified

### Metrics bucket semantics

Applies to all three metrics endpoints (`/api/metrics/host`, `/api/metrics/containers/{log_group}`, `/api/metrics/containers`):

- `resolution` is a **required** query parameter; allowed values are `10s`, `1m`, `5m`, and `1h`. There is no un-downsampled/"raw" mode — `10s` is simply the finest fixed bucket size and follows the same rules as the others. Missing or invalid `resolution` returns `400 Bad Request`.
- Samples are grouped into half-open, epoch-aligned buckets: `(bucket_end - resolution, bucket_end]`, where `bucket_end = ceil(ts_ms / bucket_size_ms) * bucket_size_ms`. Bucket boundaries are anchored to the Unix epoch, not to the request's `from`. For example, `10s` buckets end on `:00, :10, :20, :30, :40`, or `:50` seconds of each minute; `1m` buckets end on each minute; `1h` buckets end on each hour.
- Returned `ts` is the bucket end.
- Only fully elapsed buckets (`ts <= now` at request time) are returned. An open trailing bucket is omitted, even if `to` extends past it.
- Empty buckets are omitted.
- Gauge metrics (e.g. `cpu_pct`, `mem_used`, `mem_total`, `storage_used`, `storage_total`, `metrics_size`, `logs_size`, `mem_limit`) are averaged across samples within a bucket.
- Cumulative counters (e.g. `net_rx`, `net_tx`, `disk_read`, `disk_write`, `blk_read`, `blk_write`) use the last observed sample within a bucket.

## 1) Health check

### GET `/api/health`

Returns API availability and runtime metadata.

Parameters: none

Example response:
```json
{
  "ok": true,
  "service": "vpsiner",
  "version": "0.1.0",
  "port": 8080,
  "sample_interval_ms": 10000,
  "retention_weeks": 12,
  "docker_controls_available": true
}
```

`sample_interval_ms` is the metrics sampling interval; clients should not poll metrics endpoints more often than this.

`version` is the service version string.

`retention_weeks` is the configured metrics and logs retention window in weeks.

`docker_controls_available` indicates whether the `/api/containers/{id}/start|stop|restart` endpoints are currently available. Clients should poll `/api/health` and show/hide container lifecycle controls based on this flag. This is enforced server-side: while `docker_controls_available` is `false`, action endpoints return `403 Forbidden`.

---

## 2) List containers

### GET `/api/containers`

Returns containers and their `log_group` values.

`log_group` resolution precedence:
- if container label `vpsiner.log_group` exists and is non-empty, it is used
- otherwise, if both Docker Compose labels `com.docker.compose.project` and `com.docker.compose.service` exist and are non-empty, `log_group` is `{project}-{service}`
- otherwise, `log_group` falls back to the container name without a leading slash

Parameters: none

Example response:
```json
[
  {
    "id": "8af7d6c1273d",
    "name": "web-1",
    "log_group": "project-web",
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
    "log_group": "project-db",
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

## 3) Container management

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

## 4) Host metrics

### GET `/api/metrics/host?from={ts}&to={ts}&resolution={r}`

Returns a time series of host metrics.

Metric semantics:
- `cpu_pct` is total host CPU utilization during the sample interval, expressed as a percentage of total logical CPU capacity
- `cpu_pct` uses a `0..100` scale, where `100` means all logical CPUs are fully utilized
- `mem_used`, `mem_total`, `storage_used`, `storage_total`, `metrics_size`, `logs_size`, `net_rx`, `net_tx`, `disk_read`, and `disk_write` are byte values
- `metrics_size` is the on-disk size of Vpsiner's metrics database at sample time
- `logs_size` is the combined on-disk size of Vpsiner's log databases at sample time
- `net_rx`, `net_tx`, `disk_read`, and `disk_write` are cumulative counters

Ordering:
- samples MUST be returned in ascending timestamp order: `ts ASC`

Downsampling: see "Metrics bucket semantics" above.

Default values:
- `from`, `to`, and `resolution` are required

Query parameters:
- `from` (required): start time in ms
- `to` (required): end time in ms
- `resolution` (required): returned sample resolution; allowed values: `10s`, `1m`, `5m`, `1h`

Example:
```http
GET /api/metrics/host?from=1720000000000&to=1720003600000&resolution=10s
```

Example response:
```json
[
  {
    "ts": 1720000000000,
    "cpu_pct": 21.4,
    "mem_used": 2147483648,
    "mem_total": 8589934592,
    "storage_used": 104857600,
    "storage_total": 536870912,
    "metrics_size": 5242880,
    "logs_size": 73400320,
    "net_rx": 1543200,
    "net_tx": 965000,
    "disk_read": 2000000,
    "disk_write": 1500000
  }
]
```

---

## 5) Metrics for a specific log group

### GET `/api/metrics/containers/{log_group}?from={ts}&to={ts}&resolution={r}`

Returns container metrics for a single `log_group`.

The response contains:
- `sum`: one time series aggregated across all container IDs in this `log_group` at each timestamp
- `containers`: individual time series keyed by container ID, for viewing each sampled container separately

`sum` aggregates the metric values for all container samples in the `log_group` that share a timestamp.

Metric semantics:
- `cpu_pct` is the container CPU utilization during the sample interval, expressed as a percentage of total host logical CPU capacity
- a container using one full logical CPU on a host with `N` logical CPUs contributes approximately `100 / N` to `cpu_pct`
- aggregated `cpu_pct` values are summed by timestamp, so group CPU usage is comparable to host CPU usage and normally does not exceed `100`, aside from sampling jitter
- `mem_used`, `mem_limit`, `net_rx`, `net_tx`, `blk_read`, and `blk_write` are byte values
- `net_rx`, `net_tx`, `blk_read`, and `blk_write` are cumulative counters

Aggregation rules:
- `sum` is computed by grouping container samples by exact `ts` and summing all numeric metric fields for that timestamp
- a `sum` data point at timestamp `ts` includes only container samples with that timestamp
- missing container samples are not interpolated
- no synthetic zero-valued container samples are generated
- `containers` includes container IDs with metric samples for the requested `log_group` and time range

Ordering:
- `sum` samples MUST be returned in ascending timestamp order: `ts ASC`
- each array under `containers` MUST be returned in ascending timestamp order: `ts ASC`

Downsampling: see "Metrics bucket semantics" above. `sum` is calculated from each container's downsampled series by bucket timestamp.

Default values:
- `from`, `to`, and `resolution` are required

Parameters:
- `log_group` (path): `log_group` from `/api/containers`
- `from` (query): start time in ms
- `to` (query): end time in ms
- `resolution` (query, required): returned sample resolution; allowed values: `10s`, `1m`, `5m`, `1h`

Example:
```http
GET /api/metrics/containers/project-web?from=1720000000000&to=1720003600000&resolution=10s
```

Example response:
```json
{
  "sum": [
    {
      "ts": 1720000000000,
      "log_group": "project-web",
      "cpu_pct": 20.4,
      "mem_used": 704643072,
      "mem_limit": 2147483648,
      "net_rx": 175000,
      "net_tx": 126000,
      "blk_read": 88000,
      "blk_write": 74000
    }
  ],
  "containers": {
    "8af7d6c1273d": [
      {
        "ts": 1720000000000,
        "log_group": "project-web",
        "cid": "8af7d6c1273d",
        "cpu_pct": 12.8,
        "mem_used": 402653184,
        "mem_limit": 1073741824,
        "net_rx": 105000,
        "net_tx": 73000,
        "blk_read": 52000,
        "blk_write": 41000
      }
    ],
    "91bc832df407": [
      {
        "ts": 1720000000000,
        "log_group": "project-web",
        "cid": "91bc832df407",
        "cpu_pct": 7.6,
        "mem_used": 301989888,
        "mem_limit": 1073741824,
        "net_rx": 70000,
        "net_tx": 53000,
        "blk_read": 36000,
        "blk_write": 33000
      }
    ]
  }
}
```

Notes:
- the keys under `containers` are container IDs
- `sum` data points do not include `cid`, because they represent the whole `log_group`
- `containers` data points include `cid`, because they represent a specific container ID

---

## 6) Aggregate metrics for all container log groups

### GET `/api/metrics/containers?from={ts}&to={ts}&resolution={r}`

Returns aggregate container metrics for all `log_group` values during the given interval.

The response is keyed by `log_group`. Each value is the aggregated time series for that group, using the same summing rules as the `sum` field from `/api/metrics/containers/{log_group}`.

Metric semantics:
- `cpu_pct` uses the same host-normalized scale as `HostSample.cpu_pct`
- aggregated `cpu_pct` values are summed by timestamp and are comparable to host CPU usage
- values use the units defined by `ContainerSample`
- `net_rx`, `net_tx`, `blk_read`, and `blk_write` are cumulative counters

Aggregation rules:
- each `log_group` time series is computed by grouping container samples by exact `log_group` and `ts`, then summing all numeric metric fields
- a group data point at timestamp `ts` includes only container samples with that timestamp
- missing container samples are not interpolated
- no synthetic zero-valued container samples are generated

Ordering:
- each `log_group` time series MUST be returned in ascending timestamp order: `ts ASC`

Downsampling: see "Metrics bucket semantics" above. Each group series is calculated from downsampled container series by bucket timestamp.

Default values:
- `from`, `to`, and `resolution` are required

Query parameters:
- `from` (required): start time in ms
- `to` (required): end time in ms
- `resolution` (required): returned sample resolution; allowed values: `10s`, `1m`, `5m`, `1h`

Example:
```http
GET /api/metrics/containers?from=1720000000000&to=1720003600000&resolution=10s
```

Example response:
```json
{
  "project-web": [
    {
      "ts": 1720000000000,
      "log_group": "project-web",
      "cpu_pct": 20.4,
      "mem_used": 704643072,
      "mem_limit": 2147483648,
      "net_rx": 175000,
      "net_tx": 126000,
      "blk_read": 88000,
      "blk_write": 74000
    }
  ],
  "project-db": [
    {
      "ts": 1720000000000,
      "log_group": "project-db",
      "cpu_pct": 5.2,
      "mem_used": 671088640,
      "mem_limit": 1073741824,
      "net_rx": 33000,
      "net_tx": 22000,
      "blk_read": 78000,
      "blk_write": 93000
    }
  ]
}
```

---

## 7) List log groups

### GET `/api/logs`

Returns known `log_group` values with the timestamp of the newest log line and current live status. Groups known only from Docker are included even when no logs have been stored yet.

Parameters: none

Ordering:
- object keys are emitted in ascending `log_group` order for stable responses

Notes:
- `last_received` is `null` when the group has no log lines
- `last_received` is the greatest `ts` in the group
- `live` is `true` when at least one container in the group is currently running

Example response:
```json
{
  "project-db": { "last_received": 1720003600000, "live": true },
  "project-web": { "last_received": 1720003550000, "live": false },
  "system-nginx": { "last_received": null, "live": false }
}
```

---

## 8) Query logs

### GET `/api/logs/{log_group}?from={ts}&to={ts}&q={text}&level={lvl}&stream={s}&limit={n}&before={token}&after={token}`

Returns paginated logs for the given group.

Each log line includes its `log_group`, source container ID, and a single text field (`line`) with ANSI/VT100 color escape sequences removed. `q` text search matches against `line`. The backend does not parse any other structured fields out of the text — that is left up to clients.

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
- cursors do not encode filter values; the same cursor may be used with different `from`, `to`, `q`, `level`, and `stream` filters, and those filters are applied to records before or after the cursor boundary
- clients SHOULD discard cursors when changing filters if they want to restart pagination for the new result set
- cursors are intended for the same `log_group`; clients SHOULD NOT reuse a cursor from one `log_group` with another `log_group`

Default values for this endpoint:
- `level`: no filtering
- `stream`: no filtering
- `limit`: `100`
- `from` / `to`: no default bounds
- `q` / `before` / `after`: no default values

Parameters:
- `log_group` (path): log group
- `from` (optional): start filter time in ms; default: none (no lower bound)
- `to` (optional): end filter time in ms; default: none (no upper bound)
- `q` (optional): text search query; default: none
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
      "log_group": "project-web",
      "cid": "8af7d6c1273d",
      "stream": "stdout",
      "level": "info",
      "line": "[2024-07-04T12:33:54Z] INFO Request completed in 42ms request_id=abc duration_ms=42"
    },
    {
      "ts": 1720000241000,
      "log_group": "project-web",
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
- `items` contains log entries with `log_group`, `cid`, `stream`, `level`, and `line` values
- if `has_older` is `true`, the request can be repeated with `before=older_cursor` to fetch the next older page
- `newer_cursor` can be used with `after=newer_cursor` for live polling even when `has_newer` is `false`; the response may contain an empty `items` array when no newer logs exist yet
- if an `after` request returns no new items, `newer_cursor` repeats the submitted `after` cursor
- if an initial request returns no items, `older_cursor` and `newer_cursor` are `null`, and both `has_older` and `has_newer` are `false`
- invalid `level` or `stream` values return `400 Bad Request`
- invalid or malformed cursors return `400 Bad Request`

---

## 9) Types and contracts

### `HealthResponse`
```json
{
  "ok": true,
  "service": "string",
  "version": "string",
  "port": 8080,
  "sample_interval_ms": 10000,
  "retention_weeks": 12,
  "docker_controls_available": true
}
```

### `ContainerSummary`
```json
{
  "id": "string",
  "name": "string",
  "log_group": "string",
  "image": "string",
  "image_sha": "string",
  "ports": ["string"],
  "labels": ["string"],
  "state": "created | restarting | running | removing | paused | exited | dead",
  "started_at": null
}
```

### `HostSample`
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
  "net_rx": 1000,
  "net_tx": 2000,
  "disk_read": 3000,
  "disk_write": 4000
}
```

### `ContainerSample`
```json
{
  "ts": 1234567890123,
  "log_group": "string",
  "cid": "string",
  "cpu_pct": 12.5,
  "mem_used": 123456789,
  "mem_limit": 456789123,
  "net_rx": 1000,
  "net_tx": 2000,
  "blk_read": 3000,
  "blk_write": 4000
}
```

### `ContainerGroupSample`
```json
{
  "ts": 1234567890123,
  "log_group": "string",
  "cpu_pct": 12.5,
  "mem_used": 123456789,
  "mem_limit": 456789123,
  "net_rx": 1000,
  "net_tx": 2000,
  "blk_read": 3000,
  "blk_write": 4000
}
```

### `ContainerGroupMetrics`
```json
{
  "sum": [
    {
      "ts": 1234567890123,
      "log_group": "string",
      "cpu_pct": 12.5,
      "mem_used": 123456789,
      "mem_limit": 456789123,
      "net_rx": 1000,
      "net_tx": 2000,
      "blk_read": 3000,
      "blk_write": 4000
    }
  ],
  "containers": {
    "container_id": [
      {
        "ts": 1234567890123,
        "log_group": "string",
        "cid": "string",
        "cpu_pct": 12.5,
        "mem_used": 123456789,
        "mem_limit": 456789123,
        "net_rx": 1000,
        "net_tx": 2000,
        "blk_read": 3000,
        "blk_write": 4000
      }
    ]
  }
}
```

### `ContainerMetricsByLogGroup`
```json
{
  "log_group": [
    {
      "ts": 1234567890123,
      "log_group": "string",
      "cpu_pct": 12.5,
      "mem_used": 123456789,
      "mem_limit": 456789123,
      "net_rx": 1000,
      "net_tx": 2000,
      "blk_read": 3000,
      "blk_write": 4000
    }
  ]
}
```

### `LogLine`
```json
{
  "ts": 1234567890123,
  "log_group": "string",
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
      "log_group": "string",
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

## 10) Route summary

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/health` | GET | Health check |
| `/api/containers` | GET | List containers and details |
| `/api/containers/{id}/start` | POST | Start container |
| `/api/containers/{id}/stop` | POST | Stop container |
| `/api/containers/{id}/restart` | POST | Restart container |
| `/api/metrics/host` | GET | Host metrics |
| `/api/metrics/containers/{log_group}` | GET | Container metrics (sum + per container id) |
| `/api/metrics/containers` | GET | Aggregate container metrics (sum per log_group) |
| `/api/logs` | GET | List log groups |
| `/api/logs/{log_group}` | GET | Query logs |
