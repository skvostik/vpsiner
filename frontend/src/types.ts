export type ContainerState =
  'created' | 'restarting' | 'running' | 'removing' | 'paused' | 'exited' | 'dead'

export interface ContainerSummary {
  id: string
  name: string
  service: string
  image: string
  image_sha: string
  ports: string[]
  labels: string[]
  state: ContainerState
  started_at: number | null
}

/** Incremental update pushed by GET /api/stream/containers, relative to what the client has already seen. */
export interface ContainerDiff {
  added: ContainerSummary[]
  updated: ContainerSummary[]
  removed: string[]
}

export interface HostPoint {
  ts: number
  cpu_pct: number
  mem_used: number
  mem_total: number
  storage_used: number
  storage_total: number
  metrics_size: number
  logs_size: number
  net_rx_rate: number
  net_tx_rate: number
  disk_read_rate: number
  disk_write_rate: number
}

export interface ContainerPoint {
  ts: number
  service: string
  cpu_pct: number
  mem_used: number
  mem_limit: number
  net_rx_rate: number
  net_tx_rate: number
  blk_read_rate: number
  blk_write_rate: number
}

export interface GroupPoint {
  ts: number
  cpu_pct: number
  mem_used: number
  mem_limit: number
  net_rx_rate: number
  net_tx_rate: number
  blk_read_rate: number
  blk_write_rate: number
}

export interface ContainerGroupMetrics {
  sum: GroupPoint[]
  containers: Record<string, ContainerPoint[]>
}

export type ContainerMetricsByService = Record<string, GroupPoint[]>

/** One newly-completed bucket's cross-section, pushed by GET /api/stream/metrics/containers/{service}. */
export interface ContainerGroupMetricsAppend {
  sum: GroupPoint | null
  containers: Record<string, ContainerPoint>
}

export interface MetricsSnapshot {
  host: HostPoint | null
  containers: Record<string, ContainerPoint>
  services: Record<string, GroupPoint>
}

export interface ContainerRow extends ContainerSummary {
  metrics?: ContainerPoint
}
export type LogStream = 'stdout' | 'stderr'
export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export type MetricsWindow = '10m' | '30m' | '1h' | '6h' | '24h' | '7d' | 'custom'
export type MetricsResolution = '10s' | '1m' | '5m' | '1h'

export interface TimeRange {
  from: number
  to: number
}

export interface LogLine {
  ts: number
  service: string
  cid: string
  stream: LogStream
  level: LogLevel | null
  line: string
}

export interface LogPage {
  items: LogLine[]
  older_cursor: string | null
  newer_cursor: string | null
  has_older: boolean
  has_newer: boolean
}

export interface ServiceStatus {
  last_received: number | null
  live: boolean
}

export type Services = Record<string, ServiceStatus>

/** Incremental update pushed by GET /api/stream/logs, relative to what the client has already seen. */
export interface ServiceDiff {
  added: Services
  updated: Services
  removed: string[]
}

/** One batch of newly-flushed lines pushed by GET /api/stream/logs/{service}. */
export interface LogTailAppend {
  items: LogLine[]
  newer_cursor: string | null
}

export interface LogQueryParams {
  from?: number
  to?: number
  q?: string
  level?: LogLevel[]
  stream?: LogStream[]
  limit?: number
  before?: string
  after?: string
}

export interface CustomLink {
  icon: string
  label: string
  url: string
}

export interface UiConfig {
  name?: string
  eyebrow?: string
  links?: CustomLink[]
}

export type SettingCategory = 'common' | 'advanced'

export interface SettingEntry {
  name: string
  value: string
  default: string
  description: string
  category: SettingCategory
  overridden: boolean
}

export interface ComputedEntry {
  name: string
  value: string
  description: string
}
