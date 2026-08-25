export type ContainerState =
  'created' | 'restarting' | 'running' | 'removing' | 'paused' | 'exited' | 'dead'

export interface ContainerSummary {
  id: string
  name: string
  log_group: string
  image: string
  image_sha: string
  ports: string[]
  labels: string[]
  state: ContainerState
  started_at: number | null
}

export interface HostSample {
  ts: number
  cpu_pct: number
  mem_used: number
  mem_total: number
  storage_used: number
  storage_total: number
  metrics_size: number
  logs_size: number
  net_rx: number
  net_tx: number
  disk_read: number
  disk_write: number
}

export interface ContainerSample {
  ts: number
  log_group: string
  cid: string
  cpu_pct: number
  mem_used: number
  mem_limit: number
  net_rx: number
  net_tx: number
  blk_read: number
  blk_write: number
}

export interface ContainerGroupSample {
  ts: number
  log_group: string
  cpu_pct: number
  mem_used: number
  mem_limit: number
  net_rx: number
  net_tx: number
  blk_read: number
  blk_write: number
}

export interface ContainerGroupMetrics {
  sum: ContainerGroupSample[]
  containers: Record<string, ContainerSample[]>
}

export type ContainerMetricsByLogGroup = Record<string, ContainerGroupSample[]>

export interface ContainerRow extends ContainerSummary {
  metrics?: ContainerOverviewMetrics
}

export interface ContainerOverviewMetrics {
  cpu_pct: number
  mem_used: number
  mem_limit: number
  net_rx_rate: number
  net_tx_rate: number
  disk_read_rate: number
  disk_write_rate: number
}
export type LogStream = 'stdout' | 'stderr'
export type LogLevel = 'debug' | 'info' | 'warn' | 'error'
export type LogWindow = '1h' | '6h' | '24h' | '7d' | '30d' | 'custom'

export type MetricsWindow = '10m' | '30m' | '1h' | '6h' | '24h' | '7d' | 'custom'
export type MetricsResolution = '10s' | '1m' | '5m' | '1h'

export interface TimeRange {
  from: number
  to: number
}

export interface LogLine {
  ts: number
  log_group: string
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

export interface LogGroupStatus {
  last_received: number | null
  live: boolean
}

export type LogGroups = Record<string, LogGroupStatus>

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
