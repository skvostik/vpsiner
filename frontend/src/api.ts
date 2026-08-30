import { reportBackendUnreachable } from './composables/useBackendHealth'
import type {
  ContainerGroupMetrics,
  ContainerMetricsByLogGroup,
  ContainerSummary,
  HostSample,
  LogGroups,
  LogPage,
  LogQueryParams,
  MetricsResolution,
  MetricsSnapshot,
  TimeRange,
} from './types'

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  let response: Response
  try {
    response = await fetch(path, options)
  } catch (networkError) {
    reportBackendUnreachable()
    throw networkError
  }
  if (response.status === 502 || response.status === 503 || response.status === 504)
    reportBackendUnreachable()
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`)
  return response.status === 204 ? (undefined as T) : response.json()
}

function metricsRange(range: TimeRange, resolution: MetricsResolution) {
  return new URLSearchParams({
    from: String(range.from),
    to: String(range.to),
    resolution,
  }).toString()
}

function queryString(params: LogQueryParams) {
  const query = new URLSearchParams()
  Object.entries(params).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      if (value.length) query.set(key, value.join(','))
    } else if (value !== undefined && value !== '') {
      query.set(key, String(value))
    }
  })
  return query.toString()
}

export const api = {
  containers: {
    list: () => request<ContainerSummary[]>('/api/containers'),
    metrics: (logGroup: string, range: TimeRange, resolution: MetricsResolution) =>
      request<ContainerGroupMetrics>(
        `/api/metrics/containers/${encodeURIComponent(logGroup)}?${metricsRange(range, resolution)}`
      ),
    action: (id: string, action: 'start' | 'stop' | 'restart') =>
      request<void>(`/api/containers/${encodeURIComponent(id)}/${action}`, { method: 'POST' }),
  },
  host: {
    metrics: (range: TimeRange, resolution: MetricsResolution) =>
      request<HostSample[]>(`/api/metrics/host?${metricsRange(range, resolution)}`),
  },
  metrics: {
    current: () => request<MetricsSnapshot>('/api/metrics/current'),
  },
  logs: {
    groups: () => request<LogGroups>('/api/logs'),
    query: (group: string, params: LogQueryParams) =>
      request<LogPage>(`/api/logs/${encodeURIComponent(group)}?${queryString(params)}`),
  },
}

export function containerMetricsHistory(range: TimeRange, resolution: MetricsResolution) {
  return request<ContainerMetricsByLogGroup>(
    `/api/metrics/containers?${metricsRange(range, resolution)}`
  )
}
