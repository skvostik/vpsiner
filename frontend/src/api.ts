import { reportBackendUnreachable } from './composables/useBackendHealth'
import type {
  ContainerGroupMetrics,
  ContainerMetricsByService,
  ContainerSummary,
  ComputedEntry,
  HostPoint,
  LogPage,
  LogQueryParams,
  MetricsResponse,
  SettingEntry,
  TimeRange,
  UiConfig,
} from './types'

type ErrorResponse = {
  error?: unknown
  code?: unknown
}

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
    public readonly code?: string
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

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
  if (!response.ok) {
    const body = (await response.json().catch(() => null)) as ErrorResponse | null
    const message =
      typeof body?.error === 'string' ? body.error : `${response.status} ${response.statusText}`
    const code = typeof body?.code === 'string' ? body.code : undefined
    throw new ApiError(response.status, message, code)
  }
  return response.status === 204 ? (undefined as T) : response.json()
}

function metricsRange(range: TimeRange) {
  return new URLSearchParams({
    from: String(range.from),
    to: String(range.to),
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
    metrics: (service: string, range: TimeRange) =>
      request<MetricsResponse<ContainerGroupMetrics>>(
        `/api/metrics/containers/${encodeURIComponent(service)}?${metricsRange(range)}`
      ),
    action: (id: string, action: 'start' | 'stop' | 'restart') =>
      request<void>(`/api/containers/${encodeURIComponent(id)}/${action}`, { method: 'POST' }),
  },
  host: {
    metrics: (range: TimeRange) =>
      request<MetricsResponse<HostPoint[]>>(`/api/metrics/host?${metricsRange(range)}`),
  },
  logs: {
    query: (service: string, params: LogQueryParams) =>
      request<LogPage>(`/api/logs/${encodeURIComponent(service)}?${queryString(params)}`),
  },
  config: {
    ui: () => request<UiConfig>('/api/config/ui'),
    settings: () => request<SettingEntry[]>('/api/config/settings'),
    computed: () => request<ComputedEntry[]>('/api/config/computed'),
  },
}

export function containerMetricsHistory(range: TimeRange) {
  return request<MetricsResponse<ContainerMetricsByService>>(
    `/api/metrics/containers?${metricsRange(range)}`
  )
}
