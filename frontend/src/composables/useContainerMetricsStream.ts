import { onBeforeUnmount, ref, watch, type Ref } from 'vue'

import { reportSseIssue } from './useBackendHealth'
import type {
  ContainerGroupMetricsAppend,
  ContainerPoint,
  GroupPoint,
  MetricsResolution,
  MetricsResponse,
} from '../types'

const trimTickMs = 5_000
const reconnectDelayMs = 3_000

/** Live per-service history for the container detail view, pushed instead of polled. */
export function useContainerMetricsStream(
  service: Ref<string>,
  windowMs: Ref<number>,
  active: Ref<boolean>,
  setResolution: (resolution: MetricsResolution) => void
) {
  const sum = ref<GroupPoint[]>([])
  const containers = ref<Record<string, ContainerPoint[]>>({})

  let source: EventSource | undefined
  let trimTimer: number | undefined
  let reconnectTimer: number | undefined

  function trim() {
    if (!windowMs.value) return
    const cutoff = Date.now() - windowMs.value
    sum.value = sum.value.filter((point) => point.ts >= cutoff)
    for (const cid of Object.keys(containers.value)) {
      containers.value[cid] = containers.value[cid].filter((point) => point.ts >= cutoff)
    }
  }

  function disconnect() {
    source?.close()
    source = undefined
    if (trimTimer) window.clearInterval(trimTimer)
    trimTimer = undefined
    if (reconnectTimer) window.clearTimeout(reconnectTimer)
    reconnectTimer = undefined
  }

  function reconnect(currentSource: EventSource) {
    reportSseIssue(currentSource)
    currentSource.close()
    if (source === currentSource) source = undefined
    if (!active.value || !service.value || !windowMs.value || reconnectTimer) return
    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = undefined
      connect()
    }, reconnectDelayMs)
  }

  function connect() {
    disconnect()
    if (!active.value || !service.value || !windowMs.value) return

    const from = Date.now() - windowMs.value
    const params = new URLSearchParams({ from: String(from) })
    source = new EventSource(
      `/api/stream/metrics/containers/${encodeURIComponent(service.value)}?${params}`
    )
    source.addEventListener('snapshot', (event) => {
      const response = JSON.parse((event as MessageEvent).data) as MetricsResponse<{
        sum: GroupPoint[]
        containers: Record<string, ContainerPoint[]>
      }>
      const data = response.data
      sum.value = data.sum
      containers.value = data.containers
      trim()
      setResolution(response.resolution)
      console.debug('[container-metrics-stream] snapshot', response)
    })
    source.addEventListener('append', (event) => {
      const append = JSON.parse((event as MessageEvent).data) as ContainerGroupMetricsAppend
      if (append.sum) sum.value = [...sum.value, append.sum]
      for (const [cid, point] of Object.entries(append.containers)) {
        containers.value[cid] = [...(containers.value[cid] ?? []), point]
      }
      trim()
      console.debug('[container-metrics-stream] append', append)
    })
    source.onerror = () => {
      if (source) reconnect(source)
    }

    trimTimer = window.setInterval(trim, trimTickMs)
  }

  watch([service, windowMs, active], connect, { immediate: true })
  onBeforeUnmount(disconnect)

  return { sum, containers }
}
