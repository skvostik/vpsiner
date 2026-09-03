import { onBeforeUnmount, ref, watch, type Ref } from 'vue'

import { reportSseIssue } from './useBackendHealth'
import type { HostPoint, MetricsResolution, MetricsResponse } from '../types'

const trimTickMs = 5_000
const reconnectDelayMs = 3_000

/** Live host metrics history for HostView, pushed instead of polled. */
export function useHostMetricsStream(
  windowMs: Ref<number>,
  active: Ref<boolean>,
  setResolution: (resolution: MetricsResolution) => void
) {
  const points = ref<HostPoint[]>([])

  let source: EventSource | undefined
  let trimTimer: number | undefined
  let reconnectTimer: number | undefined

  function trim() {
    if (!windowMs.value) return
    const cutoff = Date.now() - windowMs.value
    points.value = points.value.filter((point) => point.ts >= cutoff)
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
    if (!active.value || !windowMs.value || reconnectTimer) return
    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = undefined
      connect()
    }, reconnectDelayMs)
  }

  function connect() {
    disconnect()
    if (!active.value || !windowMs.value) return

    const from = Date.now() - windowMs.value
    const params = new URLSearchParams({ from: String(from) })
    source = new EventSource(`/api/stream/metrics/host?${params}`)
    source.addEventListener('snapshot', (event) => {
      const response = JSON.parse((event as MessageEvent).data) as MetricsResponse<HostPoint[]>
      points.value = response.data
      trim()
      setResolution(response.resolution)
      console.debug('[host-metrics-stream] snapshot', response)
    })
    source.addEventListener('append', (event) => {
      const point = JSON.parse((event as MessageEvent).data) as HostPoint
      points.value = [...points.value, point]
      trim()
      console.debug('[host-metrics-stream] append', point)
    })
    source.onerror = () => {
      if (source) reconnect(source)
    }

    trimTimer = window.setInterval(trim, trimTickMs)
  }

  watch([windowMs, active], connect, { immediate: true })
  onBeforeUnmount(disconnect)

  return { points }
}
