import { onBeforeUnmount, ref, watch, type Ref } from 'vue'

import { reportSseIssue } from './useBackendHealth'
import type { HostPoint, MetricsResolution } from '../types'

const trimTickMs = 5_000

/** Live host metrics history for HostView, pushed instead of polled. */
export function useHostMetricsStream(
  resolution: Ref<MetricsResolution>,
  windowMs: Ref<number>,
  active: Ref<boolean>
) {
  const points = ref<HostPoint[]>([])

  let source: EventSource | undefined
  let trimTimer: number | undefined

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
  }

  function connect() {
    disconnect()
    if (!active.value || !windowMs.value) return

    const from = Date.now() - windowMs.value
    const params = new URLSearchParams({ from: String(from), resolution: resolution.value })
    source = new EventSource(`/api/stream/metrics/host?${params}`)
    source.addEventListener('snapshot', (event) => {
      points.value = JSON.parse((event as MessageEvent).data) as HostPoint[]
      console.debug('[host-metrics-stream] snapshot', points.value)
    })
    source.addEventListener('append', (event) => {
      const point = JSON.parse((event as MessageEvent).data) as HostPoint
      points.value = [...points.value, point]
      trim()
      console.debug('[host-metrics-stream] append', point)
    })
    // The browser retries automatically; only report an outage once the stream is definitively closed.
    source.onerror = () => reportSseIssue(source)

    trimTimer = window.setInterval(trim, trimTickMs)
  }

  watch([resolution, windowMs, active], connect, { immediate: true })
  onBeforeUnmount(disconnect)

  return { points }
}
