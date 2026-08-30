import { onBeforeUnmount, ref, watch, type Ref } from 'vue'

import { reportBackendUnreachable } from './useBackendHealth'
import type { ContainerMetricsByLogGroup, GroupPoint, MetricsResolution } from '../types'

const trimTickMs = 5_000

/** Live aggregate per-log-group metrics history for HostView, pushed instead of polled. */
export function useContainersMetricsStream(
  resolution: Ref<MetricsResolution>,
  windowMs: Ref<number>,
  active: Ref<boolean>
) {
  const series = ref<ContainerMetricsByLogGroup>({})

  let source: EventSource | undefined
  let trimTimer: number | undefined

  function trim() {
    if (!windowMs.value) return
    const cutoff = Date.now() - windowMs.value
    for (const logGroup of Object.keys(series.value)) {
      series.value[logGroup] = series.value[logGroup].filter((point) => point.ts >= cutoff)
    }
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
    source = new EventSource(`/api/stream/metrics/containers?${params}`)
    source.addEventListener('snapshot', (event) => {
      series.value = JSON.parse((event as MessageEvent).data) as ContainerMetricsByLogGroup
      console.debug('[containers-metrics-stream] snapshot', series.value)
    })
    source.addEventListener('append', (event) => {
      const append = JSON.parse((event as MessageEvent).data) as Record<string, GroupPoint>
      for (const [logGroup, point] of Object.entries(append)) {
        series.value[logGroup] = [...(series.value[logGroup] ?? []), point]
      }
      trim()
      console.debug('[containers-metrics-stream] append', append)
    })
    // The browser retries automatically; just surface the outage to the rest of the UI.
    source.onerror = () => reportBackendUnreachable()

    trimTimer = window.setInterval(trim, trimTickMs)
  }

  watch([resolution, windowMs, active], connect, { immediate: true })
  onBeforeUnmount(disconnect)

  return { series }
}
