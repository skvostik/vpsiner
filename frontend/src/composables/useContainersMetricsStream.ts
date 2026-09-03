import { onBeforeUnmount, ref, watch, type Ref } from 'vue'

import { reportSseIssue } from './useBackendHealth'
import type {
  ContainerMetricsByService,
  GroupPoint,
  MetricsResolution,
  MetricsResponse,
} from '../types'

const trimTickMs = 5_000

/** Live aggregate per-log-group metrics history for HostView, pushed instead of polled. */
export function useContainersMetricsStream(
  windowMs: Ref<number>,
  active: Ref<boolean>,
  setResolution: (resolution: MetricsResolution) => void
) {
  const series = ref<ContainerMetricsByService>({})

  let source: EventSource | undefined
  let trimTimer: number | undefined

  function trim() {
    if (!windowMs.value) return
    const cutoff = Date.now() - windowMs.value
    for (const service of Object.keys(series.value)) {
      series.value[service] = series.value[service].filter((point) => point.ts >= cutoff)
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
    const params = new URLSearchParams({ from: String(from) })
    source = new EventSource(`/api/stream/metrics/containers?${params}`)
    source.addEventListener('snapshot', (event) => {
      const response = JSON.parse(
        (event as MessageEvent).data
      ) as MetricsResponse<ContainerMetricsByService>
      series.value = response.data
      setResolution(response.resolution)
      console.debug('[containers-metrics-stream] snapshot', response)
    })
    source.addEventListener('append', (event) => {
      const append = JSON.parse((event as MessageEvent).data) as Record<string, GroupPoint>
      for (const [service, point] of Object.entries(append)) {
        series.value[service] = [...(series.value[service] ?? []), point]
      }
      trim()
      console.debug('[containers-metrics-stream] append', append)
    })
    // The browser retries automatically; only report an outage once the stream is definitively closed.
    source.onerror = () => reportSseIssue(source)

    trimTimer = window.setInterval(trim, trimTickMs)
  }

  watch([windowMs, active], connect, { immediate: true })
  onBeforeUnmount(disconnect)

  return { series }
}
