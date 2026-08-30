import { onBeforeUnmount, ref, watch, type Ref } from 'vue'

import { reportSseIssue } from './useBackendHealth'
import type {
  ContainerGroupMetricsAppend,
  ContainerPoint,
  GroupPoint,
  MetricsResolution,
} from '../types'

const trimTickMs = 5_000

/** Live per-log-group history for the container detail view, pushed instead of polled. */
export function useContainerMetricsStream(
  logGroup: Ref<string>,
  resolution: Ref<MetricsResolution>,
  windowMs: Ref<number>,
  active: Ref<boolean>
) {
  const sum = ref<GroupPoint[]>([])
  const containers = ref<Record<string, ContainerPoint[]>>({})

  let source: EventSource | undefined
  let trimTimer: number | undefined

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
  }

  function connect() {
    disconnect()
    if (!active.value || !logGroup.value || !windowMs.value) return

    const from = Date.now() - windowMs.value
    const params = new URLSearchParams({
      from: String(from),
      resolution: resolution.value,
    })
    source = new EventSource(
      `/api/stream/metrics/containers/${encodeURIComponent(logGroup.value)}?${params}`
    )
    source.addEventListener('snapshot', (event) => {
      const data = JSON.parse((event as MessageEvent).data) as {
        sum: GroupPoint[]
        containers: Record<string, ContainerPoint[]>
      }
      sum.value = data.sum
      containers.value = data.containers
      console.debug('[container-metrics-stream] snapshot', data)
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
    // The browser retries automatically; only report an outage once the stream is definitively closed.
    source.onerror = () => reportSseIssue(source)

    trimTimer = window.setInterval(trim, trimTickMs)
  }

  watch([logGroup, resolution, windowMs, active], connect, { immediate: true })
  onBeforeUnmount(disconnect)

  return { sum, containers }
}
