import { onBeforeUnmount, onMounted, ref } from 'vue'

import { reportSseIssue } from './useBackendHealth'
import type { HostPoint, MetricsSnapshot } from '../types'

/** Pushes MetricsSnapshot updates over SSE instead of polling /api/metrics/current. */
export function useMetricsSnapshotStream() {
  const snapshot = ref<MetricsSnapshot>({ host: null, containers: {}, services: {} })
  let source: EventSource | undefined

  onMounted(() => {
    source = new EventSource('/api/stream/metrics/current')
    // Host and containers arrive as separate events at their own collection rates.
    source.addEventListener('snapshot', (event) => {
      snapshot.value = JSON.parse(event.data)
    })
    source.addEventListener('host', (event) => {
      const host = JSON.parse(event.data) as HostPoint | null
      snapshot.value = { ...snapshot.value, host }
    })
    source.addEventListener('containers', (event) => {
      const { containers, services } = JSON.parse(event.data) as Pick<
        MetricsSnapshot,
        'containers' | 'services'
      >
      snapshot.value = { ...snapshot.value, containers, services }
    })
    // The browser retries automatically; only report an outage once the stream is definitively closed.
    source.onerror = () => reportSseIssue(source)
  })

  onBeforeUnmount(() => {
    source?.close()
  })

  return { snapshot }
}
