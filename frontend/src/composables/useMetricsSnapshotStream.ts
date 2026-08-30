import { onBeforeUnmount, onMounted, ref } from 'vue'

import { reportBackendUnreachable } from './useBackendHealth'
import type { MetricsSnapshot } from '../types'

/** Pushes MetricsSnapshot updates over SSE instead of polling /api/metrics/current. */
export function useMetricsSnapshotStream() {
  const snapshot = ref<MetricsSnapshot>({ host: null, containers: {}, log_groups: {} })
  let source: EventSource | undefined

  onMounted(() => {
    source = new EventSource('/api/stream/metrics/current')
    source.onmessage = (event) => {
      snapshot.value = JSON.parse(event.data)
      console.debug('[metrics-stream] event received', snapshot.value)
    }
    // The browser retries automatically; just surface the outage to the rest of the UI.
    source.onerror = () => reportBackendUnreachable()
  })

  onBeforeUnmount(() => {
    source?.close()
  })

  return { snapshot }
}
