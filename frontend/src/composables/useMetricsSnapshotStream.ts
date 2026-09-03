import { onBeforeUnmount, onMounted, ref } from 'vue'

import { reportSseIssue } from './useBackendHealth'
import { reconnectDelayMs } from './streamConfig'
import type { HostPoint, MetricsSnapshot } from '../types'

/** Pushes MetricsSnapshot updates over SSE instead of polling /api/metrics/current. */
export function useMetricsSnapshotStream() {
  const snapshot = ref<MetricsSnapshot>({ host: null, containers: {}, services: {} })
  let source: EventSource | undefined
  let reconnectTimer: number | undefined
  let stopped = false

  function connect() {
    source?.close()
    if (reconnectTimer) window.clearTimeout(reconnectTimer)
    reconnectTimer = undefined

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
    // A failed handshake (e.g. proxy 502 during a backend restart) leaves the browser's
    // own retry disabled, so reconnect ourselves instead of relying on it.
    source.onerror = () => {
      const failedSource = source
      reportSseIssue(failedSource)
      failedSource?.close()
      if (source === failedSource) source = undefined
      if (!stopped && !reconnectTimer) {
        reconnectTimer = window.setTimeout(() => {
          reconnectTimer = undefined
          connect()
        }, reconnectDelayMs)
      }
    }
  }

  onMounted(connect)

  onBeforeUnmount(() => {
    stopped = true
    if (reconnectTimer) window.clearTimeout(reconnectTimer)
    source?.close()
  })

  return { snapshot }
}
