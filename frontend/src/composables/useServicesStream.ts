import { onBeforeUnmount, onMounted, ref } from 'vue'

import { reportSseIssue } from './useBackendHealth'
import { reconnectDelayMs } from './streamConfig'
import type { ServiceDiff, Services } from '../types'

// Shared state: the services list view and the log viewer's service dropdown run off one connection.
const services = ref<Services>({})
const loading = ref(true)

let source: EventSource | undefined
let consumers = 0
let reconnectTimer: number | undefined

function connect() {
  source?.close()
  if (reconnectTimer) window.clearTimeout(reconnectTimer)
  reconnectTimer = undefined

  source = new EventSource('/api/stream/logs')
  source.addEventListener('snapshot', (event) => {
    services.value = JSON.parse((event as MessageEvent).data) as Services
    loading.value = false
    console.debug('[services-stream] snapshot', services.value)
  })
  source.addEventListener('diff', (event) => {
    const diff = JSON.parse((event as MessageEvent).data) as ServiceDiff
    services.value = { ...services.value, ...diff.added, ...diff.updated }
    for (const service of diff.removed) {
      delete services.value[service]
    }
    console.debug('[services-stream] diff', diff)
  })
  // A failed handshake (e.g. proxy 502 during a backend restart) leaves the browser's
  // own retry disabled, so reconnect ourselves instead of relying on it.
  source.onerror = () => {
    const failedSource = source
    reportSseIssue(failedSource)
    failedSource?.close()
    if (source === failedSource) source = undefined
    if (consumers > 0 && !reconnectTimer) {
      reconnectTimer = window.setTimeout(() => {
        reconnectTimer = undefined
        connect()
      }, reconnectDelayMs)
    }
  }
}

export function useServicesStream() {
  onMounted(() => {
    consumers += 1
    if (consumers === 1) connect()
  })

  onBeforeUnmount(() => {
    consumers -= 1
    if (consumers > 0) return
    if (reconnectTimer) window.clearTimeout(reconnectTimer)
    reconnectTimer = undefined
    source?.close()
    source = undefined
    services.value = {}
    loading.value = true
  })

  return { services, loading }
}
