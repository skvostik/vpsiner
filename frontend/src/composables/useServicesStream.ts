import { onBeforeUnmount, onMounted, ref } from 'vue'

import { reportSseIssue } from './useBackendHealth'
import type { ServiceDiff, Services } from '../types'

// Shared state: the services list view and the log viewer's service dropdown run off one connection.
const services = ref<Services>({})
const loading = ref(true)

let source: EventSource | undefined
let consumers = 0

function connect() {
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
  // The browser retries automatically; only report an outage once the stream is definitively closed.
  source.onerror = () => reportSseIssue(source)
}

export function useServicesStream() {
  onMounted(() => {
    consumers += 1
    if (consumers === 1) connect()
  })

  onBeforeUnmount(() => {
    consumers -= 1
    if (consumers > 0) return
    source?.close()
    source = undefined
  })

  return { services, loading }
}
