import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'

import { dockerConnected, reportSseIssue } from './useBackendHealth'
import { reconnectDelayMs } from './streamConfig'
import type { ContainerDiff, ContainerSummary } from '../types'

// Shared state: the sidebar badge and the containers view run off a single connection.
// Exported so useContainerActions can watch a container's real streamed state.
export const containersById = ref<Record<string, ContainerSummary>>({})
const loading = ref(true)
const containers = computed(() => Object.values(containersById.value))
const runningCount = computed(
  () => containers.value.filter((item) => item.state === 'running').length
)

let source: EventSource | undefined
let consumers = 0
let reconnectTimer: number | undefined

function connect() {
  source?.close()
  if (reconnectTimer) window.clearTimeout(reconnectTimer)
  reconnectTimer = undefined

  source = new EventSource('/api/stream/containers')
  source.addEventListener('snapshot', (event) => {
    const snapshot = JSON.parse((event as MessageEvent).data) as ContainerSummary[]
    containersById.value = Object.fromEntries(
      snapshot.map((container) => [container.id, container])
    )
    loading.value = false
    console.debug('[containers-stream] snapshot', containersById.value)
  })
  source.addEventListener('diff', (event) => {
    const diff = JSON.parse((event as MessageEvent).data) as ContainerDiff
    for (const container of [...diff.added, ...diff.updated]) {
      containersById.value[container.id] = container
    }
    for (const id of diff.removed) {
      delete containersById.value[id]
    }
    console.debug('[containers-stream] diff', diff)
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

// The backend keeps serving its last known containers while Docker is unreachable, so drop them.
// Reconnecting on recovery pulls a fresh snapshot; a diff alone would not refill the cleared cache.
watch(dockerConnected, (connected) => {
  if (!connected) {
    containersById.value = {}
    return
  }
  if (consumers > 0) connect()
})

export function useContainersStream() {
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
    containersById.value = {}
    loading.value = true
  })

  return { containers, runningCount, loading }
}
