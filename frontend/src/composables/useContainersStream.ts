import { computed, onBeforeUnmount, onMounted, ref } from 'vue'

import { reportSseIssue } from './useBackendHealth'
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

function connect() {
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
  // The browser retries automatically; only report an outage once the stream is definitively closed.
  source.onerror = () => reportSseIssue(source)
}

export function useContainersStream() {
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

  return { containers, runningCount, loading }
}
