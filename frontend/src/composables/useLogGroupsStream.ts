import { onBeforeUnmount, onMounted, ref } from 'vue'

import { reportBackendUnreachable } from './useBackendHealth'
import type { LogGroupDiff, LogGroups } from '../types'

// Shared state: the groups list view and the log viewer's group dropdown run off one connection.
const groups = ref<LogGroups>({})
const loading = ref(true)

let source: EventSource | undefined
let consumers = 0

function connect() {
  source = new EventSource('/api/stream/logs')
  source.addEventListener('snapshot', (event) => {
    groups.value = JSON.parse((event as MessageEvent).data) as LogGroups
    loading.value = false
    console.debug('[log-groups-stream] snapshot', groups.value)
  })
  source.addEventListener('diff', (event) => {
    const diff = JSON.parse((event as MessageEvent).data) as LogGroupDiff
    groups.value = { ...groups.value, ...diff.added, ...diff.updated }
    for (const logGroup of diff.removed) {
      delete groups.value[logGroup]
    }
    console.debug('[log-groups-stream] diff', diff)
  })
  // The browser retries automatically; just surface the outage to the rest of the UI.
  source.onerror = () => reportBackendUnreachable()
}

export function useLogGroupsStream() {
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

  return { groups, loading }
}
