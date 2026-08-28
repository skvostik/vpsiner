import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useMessage } from 'naive-ui'

import { api } from '../api'
import type { ContainerSummary } from '../types'

const pollIntervalMs = 5_000

// Shared state: the sidebar badge and the containers list run off a single poll loop.
const containers = ref<ContainerSummary[]>([])
const loading = ref(true)
const error = ref('')
const runningCount = computed(
  () => containers.value.filter((item) => item.state === 'running').length
)
let message: ReturnType<typeof useMessage> | undefined
let pollTimer: number | undefined
let loadPromise: Promise<void> | undefined
let consumers = 0
let lastLoadedAt = 0
let lastReportedError = ''

function reportError(value: unknown, fallback: string) {
  const text = value instanceof Error ? value.message : fallback
  error.value = text
  if (text !== lastReportedError) {
    message?.error(text)
    lastReportedError = text
  }
}

async function load() {
  if (document.visibilityState !== 'visible') return
  if (loadPromise) return loadPromise

  loadPromise = loadContainers()
  try {
    await loadPromise
  } finally {
    loadPromise = undefined
  }
}

async function loadContainers() {
  try {
    containers.value = await api.containers.list()
    lastLoadedAt = Date.now()
    error.value = ''
  } catch (loadError) {
    reportError(loadError, 'Unable to load containers')
  } finally {
    loading.value = false
  }
}

export function useContainers() {
  message = useMessage()

  onMounted(() => {
    consumers += 1
    if (consumers > 1) return
    if (Date.now() - lastLoadedAt >= pollIntervalMs) load()
    pollTimer = window.setInterval(load, pollIntervalMs)
  })

  onBeforeUnmount(() => {
    consumers -= 1
    if (consumers > 0 || !pollTimer) return
    window.clearInterval(pollTimer)
    pollTimer = undefined
  })

  return {
    containers,
    runningCount,
    loading,
    error,
    reload: load,
  }
}
