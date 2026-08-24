import { computed, onBeforeUnmount, onMounted, ref } from 'vue'

import { metricsSampleIntervalMs } from './useBackendHealth'
import {
  computePollIntervalMs,
  computeStaleAfterMs,
  RESOLUTION_BUCKET_MS,
} from '../metricsFreshness'
import type { MetricsResolution, MetricsWindow, TimeRange } from '../types'

const storageKey = 'vpsiner.metrics.window.v1'

const durationMs: Record<Exclude<MetricsWindow, 'custom'>, number> = {
  '10m': 10 * 60 * 1000,
  '30m': 30 * 60 * 1000,
  '1h': 60 * 60 * 1000,
  '6h': 6 * 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000,
  '7d': 7 * 24 * 60 * 60 * 1000,
}

const resolutionLabels: Record<MetricsResolution, string> = {
  '10s': '10 sec avg',
  '1m': '1 min avg',
  '5m': '5 min avg',
  '1h': '1 hour avg',
}

function resolutionFor(range: TimeRange): MetricsResolution {
  const span = range.to - range.from
  if (span <= 30 * 60 * 1000) return '10s'
  if (span <= 3 * 60 * 60 * 1000) return '1m'
  if (span <= 24 * 60 * 60 * 1000) return '5m'
  return '1h'
}

const bucketMs = RESOLUTION_BUCKET_MS

export function useMetricsWindow(
  load: (range: TimeRange, resolution: MetricsResolution) => Promise<void>
) {
  const timeWindow = ref<MetricsWindow>('10m')
  const customFrom = ref<number>()
  const customTo = ref<number>()
  const loading = ref(false)
  const error = ref('')
  let pollTimer: number | undefined

  const isLive = computed(() => timeWindow.value !== 'custom')

  function computeRange(): TimeRange {
    if (timeWindow.value === 'custom') {
      const to = customTo.value ?? Date.now()
      return { from: customFrom.value ?? to - 60 * 60 * 1000, to }
    }
    const to = Date.now()
    return { from: to - durationMs[timeWindow.value], to }
  }

  const resolutionLabel = computed(() => resolutionLabels[resolutionFor(computeRange())])
  // How old a sample can be before it no longer counts as "current" for a live summary stat.
  const resolutionBucketMs = computed(() => bucketMs[resolutionFor(computeRange())])
  const pollIntervalMs = computed(() => computePollIntervalMs(metricsSampleIntervalMs.value))
  const staleAfterMs = computed(() =>
    computeStaleAfterMs(resolutionBucketMs.value, pollIntervalMs.value)
  )

  async function reload() {
    loading.value = true
    const range = computeRange()
    try {
      await load(range, resolutionFor(range))
      error.value = ''
    } catch (loadError) {
      error.value = loadError instanceof Error ? loadError.message : 'Unable to load metrics'
    } finally {
      loading.value = false
    }
  }

  function restartPolling() {
    if (pollTimer) {
      window.clearInterval(pollTimer)
      pollTimer = undefined
    }
    if (!isLive.value) return
    pollTimer = window.setInterval(() => {
      if (document.visibilityState === 'visible') reload()
    }, pollIntervalMs.value)
  }

  function persist() {
    localStorage.setItem(
      storageKey,
      JSON.stringify({
        timeWindow: timeWindow.value,
        customFrom: customFrom.value,
        customTo: customTo.value,
      })
    )
  }

  function updateWindow(value: MetricsWindow) {
    timeWindow.value = value
    if (value === 'custom' && (customFrom.value === undefined || customTo.value === undefined)) {
      customTo.value = Date.now()
      customFrom.value = customTo.value - 60 * 60 * 1000
    }
    persist()
    reload()
    restartPolling()
  }

  function updateCustomFrom(value: number | null) {
    customFrom.value = value ?? undefined
    persist()
    if (customFrom.value !== undefined && customTo.value !== undefined) reload()
  }

  function updateCustomTo(value: number | null) {
    customTo.value = value ?? undefined
    persist()
    if (customFrom.value !== undefined && customTo.value !== undefined) reload()
  }

  onMounted(() => {
    try {
      const saved = JSON.parse(localStorage.getItem(storageKey) ?? '{}') as Partial<{
        timeWindow: MetricsWindow
        customFrom: number
        customTo: number
      }>
      if (saved.timeWindow) timeWindow.value = saved.timeWindow
      customFrom.value = saved.customFrom
      customTo.value = saved.customTo
    } catch {
      // Ignore invalid local preferences.
    }
    reload()
    restartPolling()
  })

  onBeforeUnmount(() => {
    if (pollTimer) window.clearInterval(pollTimer)
  })

  return {
    timeWindow,
    customFrom,
    customTo,
    loading,
    error,
    isLive,
    resolutionLabel,
    resolutionBucketMs,
    staleAfterMs,
    updateWindow,
    updateCustomFrom,
    updateCustomTo,
    reload,
  }
}
