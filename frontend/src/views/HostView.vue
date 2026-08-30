<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'

import HostOverview from '../components/HostOverview.vue'
import LiveStatusIcon from '../components/LiveStatusIcon.vue'
import MetricsWindowPicker from '../components/MetricsWindowPicker.vue'
import { api, containerMetricsHistory } from '../api'
import { metricsSampleIntervalMs, useBackendHealth } from '../composables/useBackendHealth'
import { useMetricsWindow } from '../composables/useMetricsWindow'
import { usePageTitle } from '../composables/usePageTitle'
import { computePollIntervalMs } from '../metricsFreshness'
import type {
  ContainerGroupSample,
  HostSample,
  MetricsResolution,
  MetricsSnapshot,
  TimeRange,
} from '../types'

const hostSamples = ref<HostSample[]>([])
const containerMetricHistory = ref<ContainerGroupSample[]>([])

// Card headers always show current values, independently of the chart window below them.
const snapshot = ref<MetricsSnapshot>({ host: null, containers: {}, log_groups: {} })
const snapshotPollIntervalMs = computed(() => computePollIntervalMs(metricsSampleIntervalMs.value))
let snapshotPollTimer: number | undefined

async function loadSnapshot() {
  if (document.visibilityState !== 'visible') return
  try {
    snapshot.value = await api.metrics.current()
  } catch {
    // Headline numbers are a nice-to-have; the charts below still render on failure.
  }
}

async function load(range: TimeRange, resolution: MetricsResolution) {
  const [hostMetrics, history] = await Promise.all([
    api.host.metrics(range, resolution),
    containerMetricsHistory(range, resolution),
  ])
  hostSamples.value = hostMetrics
  containerMetricHistory.value = Object.values(history).flat()
}

const {
  timeWindow,
  customFrom,
  customTo,
  isLive,
  resolutionLabel,
  updateWindow,
  updateCustomFrom,
  updateCustomTo,
} = useMetricsWindow(load)
const { backendOnline } = useBackendHealth()
const pageStatus = computed<'live' | 'history' | 'stopped'>(() => {
  if (!backendOnline.value) return 'stopped'
  return isLive.value ? 'live' : 'history'
})

onMounted(() => {
  loadSnapshot()
  snapshotPollTimer = window.setInterval(loadSnapshot, snapshotPollIntervalMs.value)
})

onBeforeUnmount(() => {
  if (snapshotPollTimer) window.clearInterval(snapshotPollTimer)
})

usePageTitle('Host Metrics')
</script>

<template>
  <Teleport to="#app-header-title-leading">
    <LiveStatusIcon :status="pageStatus" :size="15" :pulse="pageStatus === 'live'" />
  </Teleport>
  <div class="space-y-6">
    <MetricsWindowPicker
      :window="timeWindow"
      :custom-from="customFrom"
      :custom-to="customTo"
      :resolution-label="resolutionLabel"
      @update:window="updateWindow"
      @update:custom-from="updateCustomFrom"
      @update:custom-to="updateCustomTo"
    />
    <HostOverview
      :snapshot="snapshot"
      :history="hostSamples"
      :container-history="containerMetricHistory"
    />
  </div>
</template>
