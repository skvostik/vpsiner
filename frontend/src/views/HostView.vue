<script setup lang="ts">
import { computed, ref } from 'vue'

import HostOverview from '../components/HostOverview.vue'
import LiveStatusIcon from '../components/LiveStatusIcon.vue'
import MetricsWindowPicker from '../components/MetricsWindowPicker.vue'
import { api, containerMetricsHistory } from '../api'
import { useBackendHealth, dockerConnected } from '../composables/useBackendHealth'
import { useContainersMetricsStream } from '../composables/useContainersMetricsStream'
import { useHostMetricsStream } from '../composables/useHostMetricsStream'
import { useMetricsSnapshotStream } from '../composables/useMetricsSnapshotStream'
import { useMetricsWindow } from '../composables/useMetricsWindow'
import { usePageTitle } from '../composables/usePageTitle'
import type { ContainerMetricsByService, HostPoint, TimeRange } from '../types'

const restHostSamples = ref<HostPoint[]>([])
const restContainerMetricHistory = ref<ContainerMetricsByService>({})

// Card headers always show current values, independently of the chart window below them.
const { snapshot } = useMetricsSnapshotStream()

async function load(range: TimeRange) {
  if (isLive.value) return // live windows are handled by the SSE streams instead
  const [hostMetrics, history] = await Promise.all([
    api.host.metrics(range),
    containerMetricsHistory(range),
  ])
  restHostSamples.value = hostMetrics.data
  restContainerMetricHistory.value = history.data
  return hostMetrics.resolution
}

const {
  timeWindow,
  customFrom,
  customTo,
  isLive,
  resolutionLabel,
  liveWindowMs,
  setResolution,
  updateWindow,
  updateCustomFrom,
  updateCustomTo,
} = useMetricsWindow(load)
const { points: liveHostSamples } = useHostMetricsStream(liveWindowMs, isLive, setResolution)
const { series: liveContainerMetricHistory } = useContainersMetricsStream(
  liveWindowMs,
  isLive,
  setResolution
)
const hostSamples = computed(() => (isLive.value ? liveHostSamples.value : restHostSamples.value))
const containerMetricHistory = computed(() =>
  isLive.value ? liveContainerMetricHistory.value : restContainerMetricHistory.value
)
const { backendOnline } = useBackendHealth()
const pageStatus = computed<'live' | 'history' | 'stopped' | 'docker-error'>(() => {
  if (!backendOnline.value) return 'stopped'
  if (!dockerConnected.value) return 'docker-error'
  return isLive.value ? 'live' : 'history'
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
