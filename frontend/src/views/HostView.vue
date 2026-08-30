<script setup lang="ts">
import { computed, ref } from 'vue'

import HostOverview from '../components/HostOverview.vue'
import LiveStatusIcon from '../components/LiveStatusIcon.vue'
import MetricsWindowPicker from '../components/MetricsWindowPicker.vue'
import { api, containerMetricsHistory } from '../api'
import { useBackendHealth } from '../composables/useBackendHealth'
import { useMetricsWindow } from '../composables/useMetricsWindow'
import { usePageTitle } from '../composables/usePageTitle'
import type { ContainerGroupSample, HostSample, MetricsResolution, TimeRange } from '../types'

const hostSamples = ref<HostSample[]>([])
const containerMetricHistory = ref<ContainerGroupSample[]>([])

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
  staleAfterMs,
  updateWindow,
  updateCustomFrom,
  updateCustomTo,
} = useMetricsWindow(load)
const { backendOnline } = useBackendHealth()
const pageStatus = computed<'live' | 'history' | 'stopped'>(() => {
  if (!backendOnline.value) return 'stopped'
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
      :sample="hostSamples[hostSamples.length - 1]"
      :history="hostSamples"
      :container-history="containerMetricHistory"
      :stale-after-ms="staleAfterMs"
    />
  </div>
</template>
