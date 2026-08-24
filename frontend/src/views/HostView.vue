<script setup lang="ts">
import { ref } from 'vue'

import HostOverview from '../components/HostOverview.vue'
import LivePollIndicator from '../components/LivePollIndicator.vue'
import MetricsWindowPicker from '../components/MetricsWindowPicker.vue'
import { api, containerMetricsHistory } from '../api'
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

usePageTitle('Host Metrics')
</script>

<template>
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
    <LivePollIndicator :active="isLive" label="Live host metrics" />
  </div>
</template>
