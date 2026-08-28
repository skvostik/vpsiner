<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { NInput, NSpin, NSwitch } from 'naive-ui'
import { Search } from '@lucide/vue'

import ContainerTable from '../components/ContainerTable.vue'
import LivePollIndicator from '../components/LivePollIndicator.vue'
import { containerMetricsHistory } from '../api'
import { useContainers } from '../composables/useContainers'
import { metricsSampleIntervalMs } from '../composables/useBackendHealth'
import { usePageTitle } from '../composables/usePageTitle'
import {
  computePollIntervalMs,
  computeStaleAfterMs,
  latestFreshSamples,
  RESOLUTION_BUCKET_MS,
} from '../metricsFreshness'
import type { ContainerMetricsByLogGroup, ContainerOverviewMetrics, ContainerRow } from '../types'

usePageTitle('Containers')

const { containers, loading, reload } = useContainers()
const metricsHistory = ref<ContainerMetricsByLogGroup>({})
const metricsPollIntervalMs = computed(() => computePollIntervalMs(metricsSampleIntervalMs.value))
const staleAfterMs = computed(() =>
  computeStaleAfterMs(RESOLUTION_BUCKET_MS['10s'], metricsPollIntervalMs.value)
)
let metricsPollTimer: number | undefined

async function loadMetricsHistory() {
  if (document.visibilityState !== 'visible') return
  try {
    const to = Date.now()
    metricsHistory.value = await containerMetricsHistory(
      { from: to - staleAfterMs.value - metricsPollIntervalMs.value, to },
      '10s'
    )
  } catch {
    // Row-level metrics are a nice-to-have; the container list itself still renders on failure.
  }
}

const rows = computed<ContainerRow[]>(() => {
  const flatSamples = Object.values(metricsHistory.value).flat()
  const freshGroups = new Set(
    latestFreshSamples(flatSamples, (sample) => sample.log_group, staleAfterMs.value).map(
      (sample) => sample.log_group
    )
  )
  const metricsByGroup = new Map<string, ContainerOverviewMetrics>()
  for (const [group, samples] of Object.entries(metricsHistory.value)) {
    if (!freshGroups.has(group)) continue
    const latest = samples[samples.length - 1]
    const previous = samples[samples.length - 2]
    if (!latest) continue
    const rate = (key: 'net_rx' | 'net_tx' | 'blk_read' | 'blk_write') => {
      if (!previous || latest.ts <= previous.ts || latest[key] < previous[key]) return 0
      return (latest[key] - previous[key]) / ((latest.ts - previous.ts) / 1_000)
    }
    metricsByGroup.set(group, {
      cpu_pct: latest.cpu_pct,
      mem_used: latest.mem_used,
      mem_limit: latest.mem_limit,
      net_rx_rate: rate('net_rx'),
      net_tx_rate: rate('net_tx'),
      disk_read_rate: rate('blk_read'),
      disk_write_rate: rate('blk_write'),
    })
  }
  return containers.value.map((container) => ({
    ...container,
    metrics: metricsByGroup.get(container.log_group),
  }))
})

const showOnlyRunning = ref(false)
const containerSearch = ref('')
const showOnlyRunningStorageKey = 'vpsiner.show-only-running.v1'
const visibleContainers = computed(() => {
  const search = containerSearch.value.trim().toLocaleLowerCase()
  return rows.value
    .filter((container) => {
      const matchesState =
        !showOnlyRunning.value || ['running', 'restarting'].includes(container.state)
      const matchesSearch =
        !search ||
        container.name.toLocaleLowerCase().includes(search) ||
        container.image.toLocaleLowerCase().includes(search)
      return matchesState && matchesSearch
    })
    .sort(
      (left, right) =>
        Number(right.state === 'running') - Number(left.state === 'running') ||
        left.name.localeCompare(right.name, undefined, { sensitivity: 'base', numeric: true }) ||
        left.id.localeCompare(right.id)
    )
})

onMounted(() => {
  const storedOnlyRunning = window.localStorage.getItem(showOnlyRunningStorageKey)
  showOnlyRunning.value = storedOnlyRunning === null ? false : storedOnlyRunning === 'true'
  loadMetricsHistory()
  metricsPollTimer = window.setInterval(loadMetricsHistory, metricsPollIntervalMs.value)
})

onBeforeUnmount(() => {
  if (metricsPollTimer) window.clearInterval(metricsPollTimer)
})

watch(showOnlyRunning, (value) =>
  window.localStorage.setItem(showOnlyRunningStorageKey, String(value))
)

async function handleActionComplete() {
  await Promise.all([reload(), loadMetricsHistory()])
}
</script>

<template>
  <div class="space-y-5">
    <div class="flex items-center justify-end gap-4">
      <div class="flex items-center gap-4">
        <label class="flex items-center gap-2 text-xs text-neutral-500 dark:text-neutral-400"
          ><span>Show only running</span><n-switch v-model:value="showOnlyRunning" size="small"
        /></label>
        <n-spin v-if="loading" size="small" />
      </div>
    </div>
    <n-input v-model:value="containerSearch" clearable placeholder="Search by name or image">
      <template #prefix><Search :size="16" /></template>
    </n-input>
    <ContainerTable
      :rows="visibleContainers"
      :loading="loading"
      @action-complete="handleActionComplete"
    />
    <LivePollIndicator label="Live container status" />
  </div>
</template>
