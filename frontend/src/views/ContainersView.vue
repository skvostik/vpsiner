<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { NInput, NSpin, NSwitch } from 'naive-ui'
import { Search } from '@lucide/vue'

import ContainerTable from '../components/ContainerTable.vue'
import LiveStatusIcon from '../components/LiveStatusIcon.vue'
import { api } from '../api'
import { useContainers } from '../composables/useContainers'
import { metricsSampleIntervalMs, useBackendHealth } from '../composables/useBackendHealth'
import { usePageTitle } from '../composables/usePageTitle'
import { computePollIntervalMs } from '../metricsFreshness'
import type { ContainerRow, MetricsSnapshot } from '../types'

usePageTitle('Containers')

const { containers, loading, reload } = useContainers()
const { backendOnline } = useBackendHealth()
const snapshot = ref<MetricsSnapshot>({ host: null, containers: {}, log_groups: {} })
const metricsPollIntervalMs = computed(() => computePollIntervalMs(metricsSampleIntervalMs.value))
const pageStatus = computed<'live' | 'history' | 'stopped'>(() => {
  if (!backendOnline.value) return 'stopped'
  return document.visibilityState === 'visible' ? 'live' : 'history'
})
let metricsPollTimer: number | undefined

async function loadSnapshot() {
  if (document.visibilityState !== 'visible') return
  try {
    snapshot.value = await api.metrics.current()
  } catch {
    // Row-level metrics are a nice-to-have; the container list itself still renders on failure.
  }
}

const rows = computed<ContainerRow[]>(() =>
  containers.value.map((container) => ({
    ...container,
    metrics: snapshot.value.containers[container.id],
  }))
)

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
  loadSnapshot()
  metricsPollTimer = window.setInterval(loadSnapshot, metricsPollIntervalMs.value)
})

onBeforeUnmount(() => {
  if (metricsPollTimer) window.clearInterval(metricsPollTimer)
})

watch(showOnlyRunning, (value) =>
  window.localStorage.setItem(showOnlyRunningStorageKey, String(value))
)

async function handleActionComplete() {
  await Promise.all([reload(), loadSnapshot()])
}
</script>

<template>
  <Teleport to="#app-header-title-leading">
    <LiveStatusIcon :status="pageStatus" :size="15" :pulse="pageStatus === 'live'" />
  </Teleport>
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
  </div>
</template>
