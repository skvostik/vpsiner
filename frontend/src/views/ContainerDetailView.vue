<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ArrowLeft, ChevronDown, ChevronRight, Logs, Play, RotateCcw, Square } from '@lucide/vue'
import { NButton, NCard, NEmpty, NSpin, NStatistic, NTooltip } from 'naive-ui'

import MetricChart, { type ChartPoint, type ChartSeries } from '../components/MetricChart.vue'
import LiveStatusIcon from '../components/LiveStatusIcon.vue'
import MetricsWindowPicker from '../components/MetricsWindowPicker.vue'
import { api } from '../api'
import { colorForKey } from '../colors'
import { formatBytes, formatUptime } from '../format'
import { backendOnline, dockerControlsAvailable } from '../composables/useBackendHealth'
import { useContainerMetricsStream } from '../composables/useContainerMetricsStream'
import { useContainersStream } from '../composables/useContainersStream'
import { useMetricsSnapshotStream } from '../composables/useMetricsSnapshotStream'
import { useMetricsWindow } from '../composables/useMetricsWindow'
import { usePageTitle } from '../composables/usePageTitle'
import type { ContainerPoint, MetricsResolution, TimeRange } from '../types'

const route = useRoute()
const router = useRouter()
const containerId = computed(() => (typeof route.params.id === 'string' ? route.params.id : ''))
const logGroup = computed(() => container.value?.log_group ?? '')
const restContainerSamples = ref<Record<string, ContainerPoint[]>>({})
const error = ref('')
const labelsExpanded = ref(false)
const actionKey = ref('')

// Card headers always show current values, independently of the chart window below them.
const { snapshot } = useMetricsSnapshotStream()
const { containers: allContainers, loading } = useContainersStream()
const container = computed(() => allContainers.value.find((item) => item.id === containerId.value))

function containerNameFor(cid: string) {
  return allContainers.value.find((item) => item.id === cid)?.name ?? cid.slice(0, 12)
}

const containerSeriesEntries = computed(() =>
  Object.entries(containerSamples.value).map(([cid, cidSamples]) => ({
    cid,
    name: containerNameFor(cid),
    color: colorForKey(cid),
    samples: cidSamples,
  }))
)

function containerRatePoints(
  cidSamples: ContainerPoint[],
  key: 'net_rx_rate' | 'net_tx_rate' | 'blk_read_rate' | 'blk_write_rate'
): ChartPoint[] {
  return cidSamples.map((sample) => ({ ts: sample.ts, value: sample[key] }))
}

const cpuSeries = computed<ChartSeries[]>(() =>
  containerSeriesEntries.value.map((entry) => ({
    name: entry.name,
    color: entry.color,
    points: entry.samples.map((sample) => ({ ts: sample.ts, value: sample.cpu_pct })),
  }))
)
const memorySeries = computed<ChartSeries[]>(() =>
  containerSeriesEntries.value.map((entry) => ({
    name: entry.name,
    color: entry.color,
    points: entry.samples.map((sample) => ({ ts: sample.ts, value: sample.mem_used })),
  }))
)
const networkInSeries = computed<ChartSeries[]>(() =>
  containerSeriesEntries.value.map((entry) => ({
    name: entry.name,
    color: entry.color,
    points: containerRatePoints(entry.samples, 'net_rx_rate'),
  }))
)
const networkOutSeries = computed<ChartSeries[]>(() =>
  containerSeriesEntries.value.map((entry) => ({
    name: entry.name,
    color: entry.color,
    points: containerRatePoints(entry.samples, 'net_tx_rate'),
  }))
)
const diskReadSeries = computed<ChartSeries[]>(() =>
  containerSeriesEntries.value.map((entry) => ({
    name: entry.name,
    color: entry.color,
    points: containerRatePoints(entry.samples, 'blk_read_rate'),
  }))
)
const diskWriteSeries = computed<ChartSeries[]>(() =>
  containerSeriesEntries.value.map((entry) => ({
    name: entry.name,
    color: entry.color,
    points: containerRatePoints(entry.samples, 'blk_write_rate'),
  }))
)

// Aggregate rates (log-group sum) power only the header stat numbers, not the per-container charts.
const latest = computed(() => snapshot.value.log_groups[logGroup.value])
const latestNetworkIn = computed(() => latest.value?.net_rx_rate ?? 0)
const latestNetworkOut = computed(() => latest.value?.net_tx_rate ?? 0)
const latestDiskRead = computed(() => latest.value?.blk_read_rate ?? 0)
const latestDiskWrite = computed(() => latest.value?.blk_write_rate ?? 0)

const canStart = computed(
  () =>
    dockerControlsAvailable.value &&
    container.value &&
    !['running', 'restarting'].includes(container.value.state)
)
const canStop = computed(
  () =>
    dockerControlsAvailable.value &&
    container.value &&
    ['running', 'paused', 'restarting'].includes(container.value.state)
)
const canRestart = computed(
  () =>
    dockerControlsAvailable.value &&
    container.value &&
    ['running', 'paused'].includes(container.value.state)
)

function formatRate(value: number) {
  return `${formatBytes(value)}/s`
}

async function loadMetrics(range: TimeRange, resolution: MetricsResolution) {
  if (!logGroup.value || isLive.value) return
  const next = await api.containers.metrics(logGroup.value, range, resolution)
  restContainerSamples.value = next.containers
}

const {
  timeWindow,
  customFrom,
  customTo,
  isLive,
  resolution,
  resolutionLabel,
  liveWindowMs,
  updateWindow,
  updateCustomFrom,
  updateCustomTo,
  reload,
} = useMetricsWindow(loadMetrics)
const { containers: liveContainerSamples } = useContainerMetricsStream(
  logGroup,
  resolution,
  liveWindowMs,
  isLive
)
const containerSamples = computed(() =>
  isLive.value ? liveContainerSamples.value : restContainerSamples.value
)
const pageStatus = computed<'live' | 'history' | 'stopped'>(() => {
  if (!backendOnline.value) return 'stopped'
  if (!container.value || !['running', 'restarting'].includes(container.value.state))
    return 'stopped'
  return isLive.value ? 'live' : 'history'
})

// log_group is only known after the container info loads, so trigger the first metrics fetch then.
watch(logGroup, (value, previous) => {
  if (value && !previous) reload()
})

async function runAction(action: 'start' | 'stop' | 'restart') {
  if (!container.value) return
  actionKey.value = action
  try {
    await api.containers.action(container.value.id, action)
    error.value = ''
  } catch (actionError) {
    error.value = actionError instanceof Error ? actionError.message : 'Container action failed'
  } finally {
    actionKey.value = ''
  }
}

usePageTitle(() => container.value?.name || logGroup.value || 'Container')
</script>

<template>
  <Teleport to="#app-header-title-leading">
    <LiveStatusIcon :status="pageStatus" :size="15" :pulse="pageStatus === 'live'" />
  </Teleport>
  <div>
    <div class="flex items-center gap-3">
      <router-link v-if="logGroup" :to="{ name: 'log-viewer', params: { logGroup } }">
        <n-button tertiary aria-label="Open container logs" tag="span">
          <template #icon><Logs :size="16" /></template>
          Logs
        </n-button>
      </router-link>
      <div v-if="container" class="ml-auto flex items-center gap-1">
        <n-tooltip v-if="canStart">
          <template #trigger
            ><n-button
              circle
              tertiary
              type="primary"
              :loading="actionKey === 'start'"
              aria-label="Start container"
              @click="runAction('start')"
              ><template #icon><Play :size="15" /></template></n-button
          ></template>
          Start container
        </n-tooltip>
        <n-tooltip v-if="canStop">
          <template #trigger
            ><n-button
              circle
              tertiary
              type="error"
              :loading="actionKey === 'stop'"
              aria-label="Stop container"
              @click="runAction('stop')"
              ><template #icon><Square :size="14" /></template></n-button
          ></template>
          Stop container
        </n-tooltip>
        <n-tooltip v-if="canRestart">
          <template #trigger
            ><n-button
              circle
              tertiary
              :loading="actionKey === 'restart'"
              aria-label="Restart container"
              @click="runAction('restart')"
              ><template #icon><RotateCcw :size="15" /></template></n-button
          ></template>
          Restart container
        </n-tooltip>
      </div>
    </div>

    <main class="space-y-6 py-6 sm:py-8">
      <div v-if="error" class="text-sm text-red-600 dark:text-red-400">{{ error }}</div>
      <div v-if="loading" class="flex justify-center py-12"><n-spin /></div>
      <template v-else>
        <n-card
          v-if="container"
          size="small"
          :bordered="false"
          class="border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
        >
          <div class="grid gap-4 xl:grid-cols-2">
            <div>
              <p class="text-xs text-neutral-500 dark:text-neutral-400">Image</p>
              <p
                class="mt-1 wrap-break-word text-sm font-medium text-neutral-900 dark:text-neutral-100"
              >
                {{ container.image || '—' }}
              </p>
            </div>
            <div>
              <p class="text-xs text-neutral-500 dark:text-neutral-400">State</p>
              <p class="mt-1 text-sm font-medium capitalize text-neutral-900 dark:text-neutral-100">
                {{ container.state }}
              </p>
            </div>
            <div v-if="container.state === 'running'">
              <p class="text-xs text-neutral-500 dark:text-neutral-400">Uptime</p>
              <p class="mt-1 text-sm font-medium text-neutral-900 dark:text-neutral-100">
                {{ container.started_at ? formatUptime(container.started_at) : 'Unavailable' }}
              </p>
            </div>
            <div>
              <p class="text-xs text-neutral-500 dark:text-neutral-400">Container ID</p>
              <p class="mt-1 break-all font-mono text-xs text-neutral-700 dark:text-neutral-300">
                {{ container.id || '—' }}
              </p>
            </div>
            <div>
              <p class="text-xs text-neutral-500 dark:text-neutral-400">Image SHA</p>
              <p class="mt-1 break-all font-mono text-xs text-neutral-700 dark:text-neutral-300">
                {{ container.image_sha || '—' }}
              </p>
            </div>
            <div>
              <p class="text-xs text-neutral-500 dark:text-neutral-400">Ports</p>
              <p class="mt-1 wrap-break-word text-sm text-neutral-700 dark:text-neutral-300">
                {{ container.ports.length ? container.ports.join(', ') : 'No published ports' }}
              </p>
            </div>
          </div>
          <div class="mt-4 border-t border-neutral-100 pt-4 dark:border-neutral-800">
            <n-button text size="small" class="px-0!" @click="labelsExpanded = !labelsExpanded">
              <template #icon
                ><ChevronDown v-if="labelsExpanded" :size="15" /><ChevronRight v-else :size="15"
              /></template>
              Labels{{ container.labels.length ? ` (${container.labels.length})` : '' }}
            </n-button>
            <div v-if="labelsExpanded && container.labels.length" class="mt-2 grid gap-1">
              <code
                v-for="label in container.labels"
                :key="label"
                class="break-all text-xs text-neutral-700 dark:text-neutral-300"
                >{{ label }}</code
              >
            </div>
            <p
              v-else-if="labelsExpanded"
              class="mt-1 text-sm text-neutral-500 dark:text-neutral-400"
            >
              No labels
            </p>
          </div>
        </n-card>
        <MetricsWindowPicker
          :window="timeWindow"
          :custom-from="customFrom"
          :custom-to="customTo"
          :resolution-label="resolutionLabel"
          @update:window="updateWindow"
          @update:custom-from="updateCustomFrom"
          @update:custom-to="updateCustomTo"
        />
        <n-empty v-if="!containerSeriesEntries.length" description="No metrics found" />
        <div v-else class="grid min-w-0 gap-4 lg:grid-cols-2">
          <n-card
            size="small"
            :bordered="false"
            class="min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
          >
            <n-statistic label="CPU" :value="latest ? `${latest.cpu_pct.toFixed(1)}%` : '—'" />
            <MetricChart :series="cpuSeries" :format-value="(value) => `${value.toFixed(1)}%`" />
          </n-card>
          <n-card
            size="small"
            :bordered="false"
            class="min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
          >
            <n-statistic
              label="Memory used / limit"
              :value="
                latest ? `${formatBytes(latest.mem_used)} / ${formatBytes(latest.mem_limit)}` : '—'
              "
            />
            <MetricChart :series="memorySeries" :format-value="formatBytes" />
          </n-card>
          <n-card
            size="small"
            :bordered="false"
            class="min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
          >
            <n-statistic label="Network In" :value="formatRate(latestNetworkIn)" />
            <MetricChart :series="networkInSeries" :format-value="formatRate" />
          </n-card>
          <n-card
            size="small"
            :bordered="false"
            class="min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
          >
            <n-statistic label="Network Out" :value="formatRate(latestNetworkOut)" />
            <MetricChart :series="networkOutSeries" :format-value="formatRate" />
          </n-card>
          <n-card
            size="small"
            :bordered="false"
            class="min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
          >
            <n-statistic label="Disk Read" :value="formatRate(latestDiskRead)" />
            <MetricChart :series="diskReadSeries" :format-value="formatRate" />
          </n-card>
          <n-card
            size="small"
            :bordered="false"
            class="min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
          >
            <n-statistic label="Disk Write" :value="formatRate(latestDiskWrite)" />
            <MetricChart :series="diskWriteSeries" :format-value="formatRate" />
          </n-card>
        </div>
      </template>
    </main>
  </div>
</template>
