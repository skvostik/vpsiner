<script setup lang="ts">
import { computed } from 'vue'
import { NCard, NStatistic } from 'naive-ui'

import MetricChart, { type ChartSeries } from './MetricChart.vue'
import { colorForKey } from '../colors'
import { formatBytes, formatRate } from '../format'
import type { ContainerMetricsByService, HostPoint, MetricsSnapshot } from '../types'

const props = defineProps<{
  snapshot: MetricsSnapshot
  history: HostPoint[]
  containerHistory: ContainerMetricsByService
}>()

const host = computed(() => props.snapshot.host)

function points(
  key: 'cpu_pct' | 'mem_used' | 'net_rx_rate' | 'net_tx_rate' | 'disk_read_rate' | 'disk_write_rate'
) {
  return props.history.map((sample) => ({ ts: sample.ts, value: sample[key] }))
}

const cpuPoints = computed(() => points('cpu_pct'))
const memoryPoints = computed(() => points('mem_used'))
const storagePoints = computed(() =>
  props.history.map((sample) => ({
    ts: sample.ts,
    value: sample.storage_total ? (sample.storage_used / sample.storage_total) * 100 : 0,
  }))
)
const networkReceivedPoints = computed(() => points('net_rx_rate'))
const networkSentPoints = computed(() => points('net_tx_rate'))
const diskReadPoints = computed(() => points('disk_read_rate'))
const diskWritePoints = computed(() => points('disk_write_rate'))
const databaseSizeSeries = computed<ChartSeries[]>(() => [
  {
    name: 'Metrics database',
    points: props.history.map((sample) => ({ ts: sample.ts, value: sample.metrics_size })),
    color: '#0891b2',
  },
  {
    name: 'Logs databases',
    points: props.history.map((sample) => ({ ts: sample.ts, value: sample.logs_size })),
    color: '#f59e0b',
  },
])
const networkSeries = computed<ChartSeries[]>(() => [
  { name: 'In', points: networkReceivedPoints.value, color: '#0891b2' },
  { name: 'Out', points: networkSentPoints.value, color: '#f59e0b' },
])
const diskSeries = computed<ChartSeries[]>(() => [
  { name: 'Read', points: diskReadPoints.value, color: '#0891b2' },
  { name: 'Write', points: diskWritePoints.value, color: '#f59e0b' },
])
const containerSeries = computed(() =>
  Object.entries(props.containerHistory).map(([service, samples]) => ({
    service,
    color: colorForKey(service),
    cpu: samples.map((sample) => ({
      ts: sample.ts,
      value: sample.cpu_pct,
    })),
    memory: samples.map((sample) => ({
      ts: sample.ts,
      value: sample.mem_used,
    })),
  }))
)
const containerCpuSeries = computed<ChartSeries[]>(() =>
  containerSeries.value.map((series) => ({
    name: series.service,
    color: series.color,
    points: series.cpu,
  }))
)
const containerMemorySeries = computed<ChartSeries[]>(() =>
  containerSeries.value.map((series) => ({
    name: series.service,
    color: series.color,
    points: series.memory,
  }))
)

const latestContainerSamples = computed(() => Object.values(props.snapshot.services))
const containerCpuSummary = computed(() => {
  if (!latestContainerSamples.value.length) return '—'
  const used = latestContainerSamples.value.reduce((total, service) => total + service.cpu_pct, 0)
  return `${used.toFixed(1)}%`
})
const containerMemorySummary = computed(() => {
  if (!latestContainerSamples.value.length) return '—'
  const used = latestContainerSamples.value.reduce((total, group) => total + group.mem_used, 0)
  return formatBytes(used)
})

function formatMemorySummary(sample?: HostPoint | null) {
  return sample ? `${formatBytes(sample.mem_used)} / ${formatBytes(sample.mem_total)}` : '—'
}

function formatCpuSummary(sample?: HostPoint | null) {
  return sample ? `${sample.cpu_pct.toFixed(1)}%` : '—'
}

function formatStorageSummary(sample?: HostPoint | null) {
  return sample && sample.storage_total
    ? `${formatBytes(sample.storage_used)} / ${formatBytes(sample.storage_total)}`
    : '—'
}

function formatDatabaseStorageSummary(sample?: HostPoint | null) {
  return sample ? formatBytes(sample.metrics_size + sample.logs_size) : '—'
}
</script>

<template>
  <div class="grid min-w-0 gap-4 lg:grid-cols-2">
    <n-card
      size="small"
      :bordered="false"
      class="order-1 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <n-statistic label="Host CPU" :value="formatCpuSummary(host)" />
      <MetricChart
        :points="cpuPoints"
        color="#0891b2"
        :format-value="(value) => `${value.toFixed(1)}%`"
      />
    </n-card>
    <n-card
      size="small"
      :bordered="false"
      class="order-2 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <n-statistic label="Memory used / total" :value="formatMemorySummary(host)" />
      <MetricChart :points="memoryPoints" :format-value="formatBytes" />
    </n-card>
    <n-card
      size="small"
      :bordered="false"
      class="order-7 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <n-statistic label="Storage used / total" :value="formatStorageSummary(host)" />
      <MetricChart :points="storagePoints" :format-value="(value) => `${value.toFixed(1)}%`" />
    </n-card>
    <n-card
      size="small"
      :bordered="false"
      class="order-8 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <n-statistic label="Database storage" :value="formatDatabaseStorageSummary(host)" />
      <MetricChart :series="databaseSizeSeries" :format-value="formatBytes" />
    </n-card>
    <n-card
      size="small"
      :bordered="false"
      class="order-3 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <n-statistic label="Container CPU" :value="containerCpuSummary" />
      <MetricChart :series="containerCpuSeries" :format-value="(value) => `${value.toFixed(1)}%`" />
    </n-card>
    <n-card
      size="small"
      :bordered="false"
      class="order-4 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <n-statistic label="Container memory" :value="containerMemorySummary" />
      <MetricChart :series="containerMemorySeries" :format-value="formatBytes" />
    </n-card>
    <n-card
      size="small"
      :bordered="false"
      class="order-5 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <div class="grid grid-cols-2 gap-3">
        <n-statistic label="Network in" :value="host ? formatRate(host.net_rx_rate) : '—'" />
        <n-statistic label="Network out" :value="host ? formatRate(host.net_tx_rate) : '—'" />
      </div>
      <MetricChart :series="networkSeries" :format-value="formatRate" />
    </n-card>
    <n-card
      size="small"
      :bordered="false"
      class="order-6 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <div class="grid grid-cols-2 gap-3">
        <n-statistic label="Disk read" :value="host ? formatRate(host.disk_read_rate) : '—'" />
        <n-statistic label="Disk write" :value="host ? formatRate(host.disk_write_rate) : '—'" />
      </div>
      <MetricChart :series="diskSeries" :format-value="formatRate" />
    </n-card>
  </div>
</template>
