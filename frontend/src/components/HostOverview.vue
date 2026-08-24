<script setup lang="ts">
import { computed } from 'vue'
import { NCard, NStatistic } from 'naive-ui'

import MetricChart, { type ChartPoint, type ChartSeries } from './MetricChart.vue'
import { colorForKey } from '../colors'
import { formatBytes } from '../format'
import { latestFreshSamples } from '../metricsFreshness'
import type { ContainerGroupSample, HostSample } from '../types'

const props = withDefaults(
  defineProps<{
    sample?: HostSample
    history: HostSample[]
    containerHistory: ContainerGroupSample[]
    staleAfterMs?: number
  }>(),
  { staleAfterMs: 15_000 }
)

function points(key: 'cpu_pct' | 'mem_used') {
  return props.history.map((sample) => ({ ts: sample.ts, value: sample[key] }))
}

function ratePoints(key: 'net_rx' | 'net_tx' | 'disk_read' | 'disk_write'): ChartPoint[] {
  return props.history.map((sample, index) => {
    const previous = props.history[index - 1]
    if (!previous || sample.ts <= previous.ts || sample[key] < previous[key])
      return { ts: sample.ts, value: 0 }
    return {
      ts: sample.ts,
      value: (sample[key] - previous[key]) / ((sample.ts - previous.ts) / 1_000),
    }
  })
}

const cpuPoints = computed(() => points('cpu_pct'))
const memoryPoints = computed(() => points('mem_used'))
const storagePoints = computed(() =>
  props.history.map((sample) => ({
    ts: sample.ts,
    value: sample.storage_total ? (sample.storage_used / sample.storage_total) * 100 : 0,
  }))
)
const networkReceivedPoints = computed(() => ratePoints('net_rx'))
const networkSentPoints = computed(() => ratePoints('net_tx'))
const diskReadPoints = computed(() => ratePoints('disk_read'))
const diskWritePoints = computed(() => ratePoints('disk_write'))
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
const containerSeries = computed(() => {
  const grouped = new Map<string, ContainerGroupSample[]>()
  for (const sample of props.containerHistory) {
    grouped.set(sample.log_group, [...(grouped.get(sample.log_group) ?? []), sample])
  }
  return [...grouped.entries()].map(([logGroup, samples]) => ({
    logGroup,
    color: colorForKey(logGroup),
    cpu: samples.map((sample) => ({
      ts: Math.floor(sample.ts / 10_000) * 10_000,
      value: sample.cpu_pct,
    })),
    memory: samples.map((sample) => ({
      ts: Math.floor(sample.ts / 10_000) * 10_000,
      value: sample.mem_used,
    })),
  }))
})
const containerCpuSeries = computed<ChartSeries[]>(() =>
  containerSeries.value.map((series) => ({
    name: series.logGroup,
    color: series.color,
    points: series.cpu,
  }))
)
const containerMemorySeries = computed<ChartSeries[]>(() =>
  containerSeries.value.map((series) => ({
    name: series.logGroup,
    color: series.color,
    points: series.memory,
  }))
)

const latestContainerSamples = computed(() =>
  latestFreshSamples(props.containerHistory, (sample) => sample.log_group, props.staleAfterMs)
)
const containerCpuSummary = computed(
  () =>
    `${latestContainerSamples.value.reduce((total, sample) => total + sample.cpu_pct, 0).toFixed(1)}%`
)
const containerMemorySummary = computed(() => {
  const used = latestContainerSamples.value.reduce((total, sample) => total + sample.mem_used, 0)
  return formatBytes(used)
})

function formatRate(value: number) {
  return `${formatBytes(value)}/s`
}

function formatMemorySummary(sample?: HostSample) {
  return sample ? `${formatBytes(sample.mem_used)} / ${formatBytes(sample.mem_total)}` : '—'
}

function formatCpuSummary(sample?: HostSample) {
  return sample ? `${sample.cpu_pct.toFixed(1)}%` : '—'
}

function formatStorageSummary(sample?: HostSample) {
  return sample && sample.storage_total
    ? `${formatBytes(sample.storage_used)} / ${formatBytes(sample.storage_total)}`
    : '—'
}

function formatDatabaseStorageSummary(sample?: HostSample) {
  return sample ? formatBytes(sample.metrics_size + sample.logs_size) : '—'
}

function latest(points: ChartPoint[]) {
  return points[points.length - 1]?.value
}
</script>

<template>
  <div class="grid min-w-0 gap-4 lg:grid-cols-2">
    <n-card
      size="small"
      :bordered="false"
      class="order-1 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <n-statistic label="Host CPU" :value="formatCpuSummary(sample)" />
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
      <n-statistic label="Memory used / total" :value="formatMemorySummary(sample)" />
      <MetricChart :points="memoryPoints" :format-value="formatBytes" />
    </n-card>
    <n-card
      size="small"
      :bordered="false"
      class="order-7 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <n-statistic label="Storage used / total" :value="formatStorageSummary(sample)" />
      <MetricChart :points="storagePoints" :format-value="(value) => `${value.toFixed(1)}%`" />
    </n-card>
    <n-card
      size="small"
      :bordered="false"
      class="order-8 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <n-statistic label="Database storage" :value="formatDatabaseStorageSummary(sample)" />
      <MetricChart :series="databaseSizeSeries" :format-value="formatBytes" />
    </n-card>
    <n-card
      v-if="containerCpuSeries.length"
      size="small"
      :bordered="false"
      class="order-3 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <n-statistic label="Container CPU" :value="containerCpuSummary" />
      <MetricChart :series="containerCpuSeries" :format-value="(value) => `${value.toFixed(1)}%`" />
    </n-card>
    <n-card
      v-if="containerMemorySeries.length"
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
        <n-statistic
          label="Network in"
          :value="
            sample && latest(networkReceivedPoints) !== undefined
              ? formatRate(latest(networkReceivedPoints)!)
              : '—'
          "
        />
        <n-statistic
          label="Network out"
          :value="
            sample && latest(networkSentPoints) !== undefined
              ? formatRate(latest(networkSentPoints)!)
              : '—'
          "
        />
      </div>
      <MetricChart :series="networkSeries" :format-value="formatRate" />
    </n-card>
    <n-card
      size="small"
      :bordered="false"
      class="order-6 min-w-0 border border-neutral-200 bg-white dark:border-neutral-800 dark:bg-neutral-900"
    >
      <div class="grid grid-cols-2 gap-3">
        <n-statistic
          label="Disk read"
          :value="
            sample && latest(diskReadPoints) !== undefined
              ? formatRate(latest(diskReadPoints)!)
              : '—'
          "
        />
        <n-statistic
          label="Disk write"
          :value="
            sample && latest(diskWritePoints) !== undefined
              ? formatRate(latest(diskWritePoints)!)
              : '—'
          "
        />
      </div>
      <MetricChart :series="diskSeries" :format-value="formatRate" />
    </n-card>
  </div>
</template>
