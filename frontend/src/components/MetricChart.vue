<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'

export interface ChartPoint {
  ts: number
  value: number | null
}

export interface ChartSeries {
  name: string
  points: ChartPoint[]
  color: string
}

const props = withDefaults(
  defineProps<{
    points?: ChartPoint[]
    series?: ChartSeries[]
    color?: string
    formatValue?: (value: number) => string
  }>(),
  {
    color: '#0891b2',
    formatValue: (value: number) => value.toFixed(1),
  }
)

use([CanvasRenderer, LineChart, GridComponent, TooltipComponent, LegendComponent])

const chartSeries = computed(
  () => props.series ?? [{ name: 'Value', points: props.points ?? [], color: props.color }]
)

function escapeHtml(value: string) {
  return value.replace(
    /[&<>'"]/g,
    (character) =>
      ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        "'": '&#39;',
        '"': '&quot;',
      })[character] ?? character
  )
}

function formatTooltip(params: unknown) {
  const entries = Array.isArray(params) ? params : [params]
  const first = entries[0] as { axisValue?: number } | undefined
  const timestamp = first?.axisValue
  const heading = typeof timestamp === 'number' ? new Date(timestamp).toLocaleString() : ''
  const rows = entries.map((entry) => {
    const item = entry as { seriesName?: string; color?: string; value?: [number, number | null] }
    const value = Array.isArray(item.value) ? item.value[1] : undefined
    return `<div style="display:flex;align-items:center;gap:8px"><span style="width:7px;height:7px;border-radius:50%;background:${item.color ?? '#737373'}"></span><span>${escapeHtml(item.seriesName ?? '')}</span><strong style="margin-left:auto">${value == null ? '—' : props.formatValue(value)}</strong></div>`
  })
  return `<div><div style="margin-bottom:6px;font-weight:600">${escapeHtml(heading)}</div>${rows.join('')}</div>`
}

const option = computed(() => ({
  animation: false,
  grid: {
    top: 12,
    right: 8,
    bottom: chartSeries.value.length > 1 ? 40 : 24,
    left: 8,
    containLabel: true,
  },
  tooltip: {
    trigger: 'axis',
    formatter: formatTooltip,
  },
  xAxis: {
    type: 'time',
    boundaryGap: false,
    axisLabel: { hideOverlap: true, color: '#737373', fontSize: 10 },
    axisLine: { lineStyle: { color: '#d4d4d4' } },
    splitLine: { show: false },
  },
  yAxis: {
    type: 'value',
    scale: true,
    axisLabel: {
      color: '#737373',
      fontSize: 10,
      formatter: (value: number) => props.formatValue(value),
    },
    axisLine: { show: false },
    axisTick: { show: false },
    splitLine: { lineStyle: { color: 'rgba(115, 115, 115, 0.16)' } },
  },
  legend:
    chartSeries.value.length > 1
      ? {
          type: 'scroll',
          bottom: 0,
          left: 'center',
          itemWidth: 12,
          itemHeight: 12,
          textStyle: { color: '#737373', fontSize: 12 },
          pageIconSize: 12,
          pageTextStyle: { color: '#737373', fontSize: 12 },
        }
      : undefined,
  series: chartSeries.value.map((entry) => ({
    name: entry.name,
    type: 'line',
    smooth: true,
    showSymbol: false,
    data: entry.points.map((point) => [point.ts, point.value]),
    lineStyle: { width: 2, color: entry.color },
    itemStyle: { color: entry.color },
  })),
}))
</script>

<template>
  <div class="mt-4 min-w-0 h-40">
    <v-chart :option="option" autoresize class="h-full w-full" aria-label="Metric history chart" />
  </div>
</template>
