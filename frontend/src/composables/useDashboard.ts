import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useMessage } from 'naive-ui'

import { api, containerMetricsHistory } from '../api'
import type {
  ContainerGroupSample,
  ContainerOverviewMetrics,
  ContainerRow,
  HostSample,
} from '../types'

// Shared state: the sidebar badge and the dashboard views run off a single poll loop.
const containers = ref<ContainerRow[]>([])
const hostSamples = ref<HostSample[]>([])
const hostSample = ref<HostSample>()
const containerMetricHistory = ref<ContainerGroupSample[]>([])
const loading = ref(true)
const error = ref('')
const lastUpdated = ref<Date>()
const runningCount = computed(
  () => containers.value.filter((item) => item.state === 'running').length
)
let message: ReturnType<typeof useMessage> | undefined
let pollTimer: number | undefined
let consumers = 0
let lastReportedError = ''

function reportError(value: unknown, fallback: string) {
  const text = value instanceof Error ? value.message : fallback
  error.value = text
  if (text !== lastReportedError) {
    message?.error(text)
    lastReportedError = text
  }
}

async function load() {
  if (document.visibilityState !== 'visible') return
  try {
    error.value = ''
    const containerList = await api.containers.list()

    try {
      const to = Date.now()
      const hostMetrics = await api.host.metrics({ from: to - 60 * 60 * 1000, to }, '1m')
      hostSamples.value = hostMetrics
      hostSample.value = hostMetrics.slice(-1)[0]
    } catch (metricsError) {
      reportError(metricsError, 'Unable to load host metrics')
    }

    try {
      const to = Date.now()
      const history = await containerMetricsHistory({ from: to - 60 * 60 * 1000, to }, '1m')
      containerMetricHistory.value = Object.values(history).flat()
      const metricsByGroup = new Map<string, ContainerOverviewMetrics>()
      for (const [group, samples] of Object.entries(history)) {
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
      containers.value = containerList.map((container) => ({
        ...container,
        metrics: metricsByGroup.get(container.log_group),
      }))
    } catch (metricsError) {
      reportError(metricsError, 'Unable to load container metric history')
    }

    if (!containers.value.length) containers.value = containerList
    lastUpdated.value = new Date()
  } catch (loadError) {
    reportError(loadError, 'Unable to load dashboard data')
  } finally {
    loading.value = false
  }
}

export function useDashboard() {
  message = useMessage()

  onMounted(() => {
    consumers += 1
    if (consumers > 1) return
    load()
    pollTimer = window.setInterval(load, 5_000)
  })

  onBeforeUnmount(() => {
    consumers -= 1
    if (consumers > 0 || !pollTimer) return
    window.clearInterval(pollTimer)
    pollTimer = undefined
  })

  return {
    containers,
    hostSamples,
    hostSample,
    containerMetricHistory,
    loading,
    error,
    lastUpdated,
    runningCount,
  }
}
