<script setup lang="ts">
import { computed, onMounted, ref, watch, type ComputedRef, type Ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ListFilter, RefreshCw } from '@lucide/vue'
import { NButton, NEmpty, NTooltip } from 'naive-ui'

import LogFilterBar from '../components/logs/LogFilterBar.vue'
import LiveStatusIcon from '../components/LiveStatusIcon.vue'
import LogViewer from '../components/logs/LogViewer.vue'
import { backendOnline, dockerConnected } from '../composables/useBackendHealth'
import { useLogs } from '../composables/useLogs'
import { usePageTitle } from '../composables/usePageTitle'
import type { Services, LogLevel, LogLine, LogStream } from '../types'

type LogsState = {
  services: Ref<Services>
  selectedService: Ref<string>
  logs: Ref<LogLine[]>
  query: Ref<string>
  level: Ref<LogLevel[]>
  stream: Ref<LogStream[]>
  customFrom: Ref<number | undefined>
  customTo: Ref<number | undefined>
  loadingServices: Ref<boolean>
  loadingLogs: Ref<boolean>
  hasMore: Ref<boolean>
  hasNewer: Ref<boolean>
  tailing: ComputedRef<boolean>
  freshKeys: Ref<Set<string>>
  error: Ref<string>
  queryTooShort: Ref<boolean>
  loadLogs: () => Promise<void>
  loadMore: () => Promise<void>
  loadNewer: () => Promise<void>
  reportAtBottom: (value: boolean) => void
  updateQuery: (value: string) => void
  updateLevel: (value: LogLevel[]) => void
  updateStream: (value: LogStream[]) => void
  updateCustomFrom: (value: number | null) => void
  updateCustomTo: (value: number | null) => void
}

const route = useRoute()
const router = useRouter()
const filtersExpanded = ref(false)
const filtersExpandedStorageKey = 'vpsiner.logs.filters-expanded.v1'
const routeService = computed(() =>
  typeof route.params.service === 'string' ? route.params.service : undefined
)
const logsState = useLogs(routeService.value) as LogsState
const {
  services,
  selectedService,
  logs,
  query,
  level,
  stream,
  customFrom,
  customTo,
  loadingServices,
  loadingLogs,
  hasMore,
  hasNewer,
  tailing,
  freshKeys,
  error,
  queryTooShort,
  loadLogs,
  loadMore,
  loadNewer,
  reportAtBottom,
  updateQuery,
  updateLevel,
  updateStream,
  updateCustomFrom,
  updateCustomTo,
} = logsState
const selectedServiceSummary = computed(() => services.value[selectedService.value])
const pageStatus = computed<'live' | 'history' | 'stopped' | 'docker-error'>(() => {
  if (!backendOnline.value || !selectedService.value) return 'stopped'
  if (!dockerConnected.value) return 'docker-error'
  if (!selectedServiceSummary.value?.live) return 'stopped'
  return tailing.value ? 'live' : 'history'
})
// A lower time bound, text search, or level/stream filter can all hide logs older than what's
// currently loaded, so "no more older logs" alone doesn't mean the service's history truly ends here.
const hasActiveFilters = computed(
  () =>
    !!query.value ||
    level.value.length > 0 ||
    stream.value.length > 0 ||
    customFrom.value !== undefined
)

watch(routeService, (service) => {
  if (service && service !== selectedService.value) selectedService.value = service
})
watch(selectedService, (service) => {
  if (service && service !== routeService.value)
    router.replace({ name: 'log-viewer', params: { service } })
})
usePageTitle(() => selectedService.value || 'Logs')
watch(filtersExpanded, (expanded) => {
  localStorage.setItem(filtersExpandedStorageKey, String(expanded))
})

onMounted(() => {
  const saved = localStorage.getItem(filtersExpandedStorageKey)
  if (saved !== null) filtersExpanded.value = saved === 'true'
})
</script>

<template>
  <Teleport to="#app-header-title-leading">
    <LiveStatusIcon :status="pageStatus" :size="15" :pulse="pageStatus === 'live'" />
  </Teleport>
  <Teleport to="#app-header-title-subtext">
    <template v-if="selectedServiceSummary">
      <span v-if="selectedServiceSummary.last_received === null">No logs received yet</span>
      <span v-else>
        Last log received
        <time :datetime="new Date(selectedServiceSummary.last_received).toISOString()">
          {{ new Date(selectedServiceSummary.last_received).toLocaleString() }}
        </time>
      </span>
    </template>
  </Teleport>
  <Teleport to="#app-header-actions">
    <n-tooltip>
      <template #trigger>
        <n-button
          tertiary
          :aria-label="filtersExpanded ? 'Hide filters' : 'Show filters'"
          @click="filtersExpanded = !filtersExpanded"
        >
          <template #icon><ListFilter :size="16" /></template>
        </n-button>
      </template>
      {{ filtersExpanded ? 'Hide filters' : 'Show filters' }}
    </n-tooltip>
    <n-button tertiary aria-label="Refresh logs" :loading="loadingLogs" @click="loadLogs">
      <template #icon><RefreshCw :size="16" /></template>
    </n-button>
  </Teleport>
  <Teleport to="#app-header-controls">
    <LogFilterBar
      :query="query"
      :level="level"
      :stream="stream"
      :expanded="filtersExpanded"
      :custom-from="customFrom"
      :custom-to="customTo"
      :query-too-short="queryTooShort"
      @update:query="updateQuery"
      @update:level="updateLevel"
      @update:stream="updateStream"
      @update:custom-from="updateCustomFrom"
      @update:custom-to="updateCustomTo"
    />
  </Teleport>
  <div class="space-y-5">
    <div v-if="error" class="text-sm text-red-600 dark:text-red-400">{{ error }}</div>
    <LogViewer
      :lines="logs"
      :loading="loadingLogs"
      :has-more="hasMore"
      :has-newer="hasNewer"
      :query="query"
      :tailing="tailing"
      :has-stored-logs="selectedServiceSummary?.last_received != null"
      :has-active-filters="hasActiveFilters"
      :fresh-keys="freshKeys"
      :load-older="loadMore"
      :load-newer="loadNewer"
      @at-bottom-change="reportAtBottom"
      @edit-filters="filtersExpanded = true"
    />
    <n-empty v-if="!selectedService && !loadingServices" description="No service selected" />
  </div>
</template>
