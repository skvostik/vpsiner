<script setup lang="ts">
import { computed, onMounted, ref, watch, type ComputedRef, type Ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ListFilter, RefreshCw } from '@lucide/vue'
import { NButton, NEmpty, NTooltip } from 'naive-ui'

import LogFilterBar from '../components/logs/LogFilterBar.vue'
import LogGroupStatusIcon from '../components/logs/LogGroupStatusIcon.vue'
import LogViewer from '../components/logs/LogViewer.vue'
import { useLogs } from '../composables/useLogs'
import { usePageTitle } from '../composables/usePageTitle'
import type { LogGroups, LogLevel, LogLine, LogStream, LogWindow } from '../types'

type LogsState = {
  groups: Ref<LogGroups>
  selectedGroup: Ref<string>
  logs: Ref<LogLine[]>
  query: Ref<string>
  level: Ref<LogLevel[]>
  stream: Ref<LogStream[]>
  timeWindow: Ref<LogWindow>
  customFrom: Ref<number | undefined>
  customTo: Ref<number | undefined>
  loadingGroups: Ref<boolean>
  loadingLogs: Ref<boolean>
  hasMore: Ref<boolean>
  hasNewer: Ref<boolean>
  tailing: ComputedRef<boolean>
  freshKeys: Ref<Set<string>>
  error: Ref<string>
  loadLogs: () => Promise<void>
  loadMore: () => Promise<void>
  loadNewer: () => Promise<void>
  reportAtBottom: (value: boolean) => void
  updateQuery: (value: string) => void
  updateLevel: (value: LogLevel[]) => void
  updateStream: (value: LogStream[]) => void
  updateWindow: (value: LogWindow) => void
  updateCustomFrom: (value: number | null) => void
  updateCustomTo: (value: number | null) => void
}

const route = useRoute()
const router = useRouter()
const filtersExpanded = ref(false)
const filtersExpandedStorageKey = 'vpsiner.logs.filters-expanded.v1'
const routeGroup = computed(() =>
  typeof route.params.logGroup === 'string' ? route.params.logGroup : undefined
)
const logsState = useLogs(routeGroup.value) as LogsState
const {
  groups,
  selectedGroup,
  logs,
  query,
  level,
  stream,
  timeWindow,
  customFrom,
  customTo,
  loadingGroups,
  loadingLogs,
  hasMore,
  hasNewer,
  tailing,
  freshKeys,
  error,
  loadLogs,
  loadMore,
  loadNewer,
  reportAtBottom,
  updateQuery,
  updateLevel,
  updateStream,
  updateWindow,
  updateCustomFrom,
  updateCustomTo,
} = logsState
const selectedGroupSummary = computed(() => groups.value[selectedGroup.value])

watch(routeGroup, (group) => {
  if (group && group !== selectedGroup.value) selectedGroup.value = group
})
watch(selectedGroup, (group) => {
  if (group && group !== routeGroup.value)
    router.replace({ name: 'log-viewer', params: { logGroup: group } })
})
usePageTitle(() => selectedGroup.value || 'Logs')
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
    <LogGroupStatusIcon v-if="selectedGroupSummary" :live="selectedGroupSummary.live" :size="15" />
  </Teleport>
  <Teleport to="#app-header-title-subtext">
    <template v-if="selectedGroupSummary">
      <span v-if="selectedGroupSummary.last_received === null">No logs received yet</span>
      <span v-else>
        Last log received
        <time :datetime="new Date(selectedGroupSummary.last_received).toISOString()">
          {{ new Date(selectedGroupSummary.last_received).toLocaleString() }}
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
      :window="timeWindow"
      :expanded="filtersExpanded"
      :custom-from="customFrom"
      :custom-to="customTo"
      @update:query="updateQuery"
      @update:level="updateLevel"
      @update:stream="updateStream"
      @update:window="updateWindow"
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
      :has-stored-logs="selectedGroupSummary?.last_received != null"
      :fresh-keys="freshKeys"
      :load-older="loadMore"
      :load-newer="loadNewer"
      @at-bottom-change="reportAtBottom"
      @edit-filters="filtersExpanded = true"
    />
    <n-empty v-if="!selectedGroup && !loadingGroups" description="No log group selected" />
  </div>
</template>
