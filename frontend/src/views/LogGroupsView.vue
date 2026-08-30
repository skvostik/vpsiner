<script setup lang="ts">
import { computed, ref } from 'vue'
import { NEmpty, NInput, NSpin, NSwitch } from 'naive-ui'
import { ChevronRight, Search } from '@lucide/vue'

import LiveStatusIcon from '../components/LiveStatusIcon.vue'
import { backendOnline } from '../composables/useBackendHealth'
import { useLogGroupsStream } from '../composables/useLogGroupsStream'
import { usePageTitle } from '../composables/usePageTitle'

usePageTitle('Explore Logs')

const { groups, loading } = useLogGroupsStream()
const onlyRunning = ref(false)
const groupSearch = ref('')
const onlyRunningStorageKey = 'vpsiner.log-groups.only-running.v2'

const pageStatus = computed<'live' | 'history' | 'stopped'>(() => {
  if (!backendOnline.value) return 'stopped'
  return document.visibilityState === 'visible' ? 'live' : 'history'
})

const sortedGroups = computed(() => {
  const search = groupSearch.value.trim().toLocaleLowerCase()
  return Object.entries(groups.value)
    .filter(
      ([logGroup, status]) =>
        (!onlyRunning.value || status.live) &&
        (!search || logGroup.toLocaleLowerCase().includes(search))
    )
    .map(([log_group, status]) => ({
      log_group,
      ...status,
    }))
    .sort((left, right) =>
      left.log_group.localeCompare(right.log_group, undefined, {
        sensitivity: 'base',
        numeric: true,
      })
    )
})

function formatLastReceived(value: number | null) {
  return value ? new Date(value).toLocaleString() : 'No logs yet'
}

function updateOnlyRunning(value: boolean) {
  onlyRunning.value = value
  window.localStorage.setItem(onlyRunningStorageKey, String(value))
}

const storedOnlyRunning = window.localStorage.getItem(onlyRunningStorageKey)
onlyRunning.value = storedOnlyRunning === null ? true : storedOnlyRunning === 'true'
</script>

<template>
  <Teleport to="#app-header-title-leading">
    <LiveStatusIcon :status="pageStatus" :size="15" :pulse="pageStatus === 'live'" />
  </Teleport>
  <div class="space-y-5">
    <div class="flex flex-wrap items-center justify-end gap-4">
      <label
        class="flex items-center gap-2 whitespace-nowrap text-xs text-neutral-500 dark:text-neutral-400"
      >
        <span>Show only running</span>
        <n-switch :value="onlyRunning" size="small" @update:value="updateOnlyRunning" />
      </label>
    </div>
    <n-input v-model:value="groupSearch" clearable placeholder="Search by name">
      <template #prefix><Search :size="16" /></template>
    </n-input>
    <n-spin v-if="loading" size="small" />
    <n-empty
      v-else-if="!sortedGroups.length"
      :description="
        groupSearch.trim()
          ? 'No matching log groups'
          : onlyRunning
            ? 'No running log groups'
            : 'No log groups found'
      "
    />
    <ul
      v-else
      class="divide-y divide-neutral-200 rounded border border-neutral-200 dark:divide-neutral-800 dark:border-neutral-800"
    >
      <li v-for="group in sortedGroups" :key="group.log_group">
        <router-link
          :to="{ name: 'log-viewer', params: { logGroup: group.log_group } }"
          class="flex items-center justify-between gap-4 px-4 py-3 hover:bg-neutral-50 dark:hover:bg-neutral-900"
        >
          <span class="flex min-w-0 items-center gap-3">
            <LiveStatusIcon :live="group.live" />
            <span class="truncate text-sm font-medium text-neutral-900 dark:text-neutral-100">{{
              group.log_group
            }}</span>
          </span>
          <span
            class="flex shrink-0 items-center gap-2 text-xs text-neutral-500 dark:text-neutral-400"
          >
            {{ formatLastReceived(group.last_received) }}
            <ChevronRight :size="16" />
          </span>
        </router-link>
      </li>
    </ul>
  </div>
</template>
