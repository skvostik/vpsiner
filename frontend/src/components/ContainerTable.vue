<script setup lang="ts">
import { computed } from 'vue'
import { NButton, NCard, NEmpty, NSpin, NTag, NTooltip, useMessage } from 'naive-ui'
import { Play, RotateCcw, Square } from '@lucide/vue'

import { dockerControlsAvailable } from '../composables/useBackendHealth'
import { pendingAction, runContainerAction } from '../composables/useContainerActions'
import { useNow } from '../composables/useNow'
import { formatBytes, formatRate, formatUptime } from '../format'
import type { ContainerRow, ContainerState } from '../types'

defineProps<{
  rows: ContainerRow[]
  loading: boolean
}>()

const message = useMessage()
const now = useNow()

const canControl = computed(() => dockerControlsAvailable.value)

function supportsStart(state: ContainerState) {
  return !['running', 'restarting'].includes(state)
}

function supportsStop(state: ContainerState) {
  return ['running', 'paused', 'restarting'].includes(state)
}

function supportsRestart(state: ContainerState) {
  return ['running', 'paused'].includes(state)
}

async function runAction(row: ContainerRow, action: 'start' | 'stop' | 'restart') {
  try {
    await runContainerAction(row, action)
  } catch (actionError) {
    const text = actionError instanceof Error ? actionError.message : 'Container action failed'
    message.error(text)
  }
}

function stateType(state: ContainerState) {
  if (state === 'running') return 'success'
  if (state === 'exited' || state === 'created') return 'default'
  if (state === 'dead') return 'error'
  return 'warning'
}
</script>

<template>
  <div v-if="rows.length" class="grid gap-3">
    <n-card
      v-for="row in rows"
      :key="row.id"
      :bordered="false"
      class="relative w-full min-w-0 overflow-hidden border border-neutral-200 bg-white transition-colors hover:border-cyan-400 dark:border-neutral-800 dark:bg-neutral-900 dark:hover:border-cyan-700"
    >
      <router-link
        :to="{ name: 'container-detail', params: { id: row.id } }"
        :aria-label="`Open details for ${row.name}`"
        class="absolute inset-0 z-10"
      />
      <article
        class="relative pointer-events-none w-full min-w-0 overflow-hidden lg:grid lg:grid-cols-[minmax(12rem,1fr)_minmax(12rem,16rem)_minmax(16rem,1fr)_7rem] lg:items-center lg:gap-4"
      >
        <div class="min-w-0 overflow-hidden">
          <div class="flex min-w-0 items-center justify-between gap-3">
            <div class="min-w-0">
              <h3 class="truncate font-semibold text-neutral-900 dark:text-neutral-50">
                {{ row.name }}
              </h3>
              <p class="truncate font-mono text-xs text-neutral-400 dark:text-neutral-500">
                {{ row.id.slice(0, 12) }}
              </p>
              <p
                v-if="row.state === 'running'"
                class="mt-1 text-xs text-neutral-500 dark:text-neutral-400"
              >
                {{
                  row.started_at ? `Up ${formatUptime(row.started_at, now)}` : 'Uptime unavailable'
                }}
              </p>
            </div>
            <n-tag :type="stateType(row.state)" size="small" class="shrink-0">{{
              row.state
            }}</n-tag>
          </div>
        </div>

        <div class="mt-3 min-w-0 lg:mt-0">
          <p class="truncate text-xs text-neutral-500 dark:text-neutral-400">{{ row.image }}</p>
          <p class="mt-1 truncate text-xs text-neutral-400 dark:text-neutral-500">
            {{ row.ports.length ? row.ports.join(', ') : 'No published ports' }}
          </p>
        </div>

        <dl
          class="mt-4 grid grid-cols-2 gap-3 border-y border-neutral-100 py-3 text-sm dark:border-neutral-800 lg:mt-0 lg:border-y-0 lg:border-l lg:py-0 lg:pl-4"
        >
          <div>
            <dt class="text-xs text-neutral-500 dark:text-neutral-400">CPU</dt>
            <dd class="mt-1 whitespace-nowrap font-medium text-neutral-900 dark:text-neutral-100">
              {{ row.metrics ? `${row.metrics.cpu_pct.toFixed(1)}%` : '—' }}
            </dd>
          </div>
          <div>
            <dt class="text-xs text-neutral-500 dark:text-neutral-400">Memory</dt>
            <dd class="mt-1 whitespace-nowrap font-medium text-neutral-900 dark:text-neutral-100">
              {{ row.metrics ? `${formatBytes(row.metrics.mem_used)}` : '—' }}
            </dd>
          </div>
          <div>
            <dt class="text-xs text-neutral-500 dark:text-neutral-400">Net</dt>
            <dd class="mt-1 whitespace-nowrap font-medium text-neutral-900 dark:text-neutral-100">
              {{
                row.metrics
                  ? `${formatRate(row.metrics.net_rx_rate)} / ${formatRate(row.metrics.net_tx_rate)}`
                  : '—'
              }}
            </dd>
          </div>
          <div>
            <dt class="text-xs text-neutral-500 dark:text-neutral-400">Disk</dt>
            <dd class="mt-1 whitespace-nowrap font-medium text-neutral-900 dark:text-neutral-100">
              {{
                row.metrics
                  ? `${formatRate(row.metrics.blk_read_rate)} / ${formatRate(row.metrics.blk_write_rate)}`
                  : '—'
              }}
            </dd>
          </div>
        </dl>

        <div
          v-if="canControl"
          class="relative z-20 mt-4 flex w-full justify-end gap-2 pointer-events-auto lg:mt-0 lg:justify-self-end"
        >
          <n-tooltip v-if="supportsStart(row.state)">
            <template #trigger>
              <n-button
                circle
                tertiary
                type="primary"
                class="w-8! h-8!"
                :loading="pendingAction(row.id) === 'start'"
                :disabled="!!pendingAction(row.id) && pendingAction(row.id) !== 'start'"
                aria-label="Start container"
                @click="runAction(row, 'start')"
              >
                <template #icon><Play :size="15" /></template>
              </n-button>
            </template>
            Start container
          </n-tooltip>
          <n-tooltip v-if="supportsStop(row.state)">
            <template #trigger>
              <n-button
                circle
                tertiary
                type="error"
                class="w-8! h-8!"
                :loading="pendingAction(row.id) === 'stop'"
                :disabled="!!pendingAction(row.id) && pendingAction(row.id) !== 'stop'"
                aria-label="Stop container"
                @click="runAction(row, 'stop')"
              >
                <template #icon><Square :size="14" /></template>
              </n-button>
            </template>
            Stop container
          </n-tooltip>
          <n-tooltip v-if="supportsRestart(row.state)">
            <template #trigger>
              <n-button
                circle
                tertiary
                class="w-8! h-8!"
                :loading="pendingAction(row.id) === 'restart'"
                :disabled="!!pendingAction(row.id) && pendingAction(row.id) !== 'restart'"
                aria-label="Restart container"
                @click="runAction(row, 'restart')"
              >
                <template #icon><RotateCcw :size="15" /></template>
              </n-button>
            </template>
            Restart container
          </n-tooltip>
        </div>
      </article>
    </n-card>
  </div>
  <n-empty v-else-if="!loading" description="No visible containers found" />
  <div v-else class="flex justify-center py-12"><n-spin /></div>
</template>
