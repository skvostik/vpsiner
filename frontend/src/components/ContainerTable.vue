<script setup lang="ts">
import { NButton, NCard, NEmpty, NSpin, NTag } from 'naive-ui'
import { Logs } from '@lucide/vue'

import { formatBytes, formatUptime } from '../format'
import type { ContainerRow, ContainerState } from '../types'

defineProps<{
  rows: ContainerRow[]
  loading: boolean
}>()

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
      class="w-full min-w-0 cursor-pointer overflow-hidden border border-neutral-200 bg-white transition-colors hover:border-cyan-400 dark:border-neutral-800 dark:bg-neutral-900 dark:hover:border-cyan-700"
      role="link"
      tabindex="0"
      @click="
        $router.push({
          name: 'container-detail',
          params: { id: row.id },
        })
      "
      @keydown.enter="
        $router.push({
          name: 'container-detail',
          params: { id: row.id },
        })
      "
    >
      <article
        class="w-full min-w-0 overflow-hidden lg:grid lg:grid-cols-[minmax(12rem,1fr)_minmax(12rem,16rem)_minmax(16rem,1fr)_auto] lg:items-center lg:gap-4"
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
                {{ row.started_at ? `Up ${formatUptime(row.started_at)}` : 'Uptime unavailable' }}
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
                  ? `${formatBytes(row.metrics.net_rx_rate)}/s / ${formatBytes(row.metrics.net_tx_rate)}/s`
                  : '—'
              }}
            </dd>
          </div>
          <div>
            <dt class="text-xs text-neutral-500 dark:text-neutral-400">Disk</dt>
            <dd class="mt-1 whitespace-nowrap font-medium text-neutral-900 dark:text-neutral-100">
              {{
                row.metrics
                  ? `${formatBytes(row.metrics.disk_read_rate)}/s / ${formatBytes(row.metrics.disk_write_rate)}/s`
                  : '—'
              }}
            </dd>
          </div>
        </dl>

        <div class="mt-4 flex justify-end lg:mt-0 lg:justify-self-end">
          <router-link
            :to="{ name: 'log-viewer', params: { logGroup: row.log_group } }"
            custom
            v-slot="{ navigate }"
          >
            <n-button
              secondary
              size="small"
              aria-label="Open container logs"
              @click.stop="navigate"
            >
              <template #icon><Logs :size="15" /></template>
              Logs
            </n-button>
          </router-link>
        </div>
      </article>
    </n-card>
  </div>
  <n-empty v-else-if="!loading" description="No visible containers found" />
  <div v-else class="flex justify-center py-12"><n-spin /></div>
</template>
