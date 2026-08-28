<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import { NBadge } from 'naive-ui'
import { Boxes, Gauge, Logs } from '@lucide/vue'

import { useContainers } from '../composables/useContainers'
import { backendVersion, retentionWeeks } from '../composables/useBackendHealth'

const docsUrl = 'https://github.com/skvostik/vpsiner'

const emit = defineEmits<{ navigate: [] }>()
const route = useRoute()
const { runningCount } = useContainers()

const navItems = computed(() => [
  { key: 'host', label: 'Host Metrics', icon: Gauge },
  { key: 'containers', label: 'Containers', icon: Boxes },
  { key: 'logs', label: 'Explore Logs', icon: Logs },
])

const activeKey = () => {
  if (route.name === 'containers' || route.name === 'container-detail') return 'containers'
  if (route.name === 'logs' || route.name === 'log-viewer') return 'logs'
  return 'host'
}
</script>

<template>
  <div class="flex h-full flex-col">
    <div class="px-4 py-4">
      <p
        class="text-xs mb-0 pb-0 font-semibold uppercase tracking-[0.2em] text-cyan-700 dark:text-cyan-400"
      >
        Simply Observed
      </p>
      <span
        class="mt-0 pt-0 block text-xl font-extrabold tracking-tight text-neutral-900 dark:text-neutral-50"
        >VPSiner</span
      >
    </div>
    <nav class="flex-1 px-2">
      <router-link
        v-for="item in navItems"
        :key="item.key"
        :to="{ name: item.key }"
        class="mb-1 flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors"
        :class="
          activeKey() === item.key
            ? 'bg-cyan-100 text-cyan-900 dark:bg-cyan-900/30 dark:text-cyan-200'
            : 'text-neutral-700 hover:bg-neutral-100 dark:text-neutral-200 dark:hover:bg-neutral-800'
        "
        @click="emit('navigate')"
      >
        <component :is="item.icon" :size="18" />
        <span class="flex min-w-0 flex-1 items-center justify-between gap-2">
          <span class="truncate">{{ item.label }}</span>
          <n-badge
            v-if="item.key === 'containers'"
            :value="runningCount"
            type="success"
            :show-zero="false"
          />
        </span>
      </router-link>
    </nav>
    <div
      class="px-4 pb-3 pt-10 text-xs text-neutral-500 dark:border-neutral-700 dark:text-neutral-400"
    >
      <p v-if="backendVersion">VPSiner v{{ backendVersion }}</p>
      <p v-if="retentionWeeks !== null">Data Retention: {{ retentionWeeks }} weeks</p>
      <a
        :href="docsUrl"
        target="_blank"
        rel="noopener noreferrer"
        class="block text-cyan-700 hover:underline dark:text-cyan-400 mt-4"
        >Documentation</a
      >
    </div>
  </div>
</template>
