<script setup lang="ts">
import { computed, h } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { NBadge, NMenu, type MenuOption } from 'naive-ui'
import { Boxes, Gauge, Logs } from '@lucide/vue'

import { useContainers } from '../composables/useContainers'
import { backendVersion, retentionWeeks } from '../composables/useBackendHealth'

const docsUrl = 'https://github.com/skvostik/vpsiner'

const emit = defineEmits<{ navigate: [] }>()
const route = useRoute()
const router = useRouter()
const { runningCount } = useContainers()

const menuOptions = computed<MenuOption[]>(() => [
  { key: 'host', label: 'Host Metrics', icon: () => h(Gauge, { size: 18 }) },
  {
    key: 'containers',
    label: () =>
      h('span', { class: 'flex w-full items-center justify-between gap-2' }, [
        'Containers',
        h(NBadge, { value: runningCount.value, type: 'success', showZero: false }),
      ]),
    icon: () => h(Boxes, { size: 18 }),
  },
  { key: 'logs', label: 'Explore Logs', icon: () => h(Logs, { size: 18 }) },
])

const activeKey = () => {
  if (route.name === 'containers' || route.name === 'container-detail') return 'containers'
  if (route.name === 'logs' || route.name === 'log-viewer') return 'logs'
  return 'host'
}

function handleSelect(key: string) {
  router.push({ name: key })
  emit('navigate')
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
    <n-menu
      class="flex-1"
      :value="activeKey()"
      :options="menuOptions"
      @update:value="handleSelect"
    />
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
