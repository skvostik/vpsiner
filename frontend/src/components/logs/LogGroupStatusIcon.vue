<script setup lang="ts">
import { Circle, CircleStop } from '@lucide/vue'
import { NTooltip } from 'naive-ui'

withDefaults(
  defineProps<{
    live: boolean
    size?: number
  }>(),
  { size: 16 }
)
</script>

<template>
  <n-tooltip>
    <template #trigger>
      <span
        class="inline-flex shrink-0 items-center justify-center"
        :aria-label="live ? 'Live log group' : 'No running containers in this log group'"
        role="img"
      >
        <Circle v-if="live" :size="size" class="fill-emerald-500 text-emerald-500" />
        <CircleStop v-else :size="size" class="text-red-500" />
      </span>
    </template>
    {{
      live ? 'Live: at least one container is running' : 'No running containers in this log group'
    }}
  </n-tooltip>
</template>
