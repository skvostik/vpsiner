<script setup lang="ts">
import { computed } from 'vue'
import { Circle, CircleStop } from '@lucide/vue'
import { NTooltip } from 'naive-ui'

const props = withDefaults(
  defineProps<{
    live?: boolean
    status?: 'live' | 'history' | 'stopped'
    size?: number
  }>(),
  { size: 16, live: true }
)

const resolvedStatus = computed(() => props.status ?? (props.live ? 'live' : 'stopped'))
const tooltipText = computed(() => {
  switch (resolvedStatus.value) {
    case 'live':
      return 'Live: updating in real time'
    case 'history':
      return 'Connected: browsing historical data'
    case 'stopped':
      return 'Disconnected or not live'
  }
})
const ariaLabel = computed(() => {
  switch (resolvedStatus.value) {
    case 'live':
      return 'Live data updates'
    case 'history':
      return 'Browsing historical data'
    case 'stopped':
      return 'Disconnected or not live'
  }
})
</script>

<template>
  <n-tooltip>
    <template #trigger>
      <span
        class="inline-flex shrink-0 items-center justify-center"
        :aria-label="ariaLabel"
        role="img"
      >
        <Circle
          v-if="resolvedStatus !== 'stopped'"
          :size="size"
          class="fill-emerald-500 text-emerald-500"
          :class="resolvedStatus === 'live' ? 'status-live' : 'status-history'"
        />
        <CircleStop v-else :size="size" class="text-red-500" />
      </span>
    </template>
    {{ tooltipText }}
  </n-tooltip>
</template>

<style scoped>
.status-live {
  animation: page-status-pulse 1.6s ease-in-out infinite;
}

.status-history {
  opacity: 0.9;
}

@keyframes page-status-pulse {
  0% {
    transform: scale(1);
    opacity: 1;
  }

  50% {
    transform: scale(1.12);
    opacity: 0.72;
  }

  100% {
    transform: scale(1);
    opacity: 1;
  }
}
</style>
