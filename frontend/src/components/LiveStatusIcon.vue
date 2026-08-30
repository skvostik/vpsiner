<script setup lang="ts">
import { computed } from 'vue'
import { Circle, CircleStop } from '@lucide/vue'
import { NTooltip } from 'naive-ui'

const props = withDefaults(
  defineProps<{
    live?: boolean
    status?: 'live' | 'history' | 'stopped'
    size?: number
    pulse?: boolean
  }>(),
  { size: 16, live: true, pulse: false }
)

const resolvedStatus = computed(() => props.status ?? (props.live ? 'live' : 'stopped'))
const tooltipText = computed(() => {
  switch (resolvedStatus.value) {
    case 'live':
      return props.pulse ? 'Live: updating in real time' : 'Live: at least one container is running'
    case 'history':
      return 'Connected: browsing historical data'
    case 'stopped':
      return 'Disconnected or not live'
  }
})
const ariaLabel = computed(() => {
  switch (resolvedStatus.value) {
    case 'live':
      return props.pulse ? 'Live data updates' : 'Log group is live'
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
          :class="resolvedStatus === 'live' && pulse ? 'status-live' : 'status-static'"
        />
        <CircleStop v-else :size="size" class="text-red-500" />
      </span>
    </template>
    {{ tooltipText }}
  </n-tooltip>
</template>

<style scoped>
.status-live {
  animation: page-status-pulse 1.2s ease-in-out infinite;
  filter: drop-shadow(0 0 3px rgba(16, 185, 129, 0.7)) drop-shadow(0 0 12px rgba(16, 185, 129, 0.7));
}

.status-static {
  opacity: 0.9;
}

@keyframes page-status-pulse {
  0% {
    transform: scale(1);
    opacity: 1;
  }

  50% {
    transform: scale(1.08);
    opacity: 0.92;
  }

  100% {
    transform: scale(1);
    opacity: 1;
  }
}
</style>
