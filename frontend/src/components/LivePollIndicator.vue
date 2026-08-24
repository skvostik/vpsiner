<script setup lang="ts">
withDefaults(
  defineProps<{
    active?: boolean
    label?: string
  }>(),
  { active: true, label: 'Live data updates' }
)
</script>

<template>
  <Teleport to="body">
    <div
      v-if="active"
      class="pointer-events-none fixed inset-x-0 bottom-0 z-30 h-1 overflow-hidden bg-neutral-200/80 dark:bg-neutral-800/80"
      role="status"
      :aria-label="label"
    >
      <div class="live-poll-indicator__bar h-full w-1/3 bg-cyan-500" />
    </div>
  </Teleport>
</template>

<style scoped>
.live-poll-indicator__bar {
  animation: live-poll-indicator 1.6s ease-in-out infinite;
}

@keyframes live-poll-indicator {
  0% {
    transform: translateX(-100%);
  }

  50% {
    transform: translateX(150%);
  }

  100% {
    transform: translateX(300%);
  }
}
</style>
