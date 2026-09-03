<script setup lang="ts">
import { NSpin } from 'naive-ui'
</script>

<template>
  <div
    class="fixed inset-0 z-50 flex flex-col items-center justify-center bg-white px-6 text-center dark:bg-neutral-950"
  >
    <p class="text-xs font-semibold uppercase tracking-[0.2em] text-cyan-700 dark:text-cyan-400">
      Simply Observed
    </p>
    <h1 class="text-4xl font-extrabold tracking-tight text-neutral-900 dark:text-neutral-50">
      VPSiner
    </h1>
    <p class="text-sm text-neutral-600 dark:text-neutral-400 my-6">
      Backend is unavailable. This page reconnects automatically.
    </p>
    <n-spin size="small" />
  </div>
</template>
