<script setup lang="ts">
import { NLayoutHeader } from 'naive-ui'
import { pageTitle } from '../composables/usePageTitle'
</script>

<template>
  <n-layout-header
    bordered
    class="sticky top-0 z-10 bg-white/90 backdrop-blur dark:bg-neutral-950/90"
  >
    <div class="mx-auto min-w-0 max-w-7xl px-5 py-4 sm:px-8">
      <div class="flex items-center justify-between gap-4">
        <div class="flex min-w-0 items-center gap-3">
          <slot name="leading" />
          <div id="app-header-title-leading" class="flex shrink-0 items-center empty:hidden"></div>
          <div class="min-w-0">
            <h1
              class="truncate text-lg font-semibold tracking-tight text-neutral-900 dark:text-neutral-50"
            >
              {{ pageTitle }}
            </h1>
            <div
              id="app-header-title-subtext"
              class="truncate text-xs text-neutral-500 empty:hidden dark:text-neutral-400"
            ></div>
          </div>
        </div>
        <div id="app-header-actions" class="flex shrink-0 items-center gap-2"></div>
      </div>
      <div id="app-header-controls" class="mt-4 min-w-0 empty:mt-0"></div>
    </div>
  </n-layout-header>
</template>
