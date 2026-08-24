<script setup lang="ts">
import { NDatePicker, NSelect } from 'naive-ui'

import type { MetricsWindow } from '../types'

defineProps<{
  window: MetricsWindow
  customFrom?: number
  customTo?: number
  resolutionLabel?: string
}>()

defineEmits<{
  'update:window': [value: MetricsWindow]
  'update:custom-from': [value: number | null]
  'update:custom-to': [value: number | null]
}>()

const windowOptions = [
  { label: 'Last 10 minutes', value: '10m' },
  { label: 'Last 30 minutes', value: '30m' },
  { label: 'Last hour', value: '1h' },
  { label: 'Last 6 hours', value: '6h' },
  { label: 'Last 24 hours', value: '24h' },
  { label: 'Last 7 days', value: '7d' },
  { label: 'Custom range', value: 'custom' },
]
</script>

<template>
  <div class="flex flex-wrap items-center gap-3">
    <n-select
      class="w-44"
      :value="window"
      :options="windowOptions"
      @update:value="$emit('update:window', $event)"
    />
    <div v-if="window === 'custom'" class="flex flex-wrap gap-3">
      <n-date-picker
        :value="customFrom ?? null"
        type="datetime"
        clearable
        placeholder="From"
        @update:value="$emit('update:custom-from', $event)"
      />
      <n-date-picker
        :value="customTo ?? null"
        type="datetime"
        clearable
        placeholder="To"
        @update:value="$emit('update:custom-to', $event)"
      />
    </div>
    <span v-if="resolutionLabel" class="text-xs text-neutral-500 dark:text-neutral-400">
      Resolution: {{ resolutionLabel }}
    </span>
  </div>
</template>
