<script setup lang="ts">
import { NDatePicker, NInput, NSelect } from 'naive-ui'
import { Search } from '@lucide/vue'

import type { LogLevel, LogStream, LogWindow } from '../../types'

defineProps<{
  query: string
  level: LogLevel[]
  stream: LogStream[]
  window: LogWindow
  expanded: boolean
  customFrom?: number
  customTo?: number
}>()

defineEmits<{
  'update:query': [value: string]
  'update:level': [value: LogLevel[]]
  'update:stream': [value: LogStream[]]
  'update:window': [value: LogWindow]
  'update:custom-from': [value: number | null]
  'update:custom-to': [value: number | null]
}>()

const levelOptions = [
  { label: 'Debug', value: 'debug' },
  { label: 'Info', value: 'info' },
  { label: 'Warn', value: 'warn' },
  { label: 'Error', value: 'error' },
]
const streamOptions = [
  { label: 'stdout', value: 'stdout' },
  { label: 'stderr', value: 'stderr' },
]
const windowOptions = [
  { label: 'Last hour', value: '1h' },
  { label: 'Last 6 hours', value: '6h' },
  { label: 'Last 24 hours', value: '24h' },
  { label: 'Last 7 days', value: '7d' },
  { label: 'Last 30 days', value: '30d' },
  { label: 'Custom range', value: 'custom' },
]

function toggleValue<T extends string>(values: T[], value: T) {
  return values.includes(value) ? values.filter((item) => item !== value) : [...values, value]
}
</script>

<template>
  <div class="grid min-w-0 gap-3 sm:grid-cols-[minmax(0,1fr)_10rem] sm:items-center">
    <n-input
      :class="expanded ? 'min-w-0 w-full' : 'min-w-0 w-full sm:col-span-2'"
      :value="query"
      placeholder="Search log text"
      clearable
      @update:value="$emit('update:query', $event)"
    >
      <template #prefix><Search :size="16" /></template>
    </n-input>
    <n-select
      v-if="expanded"
      class="min-w-0 w-full"
      :value="window"
      :options="windowOptions"
      placeholder="Time window"
      @update:value="$emit('update:window', $event)"
    />
    <div
      v-if="expanded && window === 'custom'"
      class="grid min-w-0 gap-3 sm:col-span-2 sm:grid-cols-2"
    >
      <n-date-picker
        class="min-w-0 w-full"
        :value="customFrom ?? null"
        type="datetime"
        clearable
        placeholder="From"
        @update:value="$emit('update:custom-from', $event)"
      />
      <n-date-picker
        class="min-w-0 w-full"
        :value="customTo ?? null"
        type="datetime"
        clearable
        placeholder="To"
        @update:value="$emit('update:custom-to', $event)"
      />
    </div>
    <div v-if="expanded" class="flex flex-wrap items-center gap-2 sm:col-span-2">
      <span class="w-full text-xs font-medium text-neutral-500 dark:text-neutral-400 sm:w-auto"
        >Level</span
      >
      <button
        v-for="option in levelOptions"
        :key="option.value"
        type="button"
        :aria-pressed="level.includes(option.value as LogLevel)"
        class="rounded-full border px-3 py-1 text-xs font-medium transition-colors"
        :class="
          level.includes(option.value as LogLevel)
            ? 'border-cyan-600 bg-cyan-50 text-cyan-800 dark:border-cyan-400 dark:bg-cyan-950/50 dark:text-cyan-200'
            : 'border-neutral-200 text-neutral-500 hover:border-neutral-400 dark:border-neutral-700 dark:text-neutral-400 dark:hover:border-neutral-500'
        "
        @click="$emit('update:level', toggleValue(level, option.value as LogLevel))"
      >
        {{ option.label }}
      </button>
      <span
        class="ml-0 w-full text-xs font-medium text-neutral-500 dark:text-neutral-400 sm:ml-4 sm:w-auto"
        >Stream</span
      >
      <button
        v-for="option in streamOptions"
        :key="option.value"
        type="button"
        :aria-pressed="stream.includes(option.value as LogStream)"
        class="rounded-full border px-3 py-1 text-xs font-medium transition-colors"
        :class="
          stream.includes(option.value as LogStream)
            ? 'border-cyan-600 bg-cyan-50 text-cyan-800 dark:border-cyan-400 dark:bg-cyan-950/50 dark:text-cyan-200'
            : 'border-neutral-200 text-neutral-500 hover:border-neutral-400 dark:border-neutral-700 dark:text-neutral-400 dark:hover:border-neutral-500'
        "
        @click="$emit('update:stream', toggleValue(stream, option.value as LogStream))"
      >
        {{ option.label }}
      </button>
    </div>
  </div>
</template>
