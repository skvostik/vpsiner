<script setup lang="ts">
import { NDatePicker, NInput } from 'naive-ui'
import { Search } from '@lucide/vue'

import type { LogLevel, LogStream } from '../../types'

defineProps<{
  query: string
  level: LogLevel[]
  stream: LogStream[]
  expanded: boolean
  customFrom?: number
  customTo?: number
}>()

defineEmits<{
  'update:query': [value: string]
  'update:level': [value: LogLevel[]]
  'update:stream': [value: LogStream[]]
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

function toggleValue<T extends string>(values: T[], value: T) {
  return values.includes(value) ? values.filter((item) => item !== value) : [...values, value]
}
</script>

<template>
  <div class="grid min-w-0 gap-3 sm:items-center">
    <n-input
      class="min-w-0 w-full"
      :value="query"
      placeholder="Search log text"
      clearable
      @update:value="$emit('update:query', $event)"
    >
      <template #prefix><Search :size="16" /></template>
    </n-input>
    <div v-if="expanded" class="grid min-w-0 gap-3 sm:grid-cols-2">
      <n-date-picker
        class="min-w-0 w-full"
        :value="customFrom ?? null"
        type="datetime"
        clearable
        placeholder="From (unbounded if empty)"
        @update:value="$emit('update:custom-from', $event)"
      />
      <n-date-picker
        class="min-w-0 w-full"
        :value="customTo ?? null"
        type="datetime"
        clearable
        placeholder="To (tailing if empty)"
        @update:value="$emit('update:custom-to', $event)"
      />
    </div>
    <div v-if="expanded" class="flex flex-wrap items-center gap-2">
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
