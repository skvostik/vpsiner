<script setup lang="ts">
import { computed } from 'vue'
import { Bug, Circle, CircleX, Info, TriangleAlert } from '@lucide/vue'

import type { LogLine } from '../../types'

const props = defineProps<{ line: LogLine; query: string; fresh?: boolean }>()

const levelIcon = computed(() => {
  switch (props.line.level) {
    case 'error':
      return CircleX
    case 'warn':
      return TriangleAlert
    case 'info':
      return Info
    case 'debug':
      return Bug
    default:
      return Circle
  }
})

const levelIconClass = computed(() => {
  switch (props.line.level) {
    case 'error':
      return 'text-red-500'
    case 'warn':
      return 'text-amber-500'
    case 'info':
      return 'text-blue-500'
    case 'debug':
      return 'text-neutral-400'
    default:
      return 'text-neutral-400'
  }
})

const highlightedParts = computed(() => {
  const query = props.query
  if (!query.trim()) return [{ text: props.line.line, match: false }]

  const parts: Array<{ text: string; match: boolean }> = []
  const source = props.line.line
  const sourceLower = source.toLocaleLowerCase()
  const queryLower = query.toLocaleLowerCase()
  let cursor = 0
  let matchStart = sourceLower.indexOf(queryLower, cursor)

  while (matchStart !== -1) {
    if (matchStart > cursor) parts.push({ text: source.slice(cursor, matchStart), match: false })
    parts.push({ text: source.slice(matchStart, matchStart + query.length), match: true })
    cursor = matchStart + query.length
    matchStart = sourceLower.indexOf(queryLower, cursor)
  }

  if (cursor < source.length) parts.push({ text: source.slice(cursor), match: false })
  return parts.length ? parts : [{ text: source, match: false }]
})
</script>

<template>
  <article :class="[fresh && 'log-entry--fresh']">
    <div class="flex items-start gap-3">
      <div class="min-w-0 flex-1">
        <div
          class="flex flex-col gap-1 text-xs text-neutral-500 sm:flex-row sm:items-baseline sm:gap-3 dark:text-neutral-400"
        >
          <div class="flex items-baseline gap-2 sm:shrink-0">
            <span class="flex w-4 shrink-0 items-center self-center" aria-hidden="true">
              <component :is="levelIcon" v-if="levelIcon" :size="14" :class="levelIconClass" />
            </span>
            <time
              class="whitespace-nowrap tabular-nums"
              :datetime="new Date(line.ts).toISOString()"
              >{{ new Date(line.ts).toLocaleString() }}</time
            >
          </div>
          <pre
            class="m-0 min-w-0 flex-1 whitespace-pre-wrap wrap-break-word font-mono text-sm leading-5 text-neutral-800 dark:text-neutral-200"
          ><span v-for="(part, index) in highlightedParts" :key="index" :class="part.match ? 'bg-amber-200/80 text-neutral-950 dark:bg-amber-500/40 dark:text-amber-50' : undefined">{{ part.text }}</span></pre>
        </div>
      </div>
    </div>
  </article>
</template>

<style scoped>
.log-entry--fresh {
  animation: log-entry-fresh 1000ms ease-out;
}

@keyframes log-entry-fresh {
  from {
    background-color: rgba(6, 181, 212, 0.1);
  }
  to {
    background-color: transparent;
  }
}

@media (prefers-reduced-motion: reduce) {
  .log-entry--fresh {
    animation: none;
  }
}
</style>
