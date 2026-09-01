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

// Mirrors the backend search: `sanitize_fts_query` splits the query on whitespace (double quotes
// group a phrase) and ORs the terms, and SQLite's `trigram` FTS5 tokenizer matches any
// case-insensitive substring, regardless of token boundaries.
// The backend's trigram FTS index cannot match substrings shorter than this.
const minQueryLength = 3

function foldCase(value: string) {
  return value.toLocaleLowerCase()
}

/** Splits the raw query into phrases the same way `sanitize_fts_query` does. */
function queryPhrases(raw: string) {
  const terms: string[] = []
  let current = ''
  let inQuotes = false

  for (const ch of raw) {
    if (ch === '"') {
      if (current) terms.push(current)
      current = ''
      inQuotes = !inQuotes
    } else if (/\s/u.test(ch) && !inQuotes) {
      if (current) terms.push(current)
      current = ''
    } else {
      current += ch
    }
  }
  if (current) terms.push(current)

  return terms.filter((term) => term.length >= minQueryLength).map(foldCase)
}

const highlightedParts = computed(() => {
  const source = props.line.line
  const phrases = queryPhrases(props.query)
  if (!phrases.length) return [{ text: source, match: false }]

  const folded = foldCase(source)
  const matches: Array<{ start: number; end: number }> = []
  for (const phrase of phrases) {
    let fromIndex = 0
    let index = folded.indexOf(phrase, fromIndex)
    while (index !== -1) {
      matches.push({ start: index, end: index + phrase.length })
      fromIndex = index + 1
      index = folded.indexOf(phrase, fromIndex)
    }
  }
  matches.sort((left, right) => left.start - right.start)

  const ranges: Array<{ start: number; end: number }> = []
  for (const match of matches) {
    const previous = ranges[ranges.length - 1]
    // Terms can overlap, so keep a single merged span per region.
    if (previous && match.start <= previous.end) previous.end = Math.max(previous.end, match.end)
    else ranges.push({ ...match })
  }

  if (!ranges.length) return [{ text: source, match: false }]

  const parts: Array<{ text: string; match: boolean }> = []
  let cursor = 0
  for (const range of ranges) {
    if (range.start > cursor) parts.push({ text: source.slice(cursor, range.start), match: false })
    parts.push({ text: source.slice(range.start, range.end), match: true })
    cursor = range.end
  }
  if (cursor < source.length) parts.push({ text: source.slice(cursor), match: false })
  return parts
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
