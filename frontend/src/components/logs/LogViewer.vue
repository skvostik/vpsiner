<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from 'vue'
import { useInfiniteScroll, useScroll } from '@vueuse/core'
import { NButton, NEmpty, NSpin } from 'naive-ui'
import { ListFilter } from '@lucide/vue'

import LogEntry from './LogEntry.vue'
import { logLineKey } from '../../composables/useLogs'
import type { LogLine } from '../../types'

const emit = defineEmits<{
  atBottomChange: [value: boolean]
  editFilters: []
}>()
const props = defineProps<{
  lines: LogLine[]
  loading: boolean
  hasMore: boolean
  hasNewer: boolean
  query: string
  tailing: boolean
  hasStoredLogs: boolean
  freshKeys: Set<string>
  loadOlder: () => Promise<void>
  loadNewer: () => Promise<void>
}>()
const topSentinel = ref<HTMLElement>()
const scrollParent = ref<HTMLElement>()
const prefetchMargin = 600
// While tailing, the poll already fetches newer logs in the background; loading forward here
// would just race it, so this only ever fires to page through a fixed, non-live range.
const showLoadNewer = computed(() => props.hasNewer && !props.tailing)
let initialScrollDone = false
// Render identity must distinguish otherwise identical log records.
const itemEls = new Map<string, HTMLElement>()
const renderKeys = new WeakMap<LogLine, string>()
let nextRenderKey = 0
let anchorKey: string | undefined
let anchorOffset = 0

function renderKey(line: LogLine) {
  let key = renderKeys.get(line)
  if (!key) {
    key = `log-${nextRenderKey++}`
    renderKeys.set(line, key)
  }
  return key
}

const renderedRange = computed(() => ({
  count: props.lines.length,
  first: props.lines[0] ? renderKey(props.lines[0]) : undefined,
  last: props.lines.length ? renderKey(props.lines[props.lines.length - 1]) : undefined,
}))

/** The app's layout scrolls an internal container, not the window, so walk up to find it. */
function resolveScrollParent(element: HTMLElement): HTMLElement {
  let node: HTMLElement | null = element.parentElement
  while (node) {
    if (/(auto|scroll|overlay)/.test(window.getComputedStyle(node).overflowY)) return node
    node = node.parentElement
  }
  return (document.scrollingElement as HTMLElement) ?? document.documentElement
}

// Tight tolerance: this drives the "at bottom" signal used for tailing.
const { arrivedState } = useScroll(scrollParent, { offset: { top: 4, bottom: 4 } })

async function requestOlderLogs() {
  if (props.loading || !props.hasMore) return
  await props.loadOlder()
}

async function requestNewerLogs() {
  if (props.loading || !showLoadNewer.value) return
  await props.loadNewer()
}

useInfiniteScroll(scrollParent, requestOlderLogs, {
  direction: 'top',
  distance: prefetchMargin,
  canLoadMore: () => props.hasMore && !props.loading,
})
useInfiniteScroll(scrollParent, requestNewerLogs, {
  direction: 'bottom',
  distance: prefetchMargin,
  canLoadMore: () => showLoadNewer.value && !props.loading,
})

function scrollHeight() {
  return scrollParent.value?.scrollHeight ?? 0
}

function scrollToBottom() {
  scrollParent.value?.scrollTo({ top: scrollHeight(), behavior: 'instant' })
}

function setItemRef(key: string, el: unknown) {
  const element = (el as { $el?: HTMLElement })?.$el ?? (el as HTMLElement | null)
  if (element instanceof HTMLElement) itemEls.set(key, element)
  else itemEls.delete(key)
}

/** Remembers the topmost visible line so scroll position can be restored after the list mutates. */
function captureAnchor() {
  if (!scrollParent.value) return
  const parentTop = scrollParent.value.getBoundingClientRect().top
  for (const line of props.lines) {
    const key = renderKey(line)
    const el = itemEls.get(key)
    if (!el) continue
    const rect = el.getBoundingClientRect()
    if (rect.bottom >= parentTop) {
      anchorKey = key
      anchorOffset = rect.top - parentTop
      return
    }
  }
  anchorKey = undefined
}

/** Adjusts scrollTop so the anchored line stays at the same visual offset after a mutation. */
function restoreAnchor() {
  if (!anchorKey || !scrollParent.value) {
    anchorKey = undefined
    return
  }
  const el = itemEls.get(anchorKey)
  anchorKey = undefined
  if (!el) return
  const parentTop = scrollParent.value.getBoundingClientRect().top
  const rect = el.getBoundingClientRect()
  scrollParent.value.scrollTop += rect.top - parentTop - anchorOffset
}

watch(
  () => arrivedState.bottom,
  (value) => emit('atBottomChange', value)
)

watch(renderedRange, async (current, previous) => {
  const shouldFollowTail = props.tailing && current.last !== previous?.last
  // Runs before this component re-renders, so the DOM here still reflects the previous lines.
  captureAnchor()
  if (!initialScrollDone && current.count) {
    initialScrollDone = true
    await nextTick()
    scrollToBottom()
    return
  }
  await nextTick()
  if (shouldFollowTail) {
    scrollToBottom()
  } else {
    restoreAnchor()
  }
})

onMounted(async () => {
  await nextTick()
  if (!topSentinel.value) return
  scrollParent.value = resolveScrollParent(topSentinel.value)
})
</script>

<template>
  <div ref="topSentinel" class="flex min-h-12 items-center justify-center">
    <n-spin v-if="loading && hasMore" size="small" />
    <span
      v-else-if="!hasMore && lines.length"
      class="text-xs text-neutral-500 dark:text-neutral-400"
      >Beginning of logs</span
    >
  </div>
  <div v-if="lines.length" class="space-y-2">
    <LogEntry
      v-for="line in lines"
      :key="renderKey(line)"
      :ref="(el) => setItemRef(renderKey(line), el)"
      :line="line"
      :query="query"
      :fresh="freshKeys.has(logLineKey(line))"
    />
  </div>
  <n-empty
    v-else-if="!loading"
    :description="
      hasStoredLogs
        ? 'No logs match the current filters'
        : 'No logs have been received from this log group yet'
    "
  >
    <template v-if="hasStoredLogs" #extra>
      <n-button secondary size="small" @click="emit('editFilters')">
        <template #icon><ListFilter :size="16" /></template>
        Edit filters
      </n-button>
    </template>
  </n-empty>
  <div v-else class="flex justify-center py-12"><n-spin /></div>
  <div v-if="showLoadNewer && loading" class="flex min-h-12 items-center justify-center">
    <n-spin size="small" />
  </div>
</template>
