import { computed, onBeforeUnmount, onMounted, ref, watch, type ComputedRef } from 'vue'
import { useMessage } from 'naive-ui'

import { api } from '../api'
import { reportBackendUnreachable } from './useBackendHealth'
import { useLogGroupsStream } from './useLogGroupsStream'
import type {
  LogLevel,
  LogGroups,
  LogLine,
  LogQueryParams,
  LogStream,
  LogTailAppend,
} from '../types'

const storageKey = 'vpsiner.logs.preferences.v1'
const pageSize = 20
// SSE batches are variable-sized, so the retention cap must count lines rather than pages.
const maxLoadedLines = 1000
const freshDurationMs = 2_000

export function logLineKey(line: LogLine) {
  return `${line.ts}:${line.cid}:${line.stream}:${line.line}`
}

// A contiguous fetched chunk; keeps its own cursors so an edge page can resume pagination after eviction.
type LogPageEntry = {
  items: LogLine[]
  olderCursor?: string
  newerCursor?: string
}

type UseLogsState = {
  groups: ReturnType<typeof ref<LogGroups>>
  selectedGroup: ReturnType<typeof ref<string>>
  logs: ComputedRef<LogLine[]>
  query: ReturnType<typeof ref<string>>
  level: ReturnType<typeof ref<LogLevel[]>>
  stream: ReturnType<typeof ref<LogStream[]>>
  customFrom: ReturnType<typeof ref<number | undefined>>
  customTo: ReturnType<typeof ref<number | undefined>>
  loadingGroups: ReturnType<typeof ref<boolean>>
  loadingLogs: ReturnType<typeof ref<boolean>>
  hasMore: ReturnType<typeof ref<boolean>>
  hasNewer: ReturnType<typeof ref<boolean>>
  tailing: ComputedRef<boolean>
  freshKeys: ReturnType<typeof ref<Set<string>>>
  error: ReturnType<typeof ref<string>>
  loadLogs: () => Promise<void>
  loadMore: () => Promise<void>
  loadNewer: () => Promise<void>
  reportAtBottom: (value: boolean) => void
  updateQuery: (value: string) => void
  updateLevel: (value: LogLevel[]) => void
  updateStream: (value: LogStream[]) => void
  updateCustomFrom: (value: number | null) => void
  updateCustomTo: (value: number | null) => void
}

export function useLogs(initialGroup?: string): UseLogsState {
  const message = useMessage()
  const { groups, loading: loadingGroups } = useLogGroupsStream()
  const selectedGroup = ref(initialGroup ?? '')
  const pages = ref<LogPageEntry[]>([])
  const logs = computed(() => pages.value.flatMap((page) => page.items))
  const query = ref('')
  const level = ref<LogLevel[]>([])
  const stream = ref<LogStream[]>([])
  const customFrom = ref<number>()
  const customTo = ref<number>()
  const loadingLogs = ref(false)
  const hasMore = ref(false)
  const hasNewer = ref(false)
  // Tailing only makes sense with no upper bound; it never depends on "from".
  const liveWindow = computed(() => customTo.value === undefined)
  const atBottom = ref(false)
  // Whether being at the bottom of a live window would currently count as tailing.
  const couldTail = computed(() => liveWindow.value && atBottom.value)
  // False the instant tailing stops; only re-armed once we prove we've caught back up (a forward
  // fetch reports no more newer data while the user is at the bottom).
  const isTailingAvailable = ref(true)
  const selectedGroupIsLive = computed(() => groups.value[selectedGroup.value]?.live ?? false)
  const tailing = computed(
    () => couldTail.value && isTailingAvailable.value && selectedGroupIsLive.value
  )
  const freshKeys = ref(new Set<string>())
  const error = ref('')
  let searchTimer: number | undefined
  let requestVersion = 0
  let freshTimer: number | undefined
  let tailSource: EventSource | undefined
  // Serializes pagination fetches so they never mutate pages concurrently.
  let fetching = false

  function totalLines() {
    return pages.value.reduce((sum, page) => sum + page.items.length, 0)
  }

  function fmtTs(ts: number | undefined) {
    return ts === undefined ? '?' : new Date(ts).toISOString().slice(11, 23)
  }

  /** Line count plus its first/last timestamps, for readable debug logging. */
  function pageSummary(items: LogLine[]) {
    const first = items[0]
    const last = items[items.length - 1]
    return first && last
      ? `${items.length} lines [${fmtTs(first.ts)} -> ${fmtTs(last.ts)}]`
      : `${items.length} lines`
  }

  function logEvent(event: string, details: Record<string, unknown> = {}) {
    console.debug(`[logs:${selectedGroup.value || '-'}] ${event}`, details)
  }

  /** Drops pages from the edge opposite the edge that just grew. */
  function evict(edge: 'head' | 'tail') {
    while (totalLines() > maxLoadedLines && pages.value.length > 1) {
      if (edge === 'head') {
        const [dropped, next] = pages.value
        // The dropped page's cursor is the only place "resume backward from here" was recorded.
        pages.value = [
          next.olderCursor === undefined ? { ...next, olderCursor: dropped.olderCursor } : next,
          ...pages.value.slice(2),
        ]
        hasMore.value = true
        logEvent('evict-head', {
          dropped: pageSummary(dropped.items),
          remainingLines: totalLines(),
        })
      } else {
        const dropped = pages.value[pages.value.length - 1]
        const next = pages.value[pages.value.length - 2]
        pages.value = [
          ...pages.value.slice(0, -2),
          next.newerCursor === undefined ? { ...next, newerCursor: dropped.newerCursor } : next,
        ]
        hasNewer.value = true
        logEvent('evict-tail', {
          dropped: pageSummary(dropped.items),
          remainingLines: totalLines(),
        })
      }
    }
  }

  function reportAtBottom(value: boolean) {
    atBottom.value = value
  }

  function markFresh(lines: LogLine[]) {
    const next = new Set(freshKeys.value)
    lines.forEach((line) => next.add(logLineKey(line)))
    freshKeys.value = next
    if (freshTimer) window.clearTimeout(freshTimer)
    freshTimer = window.setTimeout(() => {
      freshKeys.value = new Set()
    }, freshDurationMs * 2)
  }

  function appendLines(lines: LogLine[], fresh: boolean, newerCursor?: string) {
    const sorted = lines.slice().sort((left, right) => left.ts - right.ts)
    pages.value = [...pages.value, { items: sorted, newerCursor }]
    if (fresh) markFresh(lines)
    evict('head')
  }

  function reportError(value: unknown, fallback: string) {
    const text = value instanceof Error ? value.message : fallback
    error.value = text
    message.error(text)
  }

  function currentParams(mode?: 'older' | 'newer'): LogQueryParams {
    return {
      from: customFrom.value,
      // Tailing's catch-up fetch always extends to whatever exists now, ignoring any upper bound.
      to: mode === 'newer' ? undefined : customTo.value,
      q: query.value || undefined,
      level: level.value,
      stream: stream.value,
      limit: pageSize,
      before: mode === 'older' ? pages.value[0]?.olderCursor : undefined,
      after: mode === 'newer' ? pages.value[pages.value.length - 1]?.newerCursor : undefined,
    }
  }

  async function loadLogs() {
    const version = ++requestVersion
    pages.value = []
    hasMore.value = false
    hasNewer.value = false
    stopTailStream()
    freshKeys.value = new Set()
    if (!selectedGroup.value) {
      loadingLogs.value = false
      return
    }
    loadingLogs.value = true
    fetching = true
    try {
      const page = await api.logs.query(selectedGroup.value, currentParams())
      if (version !== requestVersion) return
      pages.value = [
        {
          items: page.items,
          olderCursor: page.older_cursor ?? undefined,
          newerCursor: page.newer_cursor ?? undefined,
        },
      ]
      hasMore.value = page.has_older
      hasNewer.value = page.has_newer
      // A fresh load fetches the tail of the window, so it's the ground truth for "caught up".
      isTailingAvailable.value = !page.has_newer
      error.value = ''
      if (tailing.value) startTailStream()
      logEvent('load', {
        page: pageSummary(page.items),
        hasOlder: hasMore.value,
        hasNewer: hasNewer.value,
      })
    } catch (loadError) {
      if (version === requestVersion) reportError(loadError, 'Unable to load logs')
    } finally {
      fetching = false
      if (version === requestVersion) loadingLogs.value = false
    }
  }

  async function loadMore() {
    const cursor = pages.value[0]?.olderCursor
    if (!selectedGroup.value || !hasMore.value || loadingLogs.value || fetching || !cursor) return
    const version = requestVersion
    fetching = true
    loadingLogs.value = true
    try {
      const page = await api.logs.query(selectedGroup.value, {
        ...currentParams('older'),
        before: cursor,
      })
      if (version !== requestVersion) return
      const entry: LogPageEntry = {
        items: page.items,
        olderCursor: page.older_cursor ?? undefined,
        newerCursor: page.newer_cursor ?? undefined,
      }
      pages.value = [entry, ...pages.value]
      hasMore.value = page.has_older
      evict('tail')
      logEvent('load-older', { page: pageSummary(page.items), hasOlder: hasMore.value })
    } catch (loadError) {
      if (version === requestVersion) reportError(loadError, 'Unable to load more logs')
    } finally {
      fetching = false
      if (version === requestVersion) loadingLogs.value = false
    }
  }

  async function loadNewer() {
    const cursor = pages.value[pages.value.length - 1]?.newerCursor
    if (
      !selectedGroup.value ||
      // Even with no known gap, allow one check when arriving back at the bottom unverified.
      (!hasNewer.value && isTailingAvailable.value) ||
      loadingLogs.value ||
      fetching ||
      !cursor
    )
      return
    const version = requestVersion
    fetching = true
    loadingLogs.value = true
    try {
      const page = await api.logs.query(selectedGroup.value, {
        ...currentParams('newer'),
        after: cursor,
      })
      if (version !== requestVersion) return
      const entry: LogPageEntry = {
        items: page.items,
        olderCursor: page.older_cursor ?? undefined,
        newerCursor: page.newer_cursor ?? undefined,
      }
      pages.value = [...pages.value, ...(page.items.length ? [entry] : [])]
      hasNewer.value = page.has_newer
      // Reaching the real tail while the user is actually at the bottom re-arms live tailing.
      if (atBottom.value && !page.has_newer) {
        isTailingAvailable.value = true
        logEvent('tailing-available', { value: true, reason: 'caught up' })
      }
      evict('head')
      logEvent('load-newer', { page: pageSummary(page.items), hasNewer: hasNewer.value })
    } catch (loadError) {
      if (version === requestVersion) reportError(loadError, 'Unable to load newer logs')
    } finally {
      fetching = false
      if (version === requestVersion) loadingLogs.value = false
    }
  }

  function stopTailStream() {
    tailSource?.close()
    tailSource = undefined
  }

  /** Opens (or reopens) the live tail for the current group/filters, resuming from the last cursor. */
  function startTailStream() {
    stopTailStream()
    if (!selectedGroup.value) return
    const cursor = pages.value[pages.value.length - 1]?.newerCursor
    const params = new URLSearchParams({ after: cursor ?? '' })
    if (query.value) params.set('q', query.value)
    if (level.value.length) params.set('level', level.value.join(','))
    if (stream.value.length) params.set('stream', stream.value.join(','))
    tailSource = new EventSource(
      `/api/stream/logs/${encodeURIComponent(selectedGroup.value)}?${params}`
    )
    tailSource.addEventListener('append', (event) => {
      const append = JSON.parse((event as MessageEvent).data) as LogTailAppend
      const existing = new Set(logs.value.map((line) => logLineKey(line)))
      const newLines = append.items.filter((line) => !existing.has(logLineKey(line)))
      if (newLines.length) {
        appendLines(newLines, true, append.newer_cursor ?? undefined)
        logEvent('tail-append', { page: pageSummary(newLines) })
      }
      hasNewer.value = false
    })
    // The browser retries automatically; just surface the outage to the rest of the UI.
    tailSource.onerror = () => reportBackendUnreachable()
  }

  function persist() {
    localStorage.setItem(
      storageKey,
      JSON.stringify({
        group: selectedGroup.value,
        query: query.value,
        level: level.value,
        stream: stream.value,
        customFrom: customFrom.value,
        customTo: customTo.value,
      })
    )
  }

  function updateQuery(value: string) {
    query.value = value
    persist()
    if (searchTimer) window.clearTimeout(searchTimer)
    searchTimer = window.setTimeout(loadLogs, 350)
  }

  function updateLevel(value: LogLevel[]) {
    level.value = value
    persist()
    loadLogs()
  }

  function updateStream(value: LogStream[]) {
    stream.value = value
    persist()
    loadLogs()
  }

  function updateCustomFrom(value: number | null) {
    customFrom.value = value ?? undefined
    persist()
    loadLogs()
  }

  function updateCustomTo(value: number | null) {
    customTo.value = value ?? undefined
    persist()
    loadLogs()
  }

  watch(selectedGroup, () => {
    persist()
    loadLogs()
  })

  onMounted(() => {
    try {
      const saved = JSON.parse(localStorage.getItem(storageKey) ?? '{}') as Partial<{
        group: string
        query: string
        level: LogLevel[]
        stream: LogStream[]
        customFrom: number
        customTo: number
      }>
      if (!initialGroup && saved.group) selectedGroup.value = saved.group
      query.value = saved.query ?? ''
      level.value = saved.level ?? []
      stream.value = saved.stream ?? []
      customFrom.value = saved.customFrom
      customTo.value = saved.customTo
    } catch {
      // Ignore invalid local preferences.
    }
    loadLogs()
  })

  watch(
    tailing,
    (value) => {
      if (value) startTailStream()
      else stopTailStream()
      logEvent('tailing', { active: value })
    },
    { immediate: true }
  )
  // Stopping tailing always invalidates it; landing back at the bottom re-verifies silently
  // rather than assuming we're still caught up (a gap may have opened up while away).
  watch(couldTail, (value) => {
    if (!value) {
      isTailingAvailable.value = false
      logEvent('tailing-available', { value: false, reason: 'stopped tailing' })
    } else if (!isTailingAvailable.value) {
      logEvent('tailing-available', { value: false, reason: 'verifying catch-up' })
      loadNewer()
    }
  })

  onBeforeUnmount(() => {
    if (searchTimer) window.clearTimeout(searchTimer)
    if (freshTimer) window.clearTimeout(freshTimer)
    stopTailStream()
  })

  return {
    groups,
    selectedGroup,
    logs,
    query,
    level,
    stream,
    loadingGroups,
    loadingLogs,
    hasMore,
    hasNewer,
    tailing,
    freshKeys,
    error,
    loadLogs,
    loadMore,
    loadNewer,
    reportAtBottom,
    updateQuery,
    updateLevel,
    updateStream,
    customFrom,
    customTo,
    updateCustomFrom,
    updateCustomTo,
  }
}
