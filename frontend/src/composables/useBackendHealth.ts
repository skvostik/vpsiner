import { onBeforeUnmount, onMounted, ref } from 'vue'

import {
  healthCheckTimeoutMs,
  healthOfflineIntervalMs,
  healthOnlineIntervalMs,
} from './streamConfig'

export const backendOnline = ref(true)
/** Whether container start/stop/restart endpoints are usable against the current Docker socket/proxy. */
export const dockerControlsAvailable = ref(false)
/** Whether the backend can currently reach the Docker socket/proxy. */
export const dockerConnected = ref(true)
/** Backend app version, reported via /api/health. */
export const backendVersion = ref('')
/** Data retention window in weeks, reported via /api/health. */
export const retentionWeeks = ref<number | null>(null)

let timer: number | undefined

async function check() {
  try {
    // A hung request (backend accepted the connection but never replies) must not stall polling forever.
    const response = await fetch('/api/health', {
      cache: 'no-store',
      signal: AbortSignal.timeout(healthCheckTimeoutMs),
    })
    backendOnline.value = response.ok
    if (response.ok) {
      const body = (await response.json()) as {
        docker_controls_available?: boolean
        docker_connected?: boolean
        version?: string
        retention_weeks?: number
      }
      dockerControlsAvailable.value = body.docker_controls_available ?? false
      dockerConnected.value = body.docker_connected ?? true
      if (body.version) backendVersion.value = body.version
      if (body.retention_weeks !== undefined) retentionWeeks.value = body.retention_weeks
    }
  } catch {
    backendOnline.value = false
  }
  schedule()
}

function schedule() {
  if (timer) window.clearTimeout(timer)
  timer = window.setTimeout(
    check,
    backendOnline.value ? healthOnlineIntervalMs : healthOfflineIntervalMs
  )
}

/** Lets API calls flag an outage immediately instead of waiting for the next poll. */
export function reportBackendUnreachable() {
  backendOnline.value = false
  check()
}

/** Only mark the backend as unreachable after an SSE connection is definitively closed. */
export function reportSseIssue(source?: EventSource) {
  if (source?.readyState === EventSource.CLOSED) {
    reportBackendUnreachable()
  }
}

export function useBackendHealth() {
  onMounted(check)
  onBeforeUnmount(() => {
    if (timer) window.clearTimeout(timer)
    timer = undefined
  })
  return { backendOnline }
}
