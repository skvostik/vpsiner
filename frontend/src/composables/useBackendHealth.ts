import { onBeforeUnmount, onMounted, ref } from 'vue'

const onlineIntervalMs = 15_000
const offlineIntervalMs = 3_000

export const backendOnline = ref(true)
/** Whether container start/stop/restart endpoints are usable against the current Docker socket/proxy. */
export const dockerControlsAvailable = ref(false)
/** Backend app version, reported via /api/health. */
export const backendVersion = ref('')
/** Data retention window in weeks, reported via /api/health. */
export const retentionWeeks = ref<number | null>(null)

let timer: number | undefined

async function check() {
  try {
    const response = await fetch('/api/health', { cache: 'no-store' })
    backendOnline.value = response.ok
    if (response.ok) {
      const body = (await response.json()) as {
        docker_controls_available?: boolean
        version?: string
        retention_weeks?: number
      }
      dockerControlsAvailable.value = body.docker_controls_available ?? false
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
  timer = window.setTimeout(check, backendOnline.value ? onlineIntervalMs : offlineIntervalMs)
}

/** Lets API calls flag an outage immediately instead of waiting for the next poll. */
export function reportBackendUnreachable() {
  backendOnline.value = false
  check()
}

export function useBackendHealth() {
  onMounted(check)
  onBeforeUnmount(() => {
    if (timer) window.clearTimeout(timer)
    timer = undefined
  })
  return { backendOnline }
}
