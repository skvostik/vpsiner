import { reactive, watch } from 'vue'

import { api } from '../api'
import type { ContainerState, ContainerSummary } from '../types'
import { containersById } from './useContainersStream'

export type ContainerActionKind = 'start' | 'stop' | 'restart'

// Shared across ContainerTable and ContainerDetailView so both agree on in-flight actions.
const pendingByContainer = reactive<Record<string, ContainerActionKind | undefined>>({})
const timeouts: Record<string, ReturnType<typeof setTimeout>> = {}
const stopWatchers: Record<string, () => void> = {}

// Safety net in case the streamed state never reflects the action (e.g. Docker error not surfaced).
const ACTION_TIMEOUT_MS = 30_000

function isStoppedState(containerState: ContainerState) {
  return !['running', 'restarting', 'paused'].includes(containerState)
}

function targetReached(
  action: ContainerActionKind,
  container: ContainerSummary | undefined,
  startedAtBefore: number | null
) {
  if (!container) return false
  if (action === 'start') return container.state === 'running'
  if (action === 'stop') return isStoppedState(container.state)
  // A restart is only "done" once the container is running again with a fresh start time.
  return container.state === 'running' && container.started_at !== startedAtBefore
}

function clearPending(id: string) {
  delete pendingByContainer[id]
  stopWatchers[id]?.()
  delete stopWatchers[id]
  if (timeouts[id]) {
    clearTimeout(timeouts[id])
    delete timeouts[id]
  }
}

export function pendingAction(id: string): ContainerActionKind | undefined {
  return pendingByContainer[id]
}

export async function runContainerAction(
  container: ContainerSummary,
  action: ContainerActionKind
): Promise<void> {
  if (pendingByContainer[container.id]) return

  const startedAtBefore = container.started_at
  pendingByContainer[container.id] = action

  stopWatchers[container.id] = watch(
    () => containersById.value[container.id],
    (current) => {
      if (targetReached(action, current, startedAtBefore)) clearPending(container.id)
    }
  )
  timeouts[container.id] = setTimeout(() => clearPending(container.id), ACTION_TIMEOUT_MS)

  try {
    await api.containers.action(container.id, action)
  } catch (error) {
    clearPending(container.id)
    throw error
  }
}
