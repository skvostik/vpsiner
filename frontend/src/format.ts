export function formatBytes(value: number) {
  if (value < 1024) return `${value.toFixed(1)} B`
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KB`
  if (value < 1024 ** 3) return `${(value / 1024 ** 2).toFixed(1)} MB`
  if (value < 1024 ** 4) return `${(value / 1024 ** 3).toFixed(1)} GB`
  return `${(value / 1024 ** 4).toFixed(1)} TB`
}

/** A rate is null until two consecutive counter readings exist, and after a counter reset. */
export function formatRate(value: number | null) {
  return value == null ? '—' : `${formatBytes(value)}/s`
}

export function formatUptime(startedAt: number, now = Date.now()) {
  const totalSeconds = Math.max(0, Math.floor((now - startedAt) / 1_000))
  const days = Math.floor(totalSeconds / 86_400)
  const hours = Math.floor((totalSeconds % 86_400) / 3_600)
  const minutes = Math.floor((totalSeconds % 3_600) / 60)
  const seconds = totalSeconds % 60

  if (days) return `${days}d ${hours}h`
  if (hours) return `${hours}h ${minutes}m`
  if (minutes) return `${minutes}m ${seconds}s`
  return `${seconds}s`
}
