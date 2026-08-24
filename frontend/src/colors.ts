const palette = ['#0891b2', '#f59e0b', '#10b981', '#ef4444', '#8b5cf6', '#ec4899', '#64748b']

/** Deterministic color per key (log_group/container id) so it doesn't shift between polls. */
export function colorForKey(key: string) {
  let hash = 0
  for (let i = 0; i < key.length; i += 1) {
    hash = (hash * 31 + key.charCodeAt(i)) | 0
  }
  return palette[Math.abs(hash) % palette.length]
}
