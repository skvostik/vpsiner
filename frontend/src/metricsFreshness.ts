import type { MetricsResolution } from './types'

/** Bucket size in ms per resolution; the API guarantees buckets are epoch-aligned and never partial. */
export const RESOLUTION_BUCKET_MS: Record<MetricsResolution, number> = {
  '10s': 10_000,
  '1m': 60 * 1000,
  '5m': 5 * 60 * 1000,
  '1h': 60 * 60 * 1000,
}

/** How many missed/late polls to tolerate before treating the last known sample as stale. */
export const MARGIN_POLLS = 2

/** Never poll metrics faster than the backend actually samples them. */
export function computePollIntervalMs(sampleIntervalMs: number, floorMs = 2_000) {
  return Math.max(sampleIntervalMs, floorMs)
}

/** ts is a bucket's closing instant, so the newest available bucket lags "now" by at most one
 *  bucket width; add a few poll cycles of margin on top for delay/jitter. */
export function computeStaleAfterMs(bucketSizeMs: number, pollIntervalMs: number) {
  return bucketSizeMs + MARGIN_POLLS * pollIntervalMs
}

/** Picks each group's most recent sample, dropping samples too old to still be "current". */
export function latestFreshSamples<T extends { ts: number }>(
  samples: T[],
  groupKey: (sample: T) => string,
  staleAfterMs: number
): T[] {
  const cutoff = Date.now() - staleAfterMs
  const latestByGroup = new Map<string, T>()
  for (const sample of samples) {
    if (sample.ts < cutoff) continue
    const current = latestByGroup.get(groupKey(sample))
    if (!current || sample.ts > current.ts) latestByGroup.set(groupKey(sample), sample)
  }
  return [...latestByGroup.values()]
}
