/** Never poll metrics faster than the backend actually samples them. */
export function computePollIntervalMs(sampleIntervalMs: number, floorMs = 2_000) {
  return Math.max(sampleIntervalMs, floorMs)
}
