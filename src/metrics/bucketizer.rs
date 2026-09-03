use std::collections::VecDeque;
use std::time::Duration;

use crate::model::TimestampMs;

/// Fixed-point scale for rates returned by `CounterBucketizer`.
///
/// A returned rate is measured in thousandths of the source counter's unit per
/// second. For example, if samples count bytes, `1_234` represents
/// `1.234 bytes/s`.
const COUNTER_RATE_SCALE: u64 = 1_000;

/// Buckets of samples a bucketizer keeps buffered. Only the target bucket and its two
/// neighbours are ever read, so the surplus is margin against a stalled collector.
const BUFFERED_BUCKETS: u128 = 8;

/// Sample capacity one bucketizer needs to close a bucket at the given collection rate.
pub(crate) fn buffer_capacity(collect_interval: Duration, bucket_len_ms: u64) -> usize {
    let interval_ms = collect_interval.as_millis().max(1);
    ((BUFFERED_BUCKETS * bucket_len_ms as u128) / interval_ms).max(4) as usize
}

/// One raw metric sample captured at an arbitrary timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawSample {
    ts: TimestampMs,
    value: u64,
}

/// Interpolates raw samples into one derived value per closed bucket.
///
/// Implementations share the same data eligibility rules:
/// - samples are stored in a fixed-size FIFO buffer,
/// - bucket length is fixed at construction time,
/// - each boundary may use only the target bucket and its immediate neighbor,
/// - and at least one real sample must belong to the bucket window.
///
/// The target bucket is the closed interval
/// `(bucket_end - bucket_len_ms, bucket_end]`.
///
/// Gauge bucketizers return the rounded mean in their input unit. Counter
/// bucketizers return milli-units per second; see `COUNTER_RATE_SCALE`.
pub(crate) trait Bucketizer {
    /// Pushes one raw sample into the in-memory buffer.
    fn push(&mut self, ts: TimestampMs, value: u64);

    /// Computes one value for the target bucket.
    ///
    /// Returns `None` when interpolation is not possible, or when the bucket
    /// has no raw samples in it.
    ///
    /// The bucket duration is configured in the concrete bucketizer constructor.
    fn collect(&self, bucket_end: TimestampMs) -> Option<u64>;
}

/// Fixed-capacity FIFO of timestamped samples ordered by insertion time.
///
/// The intended input stream is time-ordered. Older or duplicate samples are
/// ignored so interpolation remains well-defined.
#[derive(Debug, Clone)]
struct SampleBuffer {
    capacity: usize,
    samples: VecDeque<RawSample>,
}

impl SampleBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            samples: VecDeque::with_capacity(capacity),
        }
    }

    fn push(&mut self, ts: TimestampMs, value: u64) {
        if self.capacity == 0 {
            return;
        }

        if self.samples.back().is_some_and(|last| ts <= last.ts) {
            return;
        }

        self.samples.push_back(RawSample { ts, value });
        while self.samples.len() > self.capacity {
            self.samples.pop_front();
        }
    }

    fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Finds a boundary value from the target bucket and its immediate neighbor.
    ///
    /// An exact sample at `boundary` is used directly. Otherwise the boundary
    /// is linearly interpolated from samples inside `(left_min, boundary)` and
    /// `(boundary, right_max]`. Samples in more distant buckets are ignored.
    fn interpolate_boundary(
        &self,
        boundary: TimestampMs,
        left_min: TimestampMs,
        right_max: TimestampMs,
    ) -> Option<f64> {
        if let Some(sample) = self.samples.iter().find(|sample| sample.ts == boundary) {
            return Some(sample.value as f64);
        }

        let mut left: Option<RawSample> = None;
        let mut right: Option<RawSample> = None;

        for sample in &self.samples {
            if sample.ts > left_min && sample.ts < boundary {
                left = Some(*sample);
            }
            if sample.ts > boundary && sample.ts <= right_max {
                right = Some(*sample);
                break;
            }
        }

        let left = left?;
        let right = right?;

        let before_boundary = boundary.checked_sub(left.ts)?;
        let after_boundary = right.ts.checked_sub(boundary)?;
        let span = before_boundary.checked_add(after_boundary)?;
        if span <= 0 {
            return None;
        }

        let alpha = before_boundary as f64 / span as f64;
        Some(left.value as f64 + alpha * (right.value as f64 - left.value as f64))
    }

    /// Returns boundary-interpolated points plus all strictly interior raw samples.
    ///
    /// Output points are strictly time-ordered and include both boundaries.
    fn bucket_points(
        &self,
        bucket_start: TimestampMs,
        bucket_end: TimestampMs,
        bucket_len_ms: i64,
    ) -> Option<Vec<(TimestampMs, f64)>> {
        if bucket_start >= bucket_end || bucket_len_ms <= 0 {
            return None;
        }

        let previous_bucket_start = bucket_start.checked_sub(bucket_len_ms)?;
        let next_bucket_end = bucket_end.checked_add(bucket_len_ms)?;
        let start_value =
            self.interpolate_boundary(bucket_start, previous_bucket_start, bucket_end)?;
        let end_value = self.interpolate_boundary(bucket_end, bucket_start, next_bucket_end)?;

        let mut points: Vec<(TimestampMs, f64)> = Vec::new();
        points.push((bucket_start, start_value));

        let mut has_bucket_sample = false;
        for sample in &self.samples {
            if sample.ts > bucket_start && sample.ts <= bucket_end {
                has_bucket_sample = true;
            }
            if sample.ts > bucket_start && sample.ts < bucket_end {
                points.push((sample.ts, sample.value as f64));
            }
        }

        if !has_bucket_sample {
            return None;
        }

        points.push((bucket_end, end_value));
        Some(points)
    }
}

/// Bucketizer for gauge-like values (e.g. stored bytes, memory usage).
///
/// The produced bucket value is a time-weighted mean obtained from the
/// trapezoidal integral over the interpolated curve, rounded to the nearest
/// whole input unit.
#[derive(Debug, Clone)]
pub(crate) struct GaugeBucketizer {
    buffer: SampleBuffer,
    bucket_len_ms: u64,
}

impl GaugeBucketizer {
    /// Creates a gauge bucketizer with fixed sample capacity and bucket length.
    pub(crate) fn new(capacity: usize, bucket_len_ms: u64) -> Self {
        Self {
            buffer: SampleBuffer::new(capacity),
            bucket_len_ms,
        }
    }
}

impl Bucketizer for GaugeBucketizer {
    fn push(&mut self, ts: TimestampMs, value: u64) {
        self.buffer.push(ts, value);
    }

    fn collect(&self, bucket_end: TimestampMs) -> Option<u64> {
        if self.buffer.is_empty() || self.bucket_len_ms == 0 {
            return None;
        }

        let bucket_len_ms = i64::try_from(self.bucket_len_ms).ok()?;
        let bucket_start = bucket_end.checked_sub(bucket_len_ms)?;
        let points = self
            .buffer
            .bucket_points(bucket_start, bucket_end, bucket_len_ms)?;

        let mut integral = 0.0;
        for pair in points.windows(2) {
            let (left_ts, left_value) = pair[0];
            let (right_ts, right_value) = pair[1];
            let dt = right_ts.checked_sub(left_ts)?;
            if dt <= 0 {
                return None;
            }
            integral += (left_value + right_value) * 0.5 * dt as f64;
        }

        round_to_u64(integral / bucket_len_ms as f64)
    }
}

/// Bucketizer for monotonic counters.
///
/// The produced bucket value is the slope between interpolated start/end
/// boundary values, scaled into milli-units per second:
/// `rate = (end - start) * COUNTER_RATE_SCALE / bucket_len_seconds`.
///
/// A bucket is rejected when any raw sample needed to interpolate its
/// boundaries, including samples within the bucket, decreases from its
/// predecessor. Such a decrease indicates a counter reset.
#[derive(Debug, Clone)]
pub(crate) struct CounterBucketizer {
    buffer: SampleBuffer,
    bucket_len_ms: u64,
}

impl CounterBucketizer {
    /// Creates a counter bucketizer with fixed sample capacity and bucket length.
    pub(crate) fn new(capacity: usize, bucket_len_ms: u64) -> Self {
        Self {
            buffer: SampleBuffer::new(capacity),
            bucket_len_ms,
        }
    }
}

impl Bucketizer for CounterBucketizer {
    fn push(&mut self, ts: TimestampMs, value: u64) {
        self.buffer.push(ts, value);
    }

    fn collect(&self, bucket_end: TimestampMs) -> Option<u64> {
        if self.buffer.is_empty() || self.bucket_len_ms == 0 {
            return None;
        }

        let bucket_len_ms = i64::try_from(self.bucket_len_ms).ok()?;
        let bucket_start = bucket_end.checked_sub(bucket_len_ms)?;
        let previous_bucket_start = bucket_start.checked_sub(bucket_len_ms)?;
        let next_bucket_end = bucket_end.checked_add(bucket_len_ms)?;

        let start_sample_index =
            self.buffer.samples.iter().rposition(|sample| {
                sample.ts > previous_bucket_start && sample.ts <= bucket_start
            })?;
        let end_sample_index = self
            .buffer
            .samples
            .iter()
            .position(|sample| sample.ts >= bucket_end && sample.ts <= next_bucket_end)?;
        if start_sample_index > end_sample_index {
            return None;
        }

        let relevant_samples: Vec<&RawSample> = self
            .buffer
            .samples
            .iter()
            .enumerate()
            .filter_map(|(index, sample)| {
                (start_sample_index <= index && index <= end_sample_index).then_some(sample)
            })
            .collect();
        if !relevant_samples
            .windows(2)
            .all(|pair| pair[0].value <= pair[1].value)
        {
            return None;
        }

        // Keep the same immediate-neighbor interpolation and bucket eligibility
        // rules as gauge buckets.
        let points = self
            .buffer
            .bucket_points(bucket_start, bucket_end, bucket_len_ms)?;
        let start = points.first()?.1;
        let end = points.last()?.1;
        if end < start {
            return None;
        }
        round_to_u64((end - start) * COUNTER_RATE_SCALE as f64 * 1_000.0 / bucket_len_ms as f64)
    }
}

/// Rounds a non-negative computed value to `u64` at the bucket boundary.
///
/// All interpolation and bucket arithmetic retains fractional precision until
/// this final conversion.
fn round_to_u64(value: f64) -> Option<u64> {
    if !value.is_finite() || value < 0.0 || value > u64::MAX as f64 {
        return None;
    }
    Some(value.round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_buffer_enforces_capacity() {
        let mut buffer = SampleBuffer::new(3);
        buffer.push(1, 10);
        buffer.push(2, 20);
        buffer.push(3, 30);
        buffer.push(4, 40);

        let values: Vec<u64> = buffer.samples.iter().map(|sample| sample.value).collect();
        assert_eq!(values, vec![20, 30, 40]);
    }

    #[test]
    fn sample_buffer_rejects_duplicate_and_older_timestamps() {
        let mut buffer = SampleBuffer::new(3);
        buffer.push(1_000, 10);
        buffer.push(1_000, 20);
        buffer.push(500, 30);

        assert_eq!(
            buffer.samples.make_contiguous(),
            &[RawSample {
                ts: 1_000,
                value: 10
            }]
        );
    }

    #[test]
    fn boundary_interpolation_retains_fractional_precision() {
        let mut buffer = SampleBuffer::new(2);
        buffer.push(0, 0);
        buffer.push(1_000, 1);

        assert_eq!(buffer.interpolate_boundary(500, -1, 1_000), Some(0.5));
    }

    #[test]
    fn gauge_collects_time_weighted_mean() {
        // Linear ramp 0 -> 30 over 3 seconds; middle second rounds to 15.
        let mut bucketer = GaugeBucketizer::new(16, 1_000);
        bucketer.push(0, 0);
        bucketer.push(1_000, 10);
        bucketer.push(2_000, 20);
        bucketer.push(3_000, 30);

        assert_eq!(bucketer.collect(2_000), Some(15));
    }

    #[test]
    fn counter_collects_interpolated_rate() {
        // Interpolated boundaries: at 1s => 100, at 2s => 200, so 100_000 milli-units/s.
        let mut bucketer = CounterBucketizer::new(16, 1_000);
        bucketer.push(500, 50);
        bucketer.push(1_500, 150);
        bucketer.push(2_500, 250);

        assert_eq!(bucketer.collect(2_000), Some(100_000));
    }

    #[test]
    fn collect_returns_none_when_boundary_cannot_be_interpolated() {
        let mut bucketer = GaugeBucketizer::new(16, 1_000);
        bucketer.push(1_100, 10);
        bucketer.push(1_500, 15);
        bucketer.push(1_900, 20);

        assert_eq!(bucketer.collect(2_000), None);
    }

    #[test]
    fn collect_returns_none_without_a_target_bucket_sample() {
        let mut bucketer = GaugeBucketizer::new(16, 1_000);
        bucketer.push(1000, 10);
        bucketer.push(2_500, 30);

        // The target bucket is (1_000, 2_000]; both samples are outside it.
        assert_eq!(bucketer.collect(2_000), None);
    }

    #[test]
    fn collect_returns_none_when_start_boundary_needs_a_distant_sample() {
        let mut bucketer = GaugeBucketizer::new(16, 1_000);
        bucketer.push(-1_500, 0);
        bucketer.push(1_500, 30);
        bucketer.push(2_500, 40);

        // The prior sample is in neither the previous bucket nor the target bucket.
        assert_eq!(bucketer.collect(2_000), None);
    }

    #[test]
    fn collect_returns_none_when_end_boundary_needs_a_distant_sample() {
        let mut bucketer = GaugeBucketizer::new(16, 1_000);
        bucketer.push(500, 0);
        bucketer.push(1_500, 10);
        bucketer.push(3_500, 30);

        // The following sample is in neither the target bucket nor the next bucket.
        assert_eq!(bucketer.collect(2_000), None);
    }

    #[test]
    fn collect_accepts_sample_at_next_bucket_end() {
        let mut bucketer = GaugeBucketizer::new(16, 1_000);
        bucketer.push(500, 0);
        bucketer.push(1_500, 16);
        bucketer.push(3_000, 16);

        assert_eq!(bucketer.collect(2_000), Some(14));
    }

    #[test]
    fn gauge_accepts_sample_at_bucket_end() {
        let mut bucketer = GaugeBucketizer::new(16, 2_000);
        bucketer.push(0, 0);
        bucketer.push(2_000, 20);

        assert_eq!(bucketer.collect(2_000), Some(10));
    }

    #[test]
    fn counter_returns_none_when_value_decreases() {
        let mut bucketer = CounterBucketizer::new(16, 1_000);
        bucketer.push(0, 200);
        bucketer.push(1_000, 100);
        bucketer.push(2_000, 50);

        assert_eq!(bucketer.collect(2_000), None);
    }

    #[test]
    fn counter_returns_none_when_it_resets_inside_an_otherwise_positive_bucket() {
        let mut bucketer = CounterBucketizer::new(16, 1_000);
        bucketer.push(500, 100);
        bucketer.push(1_200, 200);
        bucketer.push(1_500, 0);
        bucketer.push(2_500, 1_000);

        // The interpolated end is higher than the start, but 200 -> 0 is a reset.
        assert_eq!(bucketer.collect(2_000), None);
    }

    #[test]
    fn counter_preserves_milli_unit_rate_precision() {
        let mut bucketer = CounterBucketizer::new(16, 10_000);
        bucketer.push(0, 0);
        bucketer.push(5_000, 1);
        bucketer.push(10_000, 1);

        assert_eq!(bucketer.collect(10_000), Some(100));
    }

    #[test]
    fn collect_returns_none_when_bucket_len_is_zero() {
        let mut bucketer = GaugeBucketizer::new(16, 0);
        bucketer.push(0, 10);
        bucketer.push(1_000, 20);
        bucketer.push(2_000, 30);

        assert_eq!(bucketer.collect(2_000), None);
    }
}
