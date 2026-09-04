//! Materialises the coarse resolution tables from the `10s` tables.
//!
//! A coarse bucket is rolled up by the first `10s` insert that lands past its closing
//! instant, so the coarse tables only ever hold fully elapsed buckets.

use std::collections::HashMap;

use sqlx::SqlitePool;

use crate::error::AppResult;
use crate::metrics::downsampling::{bucket_end, downsample_container, downsample_host};
use crate::metrics::schema;
use crate::model::{
    container_id::ContainerId,
    metrics::{ContainerSample, MetricsResolution},
    time::TimeRange,
};

/// The bucket that `previous_ts` fell into, if `new_ts` has moved past its end.
fn closed_bucket(previous_ts: i64, new_ts: i64, resolution: MetricsResolution) -> Option<i64> {
    let bucket_ms = resolution.bucket_ms();
    let previous = bucket_end(previous_ts, bucket_ms);
    (bucket_end(new_ts, bucket_ms) != previous).then_some(previous)
}

/// The `10s` rows making up the bucket closing at `bucket_end`.
fn source_range(bucket_end: i64, resolution: MetricsResolution) -> TimeRange {
    let bucket_ms = i64::try_from(resolution.bucket_ms()).expect("bucket size exceeds i64");
    // select_* is inclusive on both ends while a bucket is half-open at the start.
    TimeRange {
        from: bucket_end - bucket_ms + 1,
        to: bucket_end,
    }
}

/// Every resolution that closed a bucket this tick, paired with the timestamp it closed at.
fn resolutions_closed_by(previous_ts: i64, new_ts: i64) -> Vec<(MetricsResolution, i64)> {
    MetricsResolution::COARSE
        .into_iter()
        .filter_map(|resolution| {
            closed_bucket(previous_ts, new_ts, resolution).map(|closed| (resolution, closed))
        })
        .collect()
}

/// The widest range needed to cover every closed resolution, since coarser buckets always
/// contain the finer ones that close alongside them.
fn widest_source_range(closed: &[(MetricsResolution, i64)]) -> Option<TimeRange> {
    closed
        .iter()
        .max_by_key(|(resolution, _)| resolution.bucket_ms())
        .map(|&(resolution, closed)| source_range(closed, resolution))
}

/// The contiguous run of `samples` (ordered by `ts`) falling inside `range`.
fn slice_by_range<T>(samples: &[T], range: TimeRange, ts: impl Fn(&T) -> i64) -> &[T] {
    let start = samples.partition_point(|sample| ts(sample) < range.from);
    let end = samples.partition_point(|sample| ts(sample) <= range.to);
    &samples[start..end]
}

/// Rolls up every coarse resolution whose bucket closed between `previous_ts` and `new_ts`,
/// reading the `10s` rows once for whichever resolution needed the widest range.
pub async fn roll_up_host(
    pool: &SqlitePool,
    previous_ts: i64,
    new_ts: i64,
    max_gap_pct: u8,
) -> AppResult<()> {
    let closed = resolutions_closed_by(previous_ts, new_ts);
    let Some(fetch_range) = widest_source_range(&closed) else {
        return Ok(());
    };

    let samples = schema::select_host(pool, MetricsResolution::TenSeconds, fetch_range).await?;

    for (resolution, closed) in closed {
        let slice = slice_by_range(&samples, source_range(closed, resolution), |sample| {
            sample.ts
        });

        for sample in downsample_host(slice, resolution, max_gap_pct) {
            schema::insert_host(pool, resolution, sample).await?;
        }
    }

    Ok(())
}

/// Rolls up every coarse resolution whose bucket closed between `previous_ts` and `new_ts`,
/// reading the `10s` rows once for whichever resolution needed the widest range.
pub async fn roll_up_containers(
    pool: &SqlitePool,
    previous_ts: i64,
    new_ts: i64,
    max_gap_pct: u8,
) -> AppResult<()> {
    let closed = resolutions_closed_by(previous_ts, new_ts);
    let Some(fetch_range) = widest_source_range(&closed) else {
        return Ok(());
    };

    let samples =
        schema::select_containers(pool, MetricsResolution::TenSeconds, fetch_range).await?;

    for (resolution, closed) in closed {
        let slice = slice_by_range(&samples, source_range(closed, resolution), |sample| {
            sample.ts
        });

        let mut by_container: HashMap<ContainerId, Vec<&ContainerSample>> = HashMap::new();
        for sample in slice {
            by_container.entry(sample.cid).or_default().push(sample);
        }

        let rolled: Vec<ContainerSample> = by_container
            .into_values()
            .flat_map(|samples| downsample_container(samples, resolution, max_gap_pct))
            .collect();
        schema::insert_containers(pool, resolution, rolled).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_the_bucket_left_behind_by_the_new_sample() {
        assert_eq!(
            closed_bucket(59_000, 61_000, MetricsResolution::OneMinute),
            Some(60_000)
        );
    }

    #[test]
    fn a_sample_on_the_boundary_still_belongs_to_the_bucket_it_closes() {
        assert_eq!(
            closed_bucket(50_000, 60_000, MetricsResolution::OneMinute),
            None
        );
        assert_eq!(
            closed_bucket(60_000, 70_000, MetricsResolution::OneMinute),
            Some(60_000)
        );
    }

    #[test]
    fn stays_quiet_inside_a_bucket() {
        assert_eq!(
            closed_bucket(10_000, 20_000, MetricsResolution::OneMinute),
            None
        );
    }

    #[test]
    fn source_range_covers_exactly_one_bucket() {
        let range = source_range(120_000, MetricsResolution::OneMinute);
        assert_eq!(range.from, 60_001);
        assert_eq!(range.to, 120_000);
    }
}
