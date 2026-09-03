//! Materialises the coarse resolution tables from the `10s` tables.
//!
//! A coarse bucket is rolled up by the first `10s` insert that lands past its closing
//! instant, so the coarse tables only ever hold fully elapsed buckets.

use std::collections::HashMap;

use sqlx::SqlitePool;

use crate::error::AppResult;
use crate::metrics::downsampling::{bucket_end, downsample_container, downsample_host};
use crate::metrics::schema;
use crate::model::{ContainerSample, MetricsResolution, TimeRange};

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

/// Rolls up every coarse resolution whose bucket closed between `previous_ts` and `new_ts`.
pub async fn roll_up_host(pool: &SqlitePool, previous_ts: i64, new_ts: i64) -> AppResult<()> {
    for resolution in MetricsResolution::COARSE {
        let Some(closed) = closed_bucket(previous_ts, new_ts, resolution) else {
            continue;
        };

        let samples = schema::select_host(
            pool,
            MetricsResolution::TenSeconds,
            source_range(closed, resolution),
        )
        .await?;

        for sample in downsample_host(samples, resolution) {
            schema::insert_host(pool, resolution, sample).await?;
        }
    }

    Ok(())
}

/// Rolls up every coarse resolution whose bucket closed between `previous_ts` and `new_ts`.
pub async fn roll_up_containers(pool: &SqlitePool, previous_ts: i64, new_ts: i64) -> AppResult<()> {
    for resolution in MetricsResolution::COARSE {
        let Some(closed) = closed_bucket(previous_ts, new_ts, resolution) else {
            continue;
        };

        let samples = schema::select_containers(
            pool,
            MetricsResolution::TenSeconds,
            source_range(closed, resolution),
        )
        .await?;

        let mut by_container: HashMap<String, Vec<ContainerSample>> = HashMap::new();
        for sample in samples {
            by_container
                .entry(sample.cid.clone())
                .or_default()
                .push(sample);
        }

        let rolled: Vec<ContainerSample> = by_container
            .into_values()
            .flat_map(|samples| downsample_container(samples, resolution))
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
