use std::sync::Arc;
use std::time::Duration;

use crate::logs::{format_timestamp_ms, store::LogStore};
use crate::metadata::{MetadataStore, ServiceRegistry};
use crate::metrics::store::MetricsStore;
use crate::model::metrics::MetricsResolution;

pub async fn cleanup_once(
    metrics: &Arc<dyn MetricsStore>,
    logs: &Arc<dyn LogStore>,
    metadata: &Arc<dyn MetadataStore>,
    services: &Arc<ServiceRegistry>,
    retention_weeks: u32,
) {
    let cutoff_ms = retention_cutoff_ms(time::OffsetDateTime::now_utc(), retention_weeks);
    let cutoff = format_timestamp_ms(cutoff_ms);
    let metrics_cleaned = metrics.delete_before(cutoff_ms).await;
    let logs_removed = logs.delete_before(cutoff_ms).await;

    match (&metrics_cleaned, &logs_removed) {
        (Ok(metrics_cleaned), Ok(logs_removed)) => {
            tracing::info!(
                retention_weeks,
                cutoff = %cutoff,
                metrics_cleaned,
                logs_removed = ?logs_removed,
                "retention cleanup completed"
            );
        }
        (Err(error), _) => tracing::error!(error = %error, "failed to clean expired metrics"),
        (_, Err(error)) => tracing::error!(error = %error, "failed to clean expired log databases"),
    }

    // Only prune checkpoints once the log databases they dedup against are confirmed gone,
    // otherwise a later logs cleanup retry would treat already-checkpointed lines as new.
    if logs_removed.is_ok() {
        match metadata.delete_before(cutoff_ms).await {
            Ok(checkpoints_removed) => tracing::info!(
                cutoff = %cutoff,
                checkpoints_removed,
                "retention cleanup removed stale log checkpoints"
            ),
            Err(error) => {
                tracing::error!(error = %error, "failed to clean expired log checkpoints")
            }
        }
    }

    // Reclaimed last, so no surviving metrics row or checkpoint can reference a dropped sid.
    // Rows are stamped at their bucket end and `last_seen_ms` lags activity by the touch
    // debounce, so the cutoff must clear both or it orphans rows this same pass just spared.
    if metrics_cleaned.is_ok() && logs_removed.is_ok() {
        let reclaim_cutoff_ms = cutoff_ms
            - MetricsResolution::max_bucket_ms() as i64
            - ServiceRegistry::MAX_WATERMARK_LAG_MS;
        match services.reclaim_before(reclaim_cutoff_ms).await {
            Ok(reclaimed) => {
                tracing::info!(
                    cutoff = %format_timestamp_ms(reclaim_cutoff_ms),
                    services_reclaimed = reclaimed.len(),
                    "retention cleanup reclaimed stale service ids"
                );
            }
            Err(error) => {
                tracing::error!(error = %error, "failed to reclaim stale service ids")
            }
        }
    }
}

pub async fn run(
    metrics: Arc<dyn MetricsStore>,
    logs: Arc<dyn LogStore>,
    metadata: Arc<dyn MetadataStore>,
    services: Arc<ServiceRegistry>,
    retention_weeks: u32,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(24 * 60 * 60)).await;
        cleanup_once(&metrics, &logs, &metadata, &services, retention_weeks).await;
    }
}

pub fn retention_cutoff_ms(now: time::OffsetDateTime, retention_weeks: u32) -> i64 {
    let days_since_monday = i64::from(now.date().weekday().number_days_from_monday());
    let current_week = now.date() - time::Duration::days(days_since_monday);
    let cutoff = current_week - time::Duration::weeks(i64::from(retention_weeks));
    time::OffsetDateTime::new_utc(cutoff, time::Time::MIDNIGHT).unix_timestamp() * 1_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logs::store::MockLogStore;
    use crate::metadata::SqliteMetadataStore;
    use crate::metrics::store::MockMetricsStore;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn cutoff_starts_on_a_monday_at_midnight_utc() {
        let now = time::OffsetDateTime::new_utc(
            time::Date::from_calendar_date(2026, time::Month::August, 23).unwrap(),
            time::Time::MIDNIGHT,
        );

        let cutoff = time::OffsetDateTime::from_unix_timestamp(
            retention_cutoff_ms(now, 4).div_euclid(1_000),
        )
        .unwrap();
        assert_eq!(cutoff.date().to_string(), "2026-07-20");
        assert_eq!(cutoff.time(), time::Time::MIDNIGHT);
    }

    #[tokio::test]
    async fn spares_services_whose_watermark_lags_but_whose_rows_survive() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("vpsiner-retention-{suffix}.db"));

        let metadata: Arc<dyn MetadataStore> = Arc::new(
            SqliteMetadataStore::connect(&path, 1_024, Duration::from_secs(5))
                .await
                .unwrap(),
        );
        let cutoff_ms = retention_cutoff_ms(time::OffsetDateTime::now_utc(), 4);

        // Trails the cutoff by less than the worst case lag, so its rows can still be above it.
        let lagging = metadata
            .resolve_service("lagging", cutoff_ms - 30 * 60 * 1_000)
            .await
            .unwrap();
        let ancient = metadata
            .resolve_service("ancient", cutoff_ms - 7 * 24 * 60 * 60 * 1_000)
            .await
            .unwrap();

        let services = Arc::new(ServiceRegistry::load(metadata.clone()).await.unwrap());

        let mut metrics = MockMetricsStore::new();
        metrics.expect_delete_before().returning(|_| Ok(0));
        let metrics: Arc<dyn MetricsStore> = Arc::new(metrics);

        let mut logs = MockLogStore::new();
        logs.expect_delete_before().returning(|_| Ok(Vec::new()));
        let logs: Arc<dyn LogStore> = Arc::new(logs);

        cleanup_once(&metrics, &logs, &metadata, &services, 4).await;

        assert_eq!(services.name(lagging).as_deref(), Some("lagging"));
        assert_eq!(services.name(ancient), None);
        assert_eq!(
            metadata.list_services().await.unwrap(),
            vec![(lagging, "lagging".to_string())]
        );

        let _ = tokio::fs::remove_file(&path).await;
    }
}
