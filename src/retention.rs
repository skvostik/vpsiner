use std::sync::Arc;
use std::time::Duration;

use crate::logs::{format_timestamp_ms, store::LogStore};
use crate::metrics::store::MetricsStore;

pub async fn cleanup_once(
    metrics: &Arc<dyn MetricsStore>,
    logs: &Arc<dyn LogStore>,
    retention_weeks: u32,
) {
    let cutoff_ms = retention_cutoff_ms(time::OffsetDateTime::now_utc(), retention_weeks);
    let cutoff = format_timestamp_ms(cutoff_ms);
    let metrics_cleaned = metrics.delete_before(cutoff_ms).await;
    let logs_removed = logs.delete_before(cutoff_ms).await;

    match (metrics_cleaned, logs_removed) {
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
}

pub async fn run(metrics: Arc<dyn MetricsStore>, logs: Arc<dyn LogStore>, retention_weeks: u32) {
    loop {
        tokio::time::sleep(Duration::from_secs(24 * 60 * 60)).await;
        cleanup_once(&metrics, &logs, retention_weeks).await;
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
}
