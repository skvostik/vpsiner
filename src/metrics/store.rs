use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::SqlitePool;
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};

use crate::error::{AppError, AppResult};
use crate::metrics::downsampling::{downsample_container, downsample_host, sum_by_bucket};
use crate::metrics::{rollup, schema};
use crate::model::{
    ContainerGroupMetrics, ContainerPoint, ContainerSample, HostPoint, HostSample,
    MetricsResolution, TimeRange,
};

/// Persistence for `metrics.db`.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait MetricsStore: Send + Sync + 'static {
    async fn database_size_bytes(&self) -> AppResult<u64>;

    async fn delete_before(&self, cutoff_ms: i64) -> AppResult<u64>;

    async fn insert_host(&self, sample: HostSample) -> AppResult<()>;

    async fn insert_containers(&self, samples: Vec<ContainerSample>) -> AppResult<()>;

    async fn query_host(
        &self,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<Vec<HostPoint>>;

    /// Returns every service's samples, each broken down by container as well as summed;
    /// callers filter to a single service themselves when that's all they need.
    async fn query_containers(
        &self,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<HashMap<String, ContainerGroupMetrics>>;

    /// Checkpoints the write-ahead log and releases the connection.
    async fn close(&self);
}

/// Only a handful of distinct statements are ever prepared against this database.
const STATEMENT_CACHE_CAPACITY: usize = 32;
/// Recommended by SQLite for `PRAGMA optimize`.
const ANALYSIS_LIMIT: u32 = 400;
/// Free pages returned per `incremental_vacuum` step; must match [`VACUUM_STEP`].
const VACUUM_CHUNK_PAGES: u64 = 1_000;
const VACUUM_STEP: &str = "PRAGMA incremental_vacuum(1000)";
/// SQLite-backed implementation.
pub struct SqliteMetricsStore {
    db_path: PathBuf,
    pool: SqlitePool,
    /// Newest `10s` bucket written per source; a change of coarse bucket triggers a rollup.
    last_host_ts: Mutex<Option<i64>>,
    last_containers_ts: Mutex<Option<i64>>,
    downsample_max_gap_pct: u8,
}

impl SqliteMetricsStore {
    /// Opens the single long-lived connection used for the lifetime of the process.
    pub async fn connect(
        db_path: impl AsRef<Path>,
        cache_size_kb: u64,
        busy_timeout: Duration,
        downsample_max_gap_pct: u8,
    ) -> AppResult<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        if let Some(parent) = db_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(storage)?;
        }

        tracing::info!(database = %db_path.display(), "opening metrics database connection");

        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .auto_vacuum(SqliteAutoVacuum::Incremental)
            .busy_timeout(busy_timeout)
            .foreign_keys(false)
            .statement_cache_capacity(STATEMENT_CACHE_CAPACITY)
            .analysis_limit(ANALYSIS_LIMIT)
            .optimize_on_close(true, ANALYSIS_LIMIT)
            // Negative values are interpreted as KiB rather than pages.
            .pragma("cache_size", format!("-{cache_size_kb}"));

        // A single connection serialises every reader and writer, so the database is
        // never contended from within this process.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(connect_options)
            .await
            .map_err(storage)?;

        let store = Self {
            db_path,
            pool,
            last_host_ts: Mutex::new(None),
            last_containers_ts: Mutex::new(None),
            downsample_max_gap_pct,
        };
        store.create_schema().await?;

        *store.last_host_ts.lock().expect("poisoned") =
            schema::select_host_max_ts(&store.pool, MetricsResolution::TenSeconds).await?;
        *store.last_containers_ts.lock().expect("poisoned") =
            schema::select_containers_max_ts(&store.pool, MetricsResolution::TenSeconds).await?;

        Ok(store)
    }

    async fn create_schema(&self) -> AppResult<()> {
        for resolution in [
            MetricsResolution::TenSeconds,
            MetricsResolution::OneMinute,
            MetricsResolution::FiveMinutes,
            MetricsResolution::OneHour,
        ] {
            schema::create_host_table(&self.pool, resolution).await?;
            schema::create_container_table(&self.pool, resolution).await?;
        }
        Ok(())
    }

    /// Records the newest `10s` bucket, returning the one before it when time moved on.
    ///
    /// A `new_ts` that does not move forward means the clock stepped backwards, so no
    /// earlier bucket can be treated as complete.
    fn advance(cursor: &Mutex<Option<i64>>, new_ts: i64) -> Option<i64> {
        let mut last = cursor.lock().expect("poisoned");
        let previous = last.filter(|previous| new_ts > *previous);
        *last = Some(new_ts);
        previous
    }

    /// Returns free pages to the filesystem in bounded steps so a large retention
    /// delete never blocks the connection for one long stretch.
    async fn reclaim_free_pages(&self) {
        let free_pages: i64 = match sqlx::query_scalar("PRAGMA freelist_count")
            .fetch_one(&self.pool)
            .await
        {
            Ok(pages) => pages,
            Err(err) => {
                tracing::warn!(error = %err, "failed to read metrics database freelist");
                return;
            }
        };

        for _ in 0..free_pages.unsigned_abs().div_ceil(VACUUM_CHUNK_PAGES) {
            if let Err(err) = sqlx::query(VACUUM_STEP).execute(&self.pool).await {
                tracing::warn!(error = %err, "failed to vacuum metrics database");
                return;
            }
        }

        if let Err(err) = sqlx::query("PRAGMA optimize").execute(&self.pool).await {
            tracing::warn!(error = %err, "failed to optimize metrics database");
        }
    }

    /// Path of the write-ahead log holding not-yet-checkpointed pages.
    fn wal_path(&self) -> PathBuf {
        let mut path = self.db_path.clone().into_os_string();
        path.push("-wal");
        PathBuf::from(path)
    }
}

fn storage(err: impl std::fmt::Display) -> AppError {
    AppError::Storage(err.to_string())
}

/// The head of `range` that predates the rollup table and must come from the `10s` rows.
///
/// Rollups only run forwards, so a range reaching back before the first rolled-up bucket
/// would otherwise come back short until retention drops that older data.
fn uncovered_head(
    stored_min_ts: Option<i64>,
    range: TimeRange,
    resolution: MetricsResolution,
) -> Option<TimeRange> {
    if resolution == MetricsResolution::TenSeconds {
        return None;
    }

    let bucket_ms = i64::try_from(resolution.bucket_ms()).expect("bucket size exceeds i64");
    let uncovered_to = match stored_min_ts {
        Some(min_ts) => min_ts - bucket_ms,
        None => range.to,
    };

    (uncovered_to >= range.from).then_some(TimeRange {
        from: range.from,
        to: uncovered_to.min(range.to),
    })
}

async fn file_size_bytes(path: &Path) -> AppResult<u64> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(storage(error)),
    }
}

#[async_trait]
impl MetricsStore for SqliteMetricsStore {
    async fn database_size_bytes(&self) -> AppResult<u64> {
        // Pages committed but not yet checkpointed still live in the write-ahead log.
        Ok(file_size_bytes(&self.db_path).await? + file_size_bytes(&self.wal_path()).await?)
    }

    async fn delete_before(&self, cutoff_ms: i64) -> AppResult<u64> {
        let mut deleted = 0;
        for resolution in [
            MetricsResolution::TenSeconds,
            MetricsResolution::OneMinute,
            MetricsResolution::FiveMinutes,
            MetricsResolution::OneHour,
        ] {
            deleted += schema::delete_host_before(&self.pool, resolution, cutoff_ms).await?;
            deleted += schema::delete_containers_before(&self.pool, resolution, cutoff_ms).await?;
        }

        self.reclaim_free_pages().await;
        Ok(deleted)
    }

    async fn insert_host(&self, sample: HostSample) -> AppResult<()> {
        let ts = sample.ts;
        schema::insert_host(&self.pool, MetricsResolution::TenSeconds, sample).await?;

        // A failed rollup must not fail the insert, or the collector would stop
        // announcing the bucket and every live stream would stall with it.
        if let Some(previous) = Self::advance(&self.last_host_ts, ts)
            && let Err(err) =
                rollup::roll_up_host(&self.pool, previous, ts, self.downsample_max_gap_pct).await
        {
            tracing::error!(error = %err, "failed to roll up host metrics");
        }

        Ok(())
    }

    async fn insert_containers(&self, samples: Vec<ContainerSample>) -> AppResult<()> {
        let Some(ts) = samples.iter().map(|sample| sample.ts).max() else {
            return Ok(());
        };
        schema::insert_containers(&self.pool, MetricsResolution::TenSeconds, samples).await?;

        if let Some(previous) = Self::advance(&self.last_containers_ts, ts)
            && let Err(err) =
                rollup::roll_up_containers(&self.pool, previous, ts, self.downsample_max_gap_pct)
                    .await
        {
            tracing::error!(error = %err, "failed to roll up container metrics");
        }

        Ok(())
    }

    async fn query_host(
        &self,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<Vec<HostPoint>> {
        let stored = schema::select_host(&self.pool, resolution, range).await?;

        let mut samples = match uncovered_head(
            schema::select_host_min_ts(&self.pool, resolution).await?,
            range,
            resolution,
        ) {
            Some(uncovered) => {
                let raw = schema::select_host(&self.pool, MetricsResolution::TenSeconds, uncovered)
                    .await?;
                downsample_host(&raw, resolution, self.downsample_max_gap_pct)
            }
            None => Vec::new(),
        };
        samples.extend(stored);

        Ok(samples
            .into_iter()
            .filter(|sample| sample.ts >= range.from)
            .map(HostPoint::from)
            .collect())
    }

    async fn query_containers(
        &self,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<HashMap<String, ContainerGroupMetrics>> {
        let stored = schema::select_containers(&self.pool, resolution, range).await?;

        let uncovered = uncovered_head(
            schema::select_containers_min_ts(&self.pool, resolution).await?,
            range,
            resolution,
        );

        let mut by_service_and_container: HashMap<String, HashMap<String, Vec<ContainerSample>>> =
            HashMap::new();
        let mut group = |sample: ContainerSample| {
            by_service_and_container
                .entry(sample.service.clone())
                .or_default()
                .entry(sample.cid.clone())
                .or_default()
                .push(sample);
        };

        if let Some(uncovered) = uncovered {
            let raw =
                schema::select_containers(&self.pool, MetricsResolution::TenSeconds, uncovered)
                    .await?;
            let mut raw_by_container: HashMap<String, Vec<ContainerSample>> = HashMap::new();
            for sample in raw {
                raw_by_container
                    .entry(sample.cid.clone())
                    .or_default()
                    .push(sample);
            }
            for samples in raw_by_container.into_values() {
                downsample_container(&samples, resolution, self.downsample_max_gap_pct)
                    .into_iter()
                    .for_each(&mut group);
            }
        }
        stored.into_iter().for_each(&mut group);

        let mut by_service = HashMap::new();
        for (service, by_container) in by_service_and_container {
            let mut containers: HashMap<String, Vec<ContainerPoint>> = HashMap::new();
            for (cid, mut samples) in by_container {
                samples.sort_by_key(|sample| sample.ts);
                let points = samples
                    .into_iter()
                    .filter(|sample| sample.ts >= range.from)
                    .map(ContainerPoint::from)
                    .collect();
                containers.insert(cid, points);
            }
            let sum = sum_by_bucket(containers.values());
            by_service.insert(service, ContainerGroupMetrics { sum, containers });
        }

        Ok(by_service)
    }

    async fn close(&self) {
        tracing::info!(database = %self.db_path.display(), "closing metrics database connection");
        self.pool.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_directory(name: &str) -> std::path::PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vpsiner-metrics-{name}-{suffix}"))
    }

    async fn test_store(db_path: impl AsRef<Path>) -> SqliteMetricsStore {
        SqliteMetricsStore::connect(db_path, 1_024, Duration::from_secs(5), 40)
            .await
            .unwrap()
    }

    fn host_sample(ts: i64) -> HostSample {
        HostSample {
            ts,
            cpu_pct_mill: 12_500,
            mem_used: 100,
            mem_total: 200,
            storage_used: 700,
            storage_total: 800,
            metrics_size: 900,
            logs_size: 1_000,
            net_rx_rate_mill: Some(300_000),
            net_tx_rate_mill: Some(400_000),
            disk_read_rate_mill: Some(500_000),
            disk_write_rate_mill: Some(600_000),
        }
    }

    fn container_sample(ts: i64, service: &str) -> ContainerSample {
        ContainerSample {
            ts,
            service: service.into(),
            cid: "abc123".into(),
            cpu_pct_mill: 25_000,
            mem_used: 1_000,
            mem_limit: 2_000,
            net_rx_rate_mill: Some(3_000_000),
            net_tx_rate_mill: Some(4_000_000),
            blk_read_rate_mill: Some(5_000_000),
            blk_write_rate_mill: Some(6_000_000),
        }
    }

    #[tokio::test]
    async fn persists_and_queries_samples_by_range() {
        let directory = test_directory("range");
        let store = test_store(directory.join("metrics.db")).await;

        store.insert_host(host_sample(10_000)).await.unwrap();
        store.insert_host(host_sample(20_000)).await.unwrap();
        store
            .insert_containers(vec![
                container_sample(10_000, "web"),
                container_sample(20_000, "worker"),
                container_sample(30_000, "web"),
            ])
            .await
            .unwrap();

        let hosts = store
            .query_host(
                TimeRange {
                    from: 15_000,
                    to: 25_000,
                },
                MetricsResolution::TenSeconds,
            )
            .await
            .unwrap();
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0].ts, 20_000);
        assert_eq!(hosts[0].cpu_pct, 12.5);
        assert_eq!(hosts[0].mem_used, 100);
        assert_eq!(hosts[0].net_rx_rate, Some(300.0));

        let by_service = store
            .query_containers(
                TimeRange {
                    from: 0,
                    to: 25_000,
                },
                MetricsResolution::TenSeconds,
            )
            .await
            .unwrap();
        let containers = &by_service["web"];
        assert_eq!(containers.sum.len(), 1);
        assert_eq!(containers.sum[0].ts, 10_000);
        assert_eq!(containers.sum[0].cpu_pct, 25.0);
        assert_eq!(containers.containers.len(), 1);
        assert_eq!(containers.containers["abc123"][0].ts, 10_000);
        assert_eq!(containers.containers["abc123"][0].service, "web");
        assert_eq!(containers.containers["abc123"][0].mem_used, 1_000);

        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn host_rates_round_trip_as_null_when_missing() {
        let directory = test_directory("host-null-rates");
        let store = test_store(directory.join("metrics.db")).await;

        store
            .insert_host(HostSample {
                net_rx_rate_mill: None,
                net_tx_rate_mill: None,
                disk_read_rate_mill: None,
                disk_write_rate_mill: None,
                ..host_sample(10_000)
            })
            .await
            .unwrap();

        let hosts = store
            .query_host(
                TimeRange {
                    from: 0,
                    to: 10_000,
                },
                MetricsResolution::TenSeconds,
            )
            .await
            .unwrap();
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0].net_rx_rate, None);
        assert_eq!(hosts[0].net_tx_rate, None);
        assert_eq!(hosts[0].disk_read_rate, None);
        assert_eq!(hosts[0].disk_write_rate, None);

        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn container_rates_round_trip_as_null_when_missing() {
        let directory = test_directory("container-null-rates");
        let store = test_store(directory.join("metrics.db")).await;

        store
            .insert_containers(vec![ContainerSample {
                net_rx_rate_mill: None,
                net_tx_rate_mill: None,
                blk_read_rate_mill: None,
                blk_write_rate_mill: None,
                ..container_sample(10_000, "web")
            }])
            .await
            .unwrap();

        let by_service = store
            .query_containers(
                TimeRange {
                    from: 0,
                    to: 10_000,
                },
                MetricsResolution::TenSeconds,
            )
            .await
            .unwrap();
        let metrics = &by_service["web"];

        assert_eq!(metrics.containers["abc123"][0].net_rx_rate, None);
        assert_eq!(metrics.sum[0].net_rx_rate, None);

        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn rejects_duplicate_host_bucket_timestamps() {
        let directory = test_directory("host-duplicate-ts");
        let store = test_store(directory.join("metrics.db")).await;

        store.insert_host(host_sample(10_000)).await.unwrap();
        assert!(store.insert_host(host_sample(10_000)).await.is_err());

        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn creates_parent_directory_and_accepts_empty_batch() {
        let directory = test_directory("empty-batch");
        let database_path = directory.join("nested/metrics.db");
        let store = test_store(&database_path).await;

        store.insert_containers(Vec::new()).await.unwrap();
        store.insert_host(host_sample(10_000)).await.unwrap();

        assert!(database_path.exists());
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn reports_size_including_the_write_ahead_log() {
        let directory = test_directory("size");
        let store = test_store(directory.join("metrics.db")).await;

        store.insert_host(host_sample(10_000)).await.unwrap();

        assert!(store.database_size_bytes().await.unwrap() > 0);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn deleting_before_a_cutoff_reclaims_pages() {
        let directory = test_directory("retention");
        let store = test_store(directory.join("metrics.db")).await;

        store.insert_host(host_sample(10_000)).await.unwrap();
        store
            .insert_containers(vec![container_sample(10_000, "web")])
            .await
            .unwrap();

        assert_eq!(store.delete_before(20_000).await.unwrap(), 2);
        assert_eq!(store.delete_before(20_000).await.unwrap(), 0);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    /// One hour of 10s buckets, plus one more sample so the last hour closes.
    async fn insert_an_hour(store: &SqliteMetricsStore) {
        for bucket in 1..=361 {
            store
                .insert_host(host_sample(bucket * 10_000))
                .await
                .unwrap();
        }
    }

    async fn count(store: &SqliteMetricsStore, table: &str) -> i64 {
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(&store.pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn rolls_closed_buckets_up_into_every_coarse_table() {
        let directory = test_directory("rollup");
        let store = test_store(directory.join("metrics.db")).await;

        insert_an_hour(&store).await;

        assert_eq!(count(&store, "host_metrics_1m").await, 60);
        assert_eq!(count(&store, "host_metrics_5m").await, 12);
        assert_eq!(count(&store, "host_metrics_1h").await, 1);

        let hours = store
            .query_host(
                TimeRange {
                    from: 0,
                    to: 3_600_000,
                },
                MetricsResolution::OneHour,
            )
            .await
            .unwrap();
        assert_eq!(hours.len(), 1);
        assert_eq!(hours[0].ts, 3_600_000);
        assert_eq!(hours[0].cpu_pct, 12.5);
        assert_eq!(hours[0].net_rx_rate, Some(300.0));

        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn the_open_coarse_bucket_is_not_stored() {
        let directory = test_directory("rollup-open");
        let store = test_store(directory.join("metrics.db")).await;

        // Halfway through the first minute.
        for bucket in 1..=3 {
            store
                .insert_host(host_sample(bucket * 10_000))
                .await
                .unwrap();
        }

        assert_eq!(count(&store, "host_metrics_1m").await, 0);

        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn resuming_against_an_existing_database_rolls_up_the_straddled_bucket() {
        let directory = test_directory("rollup-restart");
        let database_path = directory.join("metrics.db");

        let store = test_store(&database_path).await;
        for bucket in 1..=6 {
            store
                .insert_host(host_sample(bucket * 10_000))
                .await
                .unwrap();
        }
        assert_eq!(count(&store, "host_metrics_1m").await, 0);
        store.close().await;

        let store = test_store(&database_path).await;
        store.insert_host(host_sample(70_000)).await.unwrap();

        assert_eq!(count(&store, "host_metrics_1m").await, 1);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn replaying_a_bucket_overwrites_rather_than_duplicating_it() {
        let directory = test_directory("rollup-replay");
        let store = test_store(directory.join("metrics.db")).await;

        insert_an_hour(&store).await;
        rollup::roll_up_host(&store.pool, 3_600_000, 3_610_000, 40)
            .await
            .unwrap();

        assert_eq!(count(&store, "host_metrics_1h").await, 1);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn a_backwards_clock_step_closes_no_bucket() {
        let directory = test_directory("rollup-backwards");
        let store = test_store(directory.join("metrics.db")).await;

        store.insert_host(host_sample(120_000)).await.unwrap();
        store.insert_host(host_sample(50_000)).await.unwrap();

        assert_eq!(count(&store, "host_metrics_1m").await, 0);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn coarse_queries_fall_back_to_raw_rows_predating_the_rollup() {
        let directory = test_directory("rollup-fallback");
        let store = test_store(directory.join("metrics.db")).await;

        // Written straight to the 10s table, as an upgraded database would already hold.
        for bucket in 1..=6 {
            schema::insert_host(
                &store.pool,
                MetricsResolution::TenSeconds,
                host_sample(bucket * 10_000),
            )
            .await
            .unwrap();
        }

        let minutes = store
            .query_host(
                TimeRange {
                    from: 0,
                    to: 60_000,
                },
                MetricsResolution::OneMinute,
            )
            .await
            .unwrap();

        assert_eq!(minutes.len(), 1);
        assert_eq!(minutes[0].ts, 60_000);
        assert_eq!(minutes[0].cpu_pct, 12.5);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn retention_expires_every_resolution() {
        let directory = test_directory("retention-rollups");
        let store = test_store(directory.join("metrics.db")).await;

        insert_an_hour(&store).await;

        // 360 of the 361 raw buckets, plus all 60 + 12 + 1 rolled-up ones.
        assert_eq!(store.delete_before(3_610_000).await.unwrap(), 433);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }
}
