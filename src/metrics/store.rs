use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::{QueryBuilder, Row, SqlitePool};

use crate::error::{AppError, AppResult};
use crate::metrics::downsampling::{downsample_container, downsample_host, sum_by_bucket};
use crate::model::{
    ContainerGroupMetrics, ContainerMetricsByService, ContainerPoint, ContainerSample, HostPoint,
    HostSample, MetricsResolution, TimeRange,
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

    async fn query_container(
        &self,
        service: &str,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<ContainerGroupMetrics>;

    async fn query_containers(
        &self,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<ContainerMetricsByService>;

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
/// Bound on rows per multi-row insert; SQLite allows 32766 bound parameters.
const INSERT_CHUNK_ROWS: usize = 3_000;

/// SQLite-backed implementation.
pub struct SqliteMetricsStore {
    db_path: PathBuf,
    pool: SqlitePool,
}

impl SqliteMetricsStore {
    /// Opens the single long-lived connection used for the lifetime of the process.
    pub async fn connect(
        db_path: impl AsRef<Path>,
        cache_size_kb: u64,
        busy_timeout: Duration,
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

        let store = Self { db_path, pool };
        store.create_schema().await?;
        Ok(store)
    }

    async fn create_schema(&self) -> AppResult<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS host_metrics_10s (
                ts INTEGER PRIMARY KEY,
                cpu_pct_mill INTEGER NOT NULL,
                mem_used INTEGER NOT NULL,
                mem_total INTEGER NOT NULL,
                storage_used INTEGER NOT NULL,
                storage_total INTEGER NOT NULL,
                metrics_size INTEGER NOT NULL,
                logs_size INTEGER NOT NULL,
                net_rx_rate_mill INTEGER,
                net_tx_rate_mill INTEGER,
                disk_read_rate_mill INTEGER,
                disk_write_rate_mill INTEGER
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(storage)?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS container_metrics_10s (
                ts INTEGER NOT NULL,
                cid TEXT NOT NULL,
                service TEXT NOT NULL,
                cpu_pct_mill INTEGER NOT NULL,
                mem_used INTEGER NOT NULL,
                mem_limit INTEGER NOT NULL,
                net_rx_rate_mill INTEGER,
                net_tx_rate_mill INTEGER,
                blk_read_rate_mill INTEGER,
                blk_write_rate_mill INTEGER,
                PRIMARY KEY (ts, cid)
            ) WITHOUT ROWID",
        )
        .execute(&self.pool)
        .await
        .map_err(storage)?;

        // Used by per-service container metrics queries: filter by service, then walk the
        // matching samples in timestamp order for a time range. Queries without a service
        // filter, and retention deletes, use the timestamp-ordered primary key instead.
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_ctr_service_ts ON container_metrics_10s(service, ts)",
        )
        .execute(&self.pool)
        .await
        .map_err(storage)?;

        Ok(())
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

async fn file_size_bytes(path: &Path) -> AppResult<u64> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(storage(error)),
    }
}

fn container_sample_from_row(row: sqlx::sqlite::SqliteRow) -> AppResult<ContainerSample> {
    fn count(row: &sqlx::sqlite::SqliteRow, column: &str) -> AppResult<u64> {
        Ok(row.try_get::<i64, _>(column).map_err(storage)? as u64)
    }

    fn rate(row: &sqlx::sqlite::SqliteRow, column: &str) -> AppResult<Option<u64>> {
        Ok(row
            .try_get::<Option<i64>, _>(column)
            .map_err(storage)?
            .map(|rate| rate as u64))
    }

    Ok(ContainerSample {
        ts: row.try_get("ts").map_err(storage)?,
        cid: row.try_get("cid").map_err(storage)?,
        service: row.try_get("service").map_err(storage)?,
        cpu_pct_mill: count(&row, "cpu_pct_mill")?,
        mem_used: count(&row, "mem_used")?,
        mem_limit: count(&row, "mem_limit")?,
        net_rx_rate_mill: rate(&row, "net_rx_rate_mill")?,
        net_tx_rate_mill: rate(&row, "net_tx_rate_mill")?,
        blk_read_rate_mill: rate(&row, "blk_read_rate_mill")?,
        blk_write_rate_mill: rate(&row, "blk_write_rate_mill")?,
    })
}

#[async_trait]
impl MetricsStore for SqliteMetricsStore {
    async fn database_size_bytes(&self) -> AppResult<u64> {
        // Pages committed but not yet checkpointed still live in the write-ahead log.
        Ok(file_size_bytes(&self.db_path).await? + file_size_bytes(&self.wal_path()).await?)
    }

    async fn delete_before(&self, cutoff_ms: i64) -> AppResult<u64> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        let host = sqlx::query("DELETE FROM host_metrics_10s WHERE ts < ?")
            .bind(cutoff_ms)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?
            .rows_affected();
        let containers = sqlx::query("DELETE FROM container_metrics_10s WHERE ts < ?")
            .bind(cutoff_ms)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?
            .rows_affected();
        transaction.commit().await.map_err(storage)?;

        self.reclaim_free_pages().await;
        Ok(host + containers)
    }

    async fn insert_host(&self, sample: HostSample) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO host_metrics_10s
                     (ts, cpu_pct_mill, mem_used, mem_total, storage_used, storage_total, metrics_size, logs_size, net_rx_rate_mill, net_tx_rate_mill, disk_read_rate_mill, disk_write_rate_mill)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(sample.ts)
        .bind(sample.cpu_pct_mill as i64)
        .bind(sample.mem_used as i64)
        .bind(sample.mem_total as i64)
        .bind(sample.storage_used as i64)
        .bind(sample.storage_total as i64)
        .bind(sample.metrics_size as i64)
        .bind(sample.logs_size as i64)
        .bind(sample.net_rx_rate_mill.map(|rate| rate as i64))
        .bind(sample.net_tx_rate_mill.map(|rate| rate as i64))
        .bind(sample.disk_read_rate_mill.map(|rate| rate as i64))
        .bind(sample.disk_write_rate_mill.map(|rate| rate as i64))
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    async fn insert_containers(&self, samples: Vec<ContainerSample>) -> AppResult<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let mut transaction = self.pool.begin().await.map_err(storage)?;
        for chunk in samples.chunks(INSERT_CHUNK_ROWS) {
            let mut builder = QueryBuilder::new(
                "INSERT INTO container_metrics_10s
                    (ts, cid, service, cpu_pct_mill, mem_used, mem_limit, net_rx_rate_mill, net_tx_rate_mill, blk_read_rate_mill, blk_write_rate_mill) ",
            );
            builder.push_values(chunk, |mut row, sample| {
                row.push_bind(sample.ts)
                    .push_bind(sample.cid.clone())
                    .push_bind(sample.service.clone())
                    .push_bind(sample.cpu_pct_mill as i64)
                    .push_bind(sample.mem_used as i64)
                    .push_bind(sample.mem_limit as i64)
                    .push_bind(sample.net_rx_rate_mill.map(|rate| rate as i64))
                    .push_bind(sample.net_tx_rate_mill.map(|rate| rate as i64))
                    .push_bind(sample.blk_read_rate_mill.map(|rate| rate as i64))
                    .push_bind(sample.blk_write_rate_mill.map(|rate| rate as i64));
            });
            builder
                .build()
                .execute(&mut *transaction)
                .await
                .map_err(storage)?;
        }
        transaction.commit().await.map_err(storage)?;
        Ok(())
    }

    async fn query_host(
        &self,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<Vec<HostPoint>> {
        let rows = sqlx::query(
              "SELECT ts, cpu_pct_mill, mem_used, mem_total, storage_used, storage_total, metrics_size, logs_size, net_rx_rate_mill, net_tx_rate_mill, disk_read_rate_mill, disk_write_rate_mill
               FROM host_metrics_10s
               WHERE ts >= ? AND ts <= ?
             ORDER BY ts ASC",
        )
        .bind(range.from)
        .bind(range.to)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| AppError::Storage(err.to_string()))?;

        let samples: AppResult<Vec<HostSample>> = rows
            .into_iter()
            .map(|row| {
                Ok(HostSample {
                    ts: row
                        .try_get("ts")
                        .map_err(|err| AppError::Storage(err.to_string()))?,
                    cpu_pct_mill: row
                        .try_get::<i64, _>("cpu_pct_mill")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    mem_used: row
                        .try_get::<i64, _>("mem_used")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    mem_total: row
                        .try_get::<i64, _>("mem_total")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    storage_used: row
                        .try_get::<i64, _>("storage_used")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    storage_total: row
                        .try_get::<i64, _>("storage_total")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    metrics_size: row
                        .try_get::<i64, _>("metrics_size")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    logs_size: row
                        .try_get::<i64, _>("logs_size")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    net_rx_rate_mill: row
                        .try_get::<Option<i64>, _>("net_rx_rate_mill")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        .map(|rate| rate as u64),
                    net_tx_rate_mill: row
                        .try_get::<Option<i64>, _>("net_tx_rate_mill")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        .map(|rate| rate as u64),
                    disk_read_rate_mill: row
                        .try_get::<Option<i64>, _>("disk_read_rate_mill")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        .map(|rate| rate as u64),
                    disk_write_rate_mill: row
                        .try_get::<Option<i64>, _>("disk_write_rate_mill")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        .map(|rate| rate as u64),
                })
            })
            .collect();

        let mut points = downsample_host(samples?, resolution);
        points.retain(|point| point.ts >= range.from);
        Ok(points)
    }

    async fn query_container(
        &self,
        service: &str,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<ContainerGroupMetrics> {
        let rows = sqlx::query(
            "SELECT ts, cid, service, cpu_pct_mill, mem_used, mem_limit, net_rx_rate_mill, net_tx_rate_mill, blk_read_rate_mill, blk_write_rate_mill
             FROM container_metrics_10s
             WHERE service = ? AND ts >= ? AND ts <= ?
             ORDER BY ts ASC",
        )
        .bind(service)
        .bind(range.from)
        .bind(range.to)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| AppError::Storage(err.to_string()))?;

        let samples: Vec<ContainerSample> = rows
            .into_iter()
            .map(container_sample_from_row)
            .collect::<AppResult<Vec<_>>>()?;

        let mut by_container: HashMap<String, Vec<ContainerSample>> = HashMap::new();
        for sample in samples {
            by_container
                .entry(sample.cid.clone())
                .or_default()
                .push(sample);
        }

        let mut containers: HashMap<String, Vec<ContainerPoint>> = HashMap::new();
        for (cid, samples) in by_container {
            let mut points = downsample_container(samples, resolution);
            points.retain(|point| point.ts >= range.from);
            containers.insert(cid, points);
        }

        let sum = sum_by_bucket(containers.values());
        Ok(ContainerGroupMetrics { sum, containers })
    }

    async fn query_containers(
        &self,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<ContainerMetricsByService> {
        let rows = sqlx::query(
            "SELECT ts, cid, service, cpu_pct_mill, mem_used, mem_limit, net_rx_rate_mill, net_tx_rate_mill, blk_read_rate_mill, blk_write_rate_mill
             FROM container_metrics_10s
             WHERE ts >= ? AND ts <= ?
             ORDER BY ts ASC",
        )
        .bind(range.from)
        .bind(range.to)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| AppError::Storage(err.to_string()))?;

        let samples: Vec<ContainerSample> = rows
            .into_iter()
            .map(container_sample_from_row)
            .collect::<AppResult<Vec<_>>>()?;

        let mut by_service_and_container: HashMap<String, HashMap<String, Vec<ContainerSample>>> =
            HashMap::new();
        for sample in samples {
            by_service_and_container
                .entry(sample.service.clone())
                .or_default()
                .entry(sample.cid.clone())
                .or_default()
                .push(sample);
        }

        let mut by_service: ContainerMetricsByService = HashMap::new();
        for (service, by_container) in by_service_and_container {
            let containers: Vec<Vec<ContainerPoint>> = by_container
                .into_values()
                .map(|samples| {
                    let mut points = downsample_container(samples, resolution);
                    points.retain(|point| point.ts >= range.from);
                    points
                })
                .collect();
            by_service.insert(service, sum_by_bucket(containers.iter()));
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
        SqliteMetricsStore::connect(db_path, 1_024, Duration::from_secs(5))
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
                container_sample(100, "web"),
                container_sample(200, "worker"),
                container_sample(300, "web"),
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

        let containers = store
            .query_container(
                "web",
                TimeRange { from: 0, to: 250 },
                MetricsResolution::TenSeconds,
            )
            .await
            .unwrap();
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

        let metrics = store
            .query_container(
                "web",
                TimeRange {
                    from: 0,
                    to: 10_000,
                },
                MetricsResolution::TenSeconds,
            )
            .await
            .unwrap();

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
}
