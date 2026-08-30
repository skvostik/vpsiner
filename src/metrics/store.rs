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
    ContainerGroupMetrics, ContainerMetricsByLogGroup, ContainerPoint, ContainerSample, HostPoint,
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
        log_group: &str,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<ContainerGroupMetrics>;

    async fn query_containers(
        &self,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<ContainerMetricsByLogGroup>;

    /// Checkpoints the write-ahead log and releases the connection.
    async fn close(&self);
}

/// Only a handful of distinct statements are ever prepared against this database.
const STATEMENT_CACHE_CAPACITY: usize = 32;
/// Recommended by SQLite for `PRAGMA optimize`.
const ANALYSIS_LIMIT: u32 = 400;
/// Value reported by `PRAGMA auto_vacuum` when incremental vacuuming is active.
const AUTO_VACUUM_INCREMENTAL: i64 = 2;
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
        store.apply_incremental_vacuum().await?;
        Ok(store)
    }

    async fn create_schema(&self) -> AppResult<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS host_metrics (
                ts INTEGER NOT NULL,
                cpu_pct REAL NOT NULL,
                mem_used INTEGER NOT NULL,
                mem_total INTEGER NOT NULL,
                storage_used INTEGER NOT NULL DEFAULT 0,
                storage_total INTEGER NOT NULL DEFAULT 0,
                metrics_size INTEGER NOT NULL,
                logs_size INTEGER NOT NULL,
                net_rx INTEGER NOT NULL,
                net_tx INTEGER NOT NULL,
                disk_read INTEGER NOT NULL,
                disk_write INTEGER NOT NULL
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(storage)?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS container_metrics (
                ts INTEGER NOT NULL,
                log_group TEXT NOT NULL,
                cid TEXT NOT NULL,
                cpu_pct REAL NOT NULL,
                mem_used INTEGER NOT NULL,
                mem_limit INTEGER NOT NULL,
                net_rx INTEGER NOT NULL,
                net_tx INTEGER NOT NULL,
                blk_read INTEGER NOT NULL,
                blk_write INTEGER NOT NULL
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(storage)?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_host_ts ON host_metrics(ts)")
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ctr_ts ON container_metrics(log_group, ts)")
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        // Serves retention deletes and range queries that span every log group.
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ctr_ts_only ON container_metrics(ts)")
            .execute(&self.pool)
            .await
            .map_err(storage)?;

        Ok(())
    }

    /// Databases created before incremental auto-vacuum was enabled only pick the
    /// setting up through a full `VACUUM`.
    async fn apply_incremental_vacuum(&self) -> AppResult<()> {
        let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
            .fetch_one(&self.pool)
            .await
            .map_err(storage)?;
        if mode == AUTO_VACUUM_INCREMENTAL {
            return Ok(());
        }

        tracing::info!("migrating metrics database to incremental auto-vacuum");
        sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        sqlx::query("VACUUM")
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

#[async_trait]
impl MetricsStore for SqliteMetricsStore {
    async fn database_size_bytes(&self) -> AppResult<u64> {
        // Pages committed but not yet checkpointed still live in the write-ahead log.
        Ok(file_size_bytes(&self.db_path).await? + file_size_bytes(&self.wal_path()).await?)
    }

    async fn delete_before(&self, cutoff_ms: i64) -> AppResult<u64> {
        let mut transaction = self.pool.begin().await.map_err(storage)?;
        let host = sqlx::query("DELETE FROM host_metrics WHERE ts < ?")
            .bind(cutoff_ms)
            .execute(&mut *transaction)
            .await
            .map_err(storage)?
            .rows_affected();
        let containers = sqlx::query("DELETE FROM container_metrics WHERE ts < ?")
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
            "INSERT INTO host_metrics
                     (ts, cpu_pct, mem_used, mem_total, storage_used, storage_total, metrics_size, logs_size, net_rx, net_tx, disk_read, disk_write)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(sample.ts)
        .bind(sample.cpu_pct)
        .bind(sample.mem_used as i64)
        .bind(sample.mem_total as i64)
        .bind(sample.storage_used as i64)
        .bind(sample.storage_total as i64)
        .bind(sample.metrics_size as i64)
        .bind(sample.logs_size as i64)
        .bind(sample.net_rx as i64)
        .bind(sample.net_tx as i64)
        .bind(sample.disk_read as i64)
        .bind(sample.disk_write as i64)
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
                "INSERT INTO container_metrics
                    (ts, log_group, cid, cpu_pct, mem_used, mem_limit, net_rx, net_tx, blk_read, blk_write) ",
            );
            builder.push_values(chunk, |mut row, sample| {
                row.push_bind(sample.ts)
                    .push_bind(sample.log_group.clone())
                    .push_bind(sample.cid.clone())
                    .push_bind(sample.cpu_pct)
                    .push_bind(sample.mem_used as i64)
                    .push_bind(sample.mem_limit as i64)
                    .push_bind(sample.net_rx as i64)
                    .push_bind(sample.net_tx as i64)
                    .push_bind(sample.blk_read as i64)
                    .push_bind(sample.blk_write as i64);
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
        // Reach one sample before `from` so the first in-range bucket has a rate.
        let rows = sqlx::query(
            "SELECT ts, cpu_pct, mem_used, mem_total, storage_used, storage_total, metrics_size, logs_size, net_rx, net_tx, disk_read, disk_write
             FROM host_metrics
             WHERE ts >= COALESCE((SELECT MAX(ts) FROM host_metrics WHERE ts < ?), ?) AND ts <= ?
             ORDER BY ts ASC",
        )
        .bind(range.from)
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
                    cpu_pct: row
                        .try_get("cpu_pct")
                        .map_err(|err| AppError::Storage(err.to_string()))?,
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
                    net_rx: row
                        .try_get::<i64, _>("net_rx")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    net_tx: row
                        .try_get::<i64, _>("net_tx")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    disk_read: row
                        .try_get::<i64, _>("disk_read")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    disk_write: row
                        .try_get::<i64, _>("disk_write")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                })
            })
            .collect();

        let mut points = downsample_host(samples?, resolution);
        points.retain(|point| point.ts >= range.from);
        Ok(points)
    }

    async fn query_container(
        &self,
        log_group: &str,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<ContainerGroupMetrics> {
        // Reach one sample before `from` so the first in-range bucket has a rate.
        let rows = sqlx::query(
            "SELECT ts, log_group, cid, cpu_pct, mem_used, mem_limit, net_rx, net_tx, blk_read, blk_write
             FROM container_metrics
             WHERE log_group = ?
               AND ts >= COALESCE((SELECT MAX(ts) FROM container_metrics WHERE log_group = ? AND ts < ?), ?)
               AND ts <= ?
             ORDER BY ts ASC",
        )
        .bind(log_group)
        .bind(log_group)
        .bind(range.from)
        .bind(range.from)
        .bind(range.to)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| AppError::Storage(err.to_string()))?;

        let raw_samples: Vec<ContainerSample> = rows
            .into_iter()
            .map(|row| {
                Ok(ContainerSample {
                    ts: row
                        .try_get("ts")
                        .map_err(|err| AppError::Storage(err.to_string()))?,
                    log_group: row
                        .try_get("log_group")
                        .map_err(|err| AppError::Storage(err.to_string()))?,
                    cid: row
                        .try_get("cid")
                        .map_err(|err| AppError::Storage(err.to_string()))?,
                    cpu_pct: row
                        .try_get("cpu_pct")
                        .map_err(|err| AppError::Storage(err.to_string()))?,
                    mem_used: row
                        .try_get::<i64, _>("mem_used")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    mem_limit: row
                        .try_get::<i64, _>("mem_limit")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    net_rx: row
                        .try_get::<i64, _>("net_rx")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    net_tx: row
                        .try_get::<i64, _>("net_tx")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    blk_read: row
                        .try_get::<i64, _>("blk_read")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    blk_write: row
                        .try_get::<i64, _>("blk_write")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                })
            })
            .collect::<AppResult<Vec<_>>>()?;

        let mut by_container: HashMap<String, Vec<ContainerSample>> = HashMap::new();
        for sample in raw_samples {
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
    ) -> AppResult<ContainerMetricsByLogGroup> {
        // Reach one sample before `from` so the first in-range bucket has a rate.
        let rows = sqlx::query(
            "SELECT ts, log_group, cid, cpu_pct, mem_used, mem_limit, net_rx, net_tx, blk_read, blk_write
             FROM container_metrics
             WHERE ts >= COALESCE((SELECT MAX(ts) FROM container_metrics WHERE ts < ?), ?) AND ts <= ?
             ORDER BY ts ASC",
        )
        .bind(range.from)
        .bind(range.from)
        .bind(range.to)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| AppError::Storage(err.to_string()))?;

        let raw_samples: Vec<ContainerSample> = rows
            .into_iter()
            .map(|row| {
                Ok(ContainerSample {
                    ts: row
                        .try_get("ts")
                        .map_err(|err| AppError::Storage(err.to_string()))?,
                    log_group: row
                        .try_get("log_group")
                        .map_err(|err| AppError::Storage(err.to_string()))?,
                    cid: row
                        .try_get("cid")
                        .map_err(|err| AppError::Storage(err.to_string()))?,
                    cpu_pct: row
                        .try_get("cpu_pct")
                        .map_err(|err| AppError::Storage(err.to_string()))?,
                    mem_used: row
                        .try_get::<i64, _>("mem_used")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    mem_limit: row
                        .try_get::<i64, _>("mem_limit")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    net_rx: row
                        .try_get::<i64, _>("net_rx")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    net_tx: row
                        .try_get::<i64, _>("net_tx")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    blk_read: row
                        .try_get::<i64, _>("blk_read")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                    blk_write: row
                        .try_get::<i64, _>("blk_write")
                        .map_err(|err| AppError::Storage(err.to_string()))?
                        as u64,
                })
            })
            .collect::<AppResult<Vec<_>>>()?;

        let mut by_group_and_container: HashMap<String, HashMap<String, Vec<ContainerSample>>> =
            HashMap::new();
        for sample in raw_samples {
            by_group_and_container
                .entry(sample.log_group.clone())
                .or_default()
                .entry(sample.cid.clone())
                .or_default()
                .push(sample);
        }

        let mut by_group: ContainerMetricsByLogGroup = HashMap::new();
        for (log_group, by_container) in by_group_and_container {
            let containers: Vec<Vec<ContainerPoint>> = by_container
                .into_values()
                .map(|samples| {
                    let mut points = downsample_container(samples, resolution);
                    points.retain(|point| point.ts >= range.from);
                    points
                })
                .collect();
            by_group.insert(log_group, sum_by_bucket(containers.iter()));
        }

        Ok(by_group)
    }

    async fn close(&self) {
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
            cpu_pct: 12.5,
            mem_used: 100,
            mem_total: 200,
            storage_used: 700,
            storage_total: 800,
            metrics_size: 900,
            logs_size: 1_000,
            net_rx: 300,
            net_tx: 400,
            disk_read: 500,
            disk_write: 600,
        }
    }

    fn container_sample(ts: i64, log_group: &str) -> ContainerSample {
        ContainerSample {
            ts,
            log_group: log_group.into(),
            cid: "abc123".into(),
            cpu_pct: 25.0,
            mem_used: 1_000,
            mem_limit: 2_000,
            net_rx: 3_000,
            net_tx: 4_000,
            blk_read: 5_000,
            blk_write: 6_000,
        }
    }

    #[tokio::test]
    async fn persists_and_queries_samples_by_range() {
        let directory = test_directory("range");
        let store = test_store(directory.join("metrics.db")).await;

        store.insert_host(host_sample(100)).await.unwrap();
        store.insert_host(host_sample(200)).await.unwrap();
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
                TimeRange { from: 150, to: 250 },
                MetricsResolution::TenSeconds,
            )
            .await
            .unwrap();
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0].ts, 10_000);
        assert_eq!(hosts[0].cpu_pct, 12.5);
        assert_eq!(hosts[0].mem_used, 100);
        assert_eq!(hosts[0].net_rx_rate, 0.0);

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
        assert_eq!(containers.containers["abc123"][0].log_group, "web");
        assert_eq!(containers.containers["abc123"][0].mem_used, 1_000);

        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn creates_parent_directory_and_accepts_empty_batch() {
        let directory = test_directory("empty-batch");
        let database_path = directory.join("nested/metrics.db");
        let store = test_store(&database_path).await;

        store.insert_containers(Vec::new()).await.unwrap();
        store.insert_host(host_sample(1)).await.unwrap();

        assert!(database_path.exists());
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn reports_size_including_the_write_ahead_log() {
        let directory = test_directory("size");
        let store = test_store(directory.join("metrics.db")).await;

        store.insert_host(host_sample(1)).await.unwrap();

        assert!(store.database_size_bytes().await.unwrap() > 0);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn deleting_before_a_cutoff_reclaims_pages() {
        let directory = test_directory("retention");
        let store = test_store(directory.join("metrics.db")).await;

        store.insert_host(host_sample(100)).await.unwrap();
        store
            .insert_containers(vec![container_sample(100, "web")])
            .await
            .unwrap();

        assert_eq!(store.delete_before(200).await.unwrap(), 2);
        assert_eq!(store.delete_before(200).await.unwrap(), 0);
        let _ = tokio::fs::remove_dir_all(directory).await;
    }
}
