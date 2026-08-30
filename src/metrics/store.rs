use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};

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
}

/// SQLite-backed implementation.
pub struct SqliteMetricsStore {
    db_path: PathBuf,
}

impl SqliteMetricsStore {
    pub fn new(db_path: impl AsRef<Path>) -> Self {
        Self {
            db_path: db_path.as_ref().to_path_buf(),
        }
    }

    async fn pool(&self) -> AppResult<SqlitePool> {
        if let Some(parent) = self.db_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| AppError::Storage(err.to_string()))?;
        }

        let connect_options = SqliteConnectOptions::new()
            .filename(&self.db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .map_err(|err| AppError::Storage(err.to_string()))?;

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
        .execute(&pool)
        .await
        .map_err(|err| AppError::Storage(err.to_string()))?;

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
        .execute(&pool)
        .await
        .map_err(|err| AppError::Storage(err.to_string()))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_host_ts ON host_metrics(ts)")
            .execute(&pool)
            .await
            .map_err(|err| AppError::Storage(err.to_string()))?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ctr_ts ON container_metrics(log_group, ts)")
            .execute(&pool)
            .await
            .map_err(|err| AppError::Storage(err.to_string()))?;

        Ok(pool)
    }
}

#[async_trait]
impl MetricsStore for SqliteMetricsStore {
    async fn database_size_bytes(&self) -> AppResult<u64> {
        match tokio::fs::metadata(&self.db_path).await {
            Ok(metadata) => Ok(metadata.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(AppError::Storage(error.to_string())),
        }
    }

    async fn delete_before(&self, cutoff_ms: i64) -> AppResult<u64> {
        if !self.db_path.exists() {
            return Ok(0);
        }

        let pool = self.pool().await?;
        let mut transaction = pool
            .begin()
            .await
            .map_err(|err| AppError::Storage(err.to_string()))?;
        let host = sqlx::query("DELETE FROM host_metrics WHERE ts < ?")
            .bind(cutoff_ms)
            .execute(&mut *transaction)
            .await
            .map_err(|err| AppError::Storage(err.to_string()))?
            .rows_affected();
        let containers = sqlx::query("DELETE FROM container_metrics WHERE ts < ?")
            .bind(cutoff_ms)
            .execute(&mut *transaction)
            .await
            .map_err(|err| AppError::Storage(err.to_string()))?
            .rows_affected();
        transaction
            .commit()
            .await
            .map_err(|err| AppError::Storage(err.to_string()))?;
        Ok(host + containers)
    }

    async fn insert_host(&self, sample: HostSample) -> AppResult<()> {
        let pool = self.pool().await?;
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
        .execute(&pool)
        .await
        .map_err(|err| AppError::Storage(err.to_string()))?;
        Ok(())
    }

    async fn insert_containers(&self, samples: Vec<ContainerSample>) -> AppResult<()> {
        if samples.is_empty() {
            return Ok(());
        }

        let pool = self.pool().await?;
        let mut transaction = pool
            .begin()
            .await
            .map_err(|err| AppError::Storage(err.to_string()))?;
        for sample in samples {
            sqlx::query(
                "INSERT INTO container_metrics
                    (ts, log_group, cid, cpu_pct, mem_used, mem_limit, net_rx, net_tx, blk_read, blk_write)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(sample.ts)
            .bind(sample.log_group)
            .bind(sample.cid)
            .bind(sample.cpu_pct)
            .bind(sample.mem_used as i64)
            .bind(sample.mem_limit as i64)
            .bind(sample.net_rx as i64)
            .bind(sample.net_tx as i64)
            .bind(sample.blk_read as i64)
            .bind(sample.blk_write as i64)
            .execute(&mut *transaction)
            .await
            .map_err(|err| AppError::Storage(err.to_string()))?;
        }
        transaction
            .commit()
            .await
            .map_err(|err| AppError::Storage(err.to_string()))?;
        Ok(())
    }

    async fn query_host(
        &self,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<Vec<HostPoint>> {
        let pool = self.pool().await?;
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
        .fetch_all(&pool)
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
        let pool = self.pool().await?;
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
        .fetch_all(&pool)
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
        let pool = self.pool().await?;
        // Reach one sample before `from` so the first in-range bucket has a rate.
        let rows = sqlx::query(
            "SELECT ts, log_group, cid, cpu_pct, mem_used, mem_limit, net_rx, net_tx, blk_read, blk_write
             FROM container_metrics
             WHERE ts >= COALESCE((SELECT MAX(ts) FROM container_metrics WHERE ts < ?), ?) AND ts <= ?
             ORDER BY log_group ASC, ts ASC",
        )
        .bind(range.from)
        .bind(range.from)
        .bind(range.to)
        .fetch_all(&pool)
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
        let store = SqliteMetricsStore::new(directory.join("metrics.db"));

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
        let store = SqliteMetricsStore::new(&database_path);

        store.insert_containers(Vec::new()).await.unwrap();
        store.insert_host(host_sample(1)).await.unwrap();

        assert!(database_path.exists());
        let _ = tokio::fs::remove_dir_all(directory).await;
    }
}
