use std::path::{Path, PathBuf};
use std::{collections::HashMap, collections::hash_map::Entry};

use async_trait::async_trait;
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};

use crate::error::{AppError, AppResult};
use crate::model::{
    ContainerGroupMetrics, ContainerGroupSample, ContainerMetricsByLogGroup, ContainerSample,
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
    ) -> AppResult<Vec<HostSample>>;

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

/// The end of the half-open `(bucket_end - bucket_ms, bucket_end]` window containing `ts`.
fn bucket_end(ts: i64, bucket_ms: i64) -> i64 {
    -(-ts).div_euclid(bucket_ms) * bucket_ms
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn downsample_host(samples: Vec<HostSample>, resolution: MetricsResolution) -> Vec<HostSample> {
    let bucket_ms = resolution.bucket_ms();
    let mut buckets: Vec<HostSample> = Vec::new();
    let mut current_bucket = i64::MIN;
    let mut count = 0_u64;
    let mut cpu_sum = 0.0_f64;
    let mut mem_used_sum = 0_u128;
    let mut mem_total_sum = 0_u128;
    let mut storage_used_sum = 0_u128;
    let mut storage_total_sum = 0_u128;
    let mut metrics_size_sum = 0_u128;
    let mut logs_size_sum = 0_u128;
    let mut last_counter = (0_u64, 0_u64, 0_u64, 0_u64);

    let flush = |bucket: i64,
                 count: u64,
                 cpu_sum: f64,
                 mem_used_sum: u128,
                 mem_total_sum: u128,
                 storage_used_sum: u128,
                 storage_total_sum: u128,
                 metrics_size_sum: u128,
                 logs_size_sum: u128,
                 last_counter: (u64, u64, u64, u64),
                 buckets: &mut Vec<HostSample>| {
        if count == 0 {
            return;
        }
        buckets.push(HostSample {
            ts: bucket,
            cpu_pct: cpu_sum / count as f64,
            mem_used: (mem_used_sum / count as u128) as u64,
            mem_total: (mem_total_sum / count as u128) as u64,
            storage_used: (storage_used_sum / count as u128) as u64,
            storage_total: (storage_total_sum / count as u128) as u64,
            metrics_size: (metrics_size_sum / count as u128) as u64,
            logs_size: (logs_size_sum / count as u128) as u64,
            net_rx: last_counter.0,
            net_tx: last_counter.1,
            disk_read: last_counter.2,
            disk_write: last_counter.3,
        });
    };

    for sample in samples {
        let bucket = bucket_end(sample.ts, bucket_ms);
        if bucket != current_bucket {
            flush(
                current_bucket,
                count,
                cpu_sum,
                mem_used_sum,
                mem_total_sum,
                storage_used_sum,
                storage_total_sum,
                metrics_size_sum,
                logs_size_sum,
                last_counter,
                &mut buckets,
            );
            current_bucket = bucket;
            count = 0;
            cpu_sum = 0.0;
            mem_used_sum = 0;
            mem_total_sum = 0;
            storage_used_sum = 0;
            storage_total_sum = 0;
            metrics_size_sum = 0;
            logs_size_sum = 0;
        }

        count += 1;
        cpu_sum += sample.cpu_pct;
        mem_used_sum += sample.mem_used as u128;
        mem_total_sum += sample.mem_total as u128;
        storage_used_sum += sample.storage_used as u128;
        storage_total_sum += sample.storage_total as u128;
        metrics_size_sum += sample.metrics_size as u128;
        logs_size_sum += sample.logs_size as u128;
        last_counter = (
            sample.net_rx,
            sample.net_tx,
            sample.disk_read,
            sample.disk_write,
        );
    }

    flush(
        current_bucket,
        count,
        cpu_sum,
        mem_used_sum,
        mem_total_sum,
        storage_used_sum,
        storage_total_sum,
        metrics_size_sum,
        logs_size_sum,
        last_counter,
        &mut buckets,
    );
    // ts is the bucket's closing instant, so a bucket has fully elapsed exactly when ts <= now.
    buckets.retain(|sample| sample.ts <= now_ms());
    buckets
}

fn downsample_container(
    samples: Vec<ContainerSample>,
    resolution: MetricsResolution,
) -> Vec<ContainerSample> {
    let bucket_ms = resolution.bucket_ms();
    let mut buckets: Vec<ContainerSample> = Vec::new();
    let mut current_bucket = i64::MIN;
    let mut count = 0_u64;
    let mut cpu_sum = 0.0_f64;
    let mut mem_used_sum = 0_u128;
    let mut mem_limit_sum = 0_u128;
    let mut last_counter = (0_u64, 0_u64, 0_u64, 0_u64);
    let mut cid = String::new();
    let mut log_group = String::new();

    let flush = |bucket: i64,
                 count: u64,
                 cpu_sum: f64,
                 mem_used_sum: u128,
                 mem_limit_sum: u128,
                 last_counter: (u64, u64, u64, u64),
                 cid: &str,
                 log_group: &str,
                 buckets: &mut Vec<ContainerSample>| {
        if count == 0 {
            return;
        }
        buckets.push(ContainerSample {
            ts: bucket,
            log_group: log_group.to_string(),
            cid: cid.to_string(),
            cpu_pct: cpu_sum / count as f64,
            mem_used: (mem_used_sum / count as u128) as u64,
            mem_limit: (mem_limit_sum / count as u128) as u64,
            net_rx: last_counter.0,
            net_tx: last_counter.1,
            blk_read: last_counter.2,
            blk_write: last_counter.3,
        });
    };

    for sample in samples {
        let bucket = bucket_end(sample.ts, bucket_ms);
        if bucket != current_bucket {
            flush(
                current_bucket,
                count,
                cpu_sum,
                mem_used_sum,
                mem_limit_sum,
                last_counter,
                &cid,
                &log_group,
                &mut buckets,
            );
            current_bucket = bucket;
            count = 0;
            cpu_sum = 0.0;
            mem_used_sum = 0;
            mem_limit_sum = 0;
        }

        if count == 0 {
            cid = sample.cid.clone();
            log_group = sample.log_group.clone();
        }
        count += 1;
        cpu_sum += sample.cpu_pct;
        mem_used_sum += sample.mem_used as u128;
        mem_limit_sum += sample.mem_limit as u128;
        last_counter = (
            sample.net_rx,
            sample.net_tx,
            sample.blk_read,
            sample.blk_write,
        );
    }

    flush(
        current_bucket,
        count,
        cpu_sum,
        mem_used_sum,
        mem_limit_sum,
        last_counter,
        &cid,
        &log_group,
        &mut buckets,
    );
    // ts is the bucket's closing instant, so a bucket has fully elapsed exactly when ts <= now.
    buckets.retain(|sample| sample.ts <= now_ms());
    buckets
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
    ) -> AppResult<Vec<HostSample>> {
        let pool = self.pool().await?;
        let rows = sqlx::query(
            "SELECT ts, cpu_pct, mem_used, mem_total, storage_used, storage_total, metrics_size, logs_size, net_rx, net_tx, disk_read, disk_write
             FROM host_metrics WHERE ts >= ? AND ts <= ? ORDER BY ts ASC",
        )
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

        Ok(downsample_host(samples?, resolution))
    }

    async fn query_container(
        &self,
        log_group: &str,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<ContainerGroupMetrics> {
        let pool = self.pool().await?;
        let rows = sqlx::query(
            "SELECT ts, log_group, cid, cpu_pct, mem_used, mem_limit, net_rx, net_tx, blk_read, blk_write
             FROM container_metrics
             WHERE log_group = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC",
        )
        .bind(log_group)
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

        let mut containers: HashMap<String, Vec<ContainerSample>> = HashMap::new();
        for (cid, samples) in by_container {
            containers.insert(cid, downsample_container(samples, resolution));
        }

        let mut sum_by_ts: HashMap<i64, ContainerGroupSample> = HashMap::new();
        for samples in containers.values() {
            for sample in samples {
                match sum_by_ts.entry(sample.ts) {
                    Entry::Occupied(mut entry) => {
                        let acc = entry.get_mut();
                        acc.cpu_pct += sample.cpu_pct;
                        acc.mem_used = acc.mem_used.saturating_add(sample.mem_used);
                        acc.mem_limit = acc.mem_limit.saturating_add(sample.mem_limit);
                        acc.net_rx = acc.net_rx.saturating_add(sample.net_rx);
                        acc.net_tx = acc.net_tx.saturating_add(sample.net_tx);
                        acc.blk_read = acc.blk_read.saturating_add(sample.blk_read);
                        acc.blk_write = acc.blk_write.saturating_add(sample.blk_write);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(ContainerGroupSample {
                            ts: sample.ts,
                            log_group: sample.log_group.clone(),
                            cpu_pct: sample.cpu_pct,
                            mem_used: sample.mem_used,
                            mem_limit: sample.mem_limit,
                            net_rx: sample.net_rx,
                            net_tx: sample.net_tx,
                            blk_read: sample.blk_read,
                            blk_write: sample.blk_write,
                        });
                    }
                }
            }
        }

        let mut sum: Vec<ContainerGroupSample> = sum_by_ts.into_values().collect();
        sum.sort_by_key(|sample| sample.ts);
        Ok(ContainerGroupMetrics { sum, containers })
    }

    async fn query_containers(
        &self,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<ContainerMetricsByLogGroup> {
        let pool = self.pool().await?;
        let rows = sqlx::query(
            "SELECT ts, log_group, cid, cpu_pct, mem_used, mem_limit, net_rx, net_tx, blk_read, blk_write
             FROM container_metrics
             WHERE ts >= ? AND ts <= ? ORDER BY log_group ASC, ts ASC",
        )
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
            let mut sum_by_ts: HashMap<i64, ContainerGroupSample> = HashMap::new();
            for samples in by_container.into_values() {
                for sample in downsample_container(samples, resolution) {
                    match sum_by_ts.entry(sample.ts) {
                        Entry::Occupied(mut entry) => {
                            let acc = entry.get_mut();
                            acc.cpu_pct += sample.cpu_pct;
                            acc.mem_used = acc.mem_used.saturating_add(sample.mem_used);
                            acc.mem_limit = acc.mem_limit.saturating_add(sample.mem_limit);
                            acc.net_rx = acc.net_rx.saturating_add(sample.net_rx);
                            acc.net_tx = acc.net_tx.saturating_add(sample.net_tx);
                            acc.blk_read = acc.blk_read.saturating_add(sample.blk_read);
                            acc.blk_write = acc.blk_write.saturating_add(sample.blk_write);
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(ContainerGroupSample {
                                ts: sample.ts,
                                log_group: log_group.clone(),
                                cpu_pct: sample.cpu_pct,
                                mem_used: sample.mem_used,
                                mem_limit: sample.mem_limit,
                                net_rx: sample.net_rx,
                                net_tx: sample.net_tx,
                                blk_read: sample.blk_read,
                                blk_write: sample.blk_write,
                            });
                        }
                    }
                }
            }
            let mut series: Vec<ContainerGroupSample> = sum_by_ts.into_values().collect();
            series.sort_by_key(|sample| sample.ts);
            by_group.insert(log_group, series);
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

    #[test]
    fn averages_database_sizes_within_host_buckets() {
        let mut first = host_sample(100);
        first.metrics_size = 100;
        first.logs_size = 200;
        let mut second = host_sample(200);
        second.metrics_size = 300;
        second.logs_size = 600;

        let samples = downsample_host(vec![first, second], MetricsResolution::TenSeconds);

        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].metrics_size, 200);
        assert_eq!(samples[0].logs_size, 400);
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
        assert_eq!(
            hosts[0],
            HostSample {
                ts: 10_000,
                ..host_sample(200)
            }
        );

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
        assert_eq!(containers.sum[0].log_group, "web");
        assert_eq!(containers.containers.len(), 1);
        assert_eq!(
            containers.containers["abc123"][0],
            ContainerSample {
                ts: 10_000,
                ..container_sample(100, "web")
            }
        );

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
