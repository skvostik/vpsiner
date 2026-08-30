use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};

use crate::error::{AppError, AppResult};
use crate::metrics::rate::{bytes_per_second, counter_delta};
use crate::model::{
    ContainerGroupMetrics, ContainerMetricsByLogGroup, ContainerPoint, ContainerSample, GroupPoint,
    HostPoint, HostSample, MetricsResolution, TimeRange,
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

/// Time-weighted counter accumulation for one bucket: observed delta over observed elapsed time.
#[derive(Default, Clone, Copy)]
struct RateAccumulator {
    delta: u128,
    elapsed_ms: i64,
}

impl RateAccumulator {
    fn observe(&mut self, current: u64, previous: u64, dt_ms: i64) {
        // An interval spanning a counter reset carries unknown traffic, so it is left out entirely.
        let Some(delta) = counter_delta(current, previous).filter(|_| dt_ms > 0) else {
            return;
        };
        self.delta += u128::from(delta);
        self.elapsed_ms += dt_ms;
    }

    fn per_second(self) -> f64 {
        bytes_per_second(self.delta, self.elapsed_ms)
    }
}

/// Four cumulative counters bucketed together. `previous` deliberately survives bucket
/// boundaries so the interval straddling a boundary counts toward the later bucket.
#[derive(Default, Clone, Copy)]
struct CounterRates {
    accumulators: [RateAccumulator; 4],
    previous: Option<(i64, [u64; 4])>,
}

impl CounterRates {
    fn observe(&mut self, ts: i64, values: [u64; 4]) {
        if let Some((previous_ts, previous_values)) = self.previous {
            let dt_ms = ts - previous_ts;
            for index in 0..4 {
                self.accumulators[index].observe(values[index], previous_values[index], dt_ms);
            }
        }
        self.previous = Some((ts, values));
    }

    fn per_second(&self) -> [f64; 4] {
        self.accumulators.map(RateAccumulator::per_second)
    }

    fn start_bucket(&mut self) {
        self.accumulators = [RateAccumulator::default(); 4];
    }
}

#[derive(Default)]
struct HostGauges {
    count: u64,
    cpu_pct: f64,
    mem_used: u128,
    mem_total: u128,
    storage_used: u128,
    storage_total: u128,
    metrics_size: u128,
    logs_size: u128,
}

impl HostGauges {
    fn add(&mut self, sample: &HostSample) {
        self.count += 1;
        self.cpu_pct += sample.cpu_pct;
        self.mem_used += sample.mem_used as u128;
        self.mem_total += sample.mem_total as u128;
        self.storage_used += sample.storage_used as u128;
        self.storage_total += sample.storage_total as u128;
        self.metrics_size += sample.metrics_size as u128;
        self.logs_size += sample.logs_size as u128;
    }

    fn finish(&self, ts: i64, rates: [f64; 4]) -> Option<HostPoint> {
        if self.count == 0 {
            return None;
        }
        let count = self.count as u128;
        Some(HostPoint {
            ts,
            cpu_pct: self.cpu_pct / self.count as f64,
            mem_used: (self.mem_used / count) as u64,
            mem_total: (self.mem_total / count) as u64,
            storage_used: (self.storage_used / count) as u64,
            storage_total: (self.storage_total / count) as u64,
            metrics_size: (self.metrics_size / count) as u64,
            logs_size: (self.logs_size / count) as u64,
            net_rx_rate: rates[0],
            net_tx_rate: rates[1],
            disk_read_rate: rates[2],
            disk_write_rate: rates[3],
        })
    }
}

fn downsample_host(samples: Vec<HostSample>, resolution: MetricsResolution) -> Vec<HostPoint> {
    let bucket_ms = resolution.bucket_ms();
    let mut points: Vec<HostPoint> = Vec::new();
    let mut current_bucket = i64::MIN;
    let mut gauges = HostGauges::default();
    let mut rates = CounterRates::default();

    for sample in samples {
        let bucket = bucket_end(sample.ts, bucket_ms);
        if bucket != current_bucket {
            points.extend(gauges.finish(current_bucket, rates.per_second()));
            gauges = HostGauges::default();
            rates.start_bucket();
            current_bucket = bucket;
        }

        gauges.add(&sample);
        rates.observe(
            sample.ts,
            [
                sample.net_rx,
                sample.net_tx,
                sample.disk_read,
                sample.disk_write,
            ],
        );
    }

    points.extend(gauges.finish(current_bucket, rates.per_second()));
    // ts is the bucket's closing instant, so a bucket has fully elapsed exactly when ts <= now.
    points.retain(|point| point.ts <= now_ms());
    points
}

#[derive(Default)]
struct ContainerGauges {
    count: u64,
    cpu_pct: f64,
    mem_used: u128,
    mem_limit: u128,
    log_group: String,
}

impl ContainerGauges {
    fn add(&mut self, sample: &ContainerSample) {
        if self.count == 0 {
            self.log_group = sample.log_group.clone();
        }
        self.count += 1;
        self.cpu_pct += sample.cpu_pct;
        self.mem_used += sample.mem_used as u128;
        self.mem_limit += sample.mem_limit as u128;
    }

    fn finish(&self, ts: i64, rates: [f64; 4]) -> Option<ContainerPoint> {
        if self.count == 0 {
            return None;
        }
        let count = self.count as u128;
        Some(ContainerPoint {
            ts,
            log_group: self.log_group.clone(),
            cpu_pct: self.cpu_pct / self.count as f64,
            mem_used: (self.mem_used / count) as u64,
            mem_limit: (self.mem_limit / count) as u64,
            net_rx_rate: rates[0],
            net_tx_rate: rates[1],
            blk_read_rate: rates[2],
            blk_write_rate: rates[3],
        })
    }
}

/// Expects the samples of a single container, ordered by `ts`.
fn downsample_container(
    samples: Vec<ContainerSample>,
    resolution: MetricsResolution,
) -> Vec<ContainerPoint> {
    let bucket_ms = resolution.bucket_ms();
    let mut points: Vec<ContainerPoint> = Vec::new();
    let mut current_bucket = i64::MIN;
    let mut gauges = ContainerGauges::default();
    let mut rates = CounterRates::default();

    for sample in samples {
        let bucket = bucket_end(sample.ts, bucket_ms);
        if bucket != current_bucket {
            points.extend(gauges.finish(current_bucket, rates.per_second()));
            gauges = ContainerGauges::default();
            rates.start_bucket();
            current_bucket = bucket;
        }

        gauges.add(&sample);
        rates.observe(
            sample.ts,
            [
                sample.net_rx,
                sample.net_tx,
                sample.blk_read,
                sample.blk_write,
            ],
        );
    }

    points.extend(gauges.finish(current_bucket, rates.per_second()));
    // ts is the bucket's closing instant, so a bucket has fully elapsed exactly when ts <= now.
    points.retain(|point| point.ts <= now_ms());
    points
}

/// Rates add cleanly across containers, so a group series is the per-bucket sum of its members.
fn sum_by_bucket<'a>(series: impl Iterator<Item = &'a Vec<ContainerPoint>>) -> Vec<GroupPoint> {
    let mut totals: HashMap<i64, GroupPoint> = HashMap::new();
    for points in series {
        for point in points {
            let total = totals.entry(point.ts).or_insert(GroupPoint {
                ts: point.ts,
                ..GroupPoint::default()
            });
            total.cpu_pct += point.cpu_pct;
            total.mem_used = total.mem_used.saturating_add(point.mem_used);
            total.mem_limit = total.mem_limit.saturating_add(point.mem_limit);
            total.net_rx_rate += point.net_rx_rate;
            total.net_tx_rate += point.net_tx_rate;
            total.blk_read_rate += point.blk_read_rate;
            total.blk_write_rate += point.blk_write_rate;
        }
    }

    let mut sum: Vec<GroupPoint> = totals.into_values().collect();
    sum.sort_by_key(|point| point.ts);
    sum
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

    fn host_counter(ts: i64, net_rx: u64) -> HostSample {
        HostSample {
            net_rx,
            ..host_sample(ts)
        }
    }

    fn container_counter(ts: i64, cid: &str, net_rx: u64) -> ContainerSample {
        ContainerSample {
            cid: cid.into(),
            net_rx,
            ..container_sample(ts, "web")
        }
    }

    #[test]
    fn weights_bucket_rate_by_elapsed_time_not_sample_count() {
        // Bucket (60s, 120s] with a missing sample: intervals are 10s, 10s, 30s, 10s and all
        // traffic falls in the 30s one. A simple mean of pair rates would report 250 B/s.
        let samples = vec![
            host_counter(60_000, 0),
            host_counter(70_000, 0),
            host_counter(80_000, 0),
            host_counter(110_000, 30_000),
            host_counter(120_000, 30_000),
        ];

        let points = downsample_host(samples, MetricsResolution::OneMinute);

        assert_eq!(points.len(), 2);
        assert_eq!(points[1].ts, 120_000);
        assert_eq!(points[1].net_rx_rate, 500.0);
    }

    #[test]
    fn excludes_reset_intervals_from_both_delta_and_elapsed_time() {
        // 1000 bytes over 10s, then a reset; counting the reset's 10s would halve the rate.
        let samples = vec![
            host_counter(60_000, 1_000),
            host_counter(70_000, 2_000),
            host_counter(80_000, 500),
        ];

        let points = downsample_host(samples, MetricsResolution::OneMinute);

        assert_eq!(points.len(), 2);
        assert_eq!(points[1].net_rx_rate, 100.0);
    }

    #[test]
    fn first_sample_without_predecessor_has_no_rate() {
        let points = downsample_host(
            vec![host_counter(60_000, 5_000)],
            MetricsResolution::OneMinute,
        );

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].net_rx_rate, 0.0);
    }

    #[test]
    fn group_sum_does_not_spike_when_a_container_appears() {
        let steady = downsample_container(
            vec![
                container_counter(60_000, "steady", 0),
                container_counter(120_000, "steady", 60_000),
            ],
            MetricsResolution::OneMinute,
        );
        // Joins late carrying a large lifetime counter; summing counters would spike here.
        let joining = downsample_container(
            vec![container_counter(120_000, "joining", 10_000_000)],
            MetricsResolution::OneMinute,
        );

        let sum = sum_by_bucket([steady, joining].iter());

        let second = sum.iter().find(|point| point.ts == 120_000).unwrap();
        assert_eq!(second.net_rx_rate, 1_000.0);
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
