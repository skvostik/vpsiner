use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::error::{AppError, AppResult};
use crate::metadata::ServiceRegistry;
use crate::metrics::downsampling::sum_by_bucket;
use crate::metrics::{rollup, schema};
use crate::model::{
    container_id::ContainerId,
    metrics::{
        ContainerGroupMetrics, ContainerPoint, ContainerSample, HostPoint, HostSample,
        MetricsResolution,
    },
    service_id::ServiceId,
    time::TimeRange,
};
use crate::sqlite::{open_pool, reclaim_free_pages};

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

/// SQLite-backed implementation.
pub struct SqliteMetricsStore {
    db_path: PathBuf,
    pool: SqlitePool,
    /// Resolves the stored `sid` back to a service name on read.
    services: Arc<ServiceRegistry>,
    /// Newest `10s` bucket written per source; a change of coarse bucket triggers a rollup.
    last_host_ts: Mutex<Option<i64>>,
    last_containers_ts: Mutex<Option<i64>>,
    downsample_max_gap_pct: u8,
}

impl SqliteMetricsStore {
    /// Opens the single long-lived connection used for the lifetime of the process.
    pub async fn connect(
        db_path: impl AsRef<Path>,
        services: Arc<ServiceRegistry>,
        cache_size_kb: u64,
        busy_timeout: Duration,
        downsample_max_gap_pct: u8,
    ) -> AppResult<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        tracing::info!(database = %db_path.display(), "opening metrics database connection");

        let pool = open_pool(&db_path, cache_size_kb, busy_timeout, true).await?;

        let store = Self {
            db_path,
            pool,
            services,
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
        file_size_bytes(&self.db_path).await
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

        reclaim_free_pages(&self.pool).await;
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
        Ok(schema::select_host(&self.pool, resolution, range)
            .await?
            .into_iter()
            .map(HostPoint::from)
            .collect())
    }

    async fn query_containers(
        &self,
        range: TimeRange,
        resolution: MetricsResolution,
    ) -> AppResult<HashMap<String, ContainerGroupMetrics>> {
        let mut by_service_and_container: HashMap<
            ServiceId,
            HashMap<ContainerId, Vec<ContainerSample>>,
        > = HashMap::new();
        // select_containers orders by (cid, ts), so each collected vector stays chronological.
        for sample in schema::select_containers(&self.pool, resolution, range).await? {
            by_service_and_container
                .entry(sample.service)
                .or_default()
                .entry(sample.cid)
                .or_default()
                .push(sample);
        }

        let mut by_service = HashMap::new();
        for (sid, by_container) in by_service_and_container {
            // Only reachable if retention reclaimed the sid between the read and the resolve.
            let Some(service) = self.services.name(sid) else {
                tracing::warn!(%sid, "dropping metrics for a service missing from the dictionary");
                continue;
            };
            let mut containers: HashMap<ContainerId, Vec<ContainerPoint>> = HashMap::new();
            for (cid, samples) in by_container {
                let points = samples
                    .into_iter()
                    .filter(|sample| sample.ts >= range.from)
                    .map(|sample| ContainerPoint::from_sample(sample, service.to_string()))
                    .collect();
                containers.insert(cid, points);
            }
            let sum = sum_by_bucket(containers.values());
            by_service.insert(
                service.to_string(),
                ContainerGroupMetrics { sum, containers },
            );
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
    use crate::metadata::SqliteMetadataStore;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_directory(name: &str) -> std::path::PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vpsiner-metrics-{name}-{suffix}"))
    }

    async fn test_store(db_path: impl AsRef<Path>) -> SqliteMetricsStore {
        let db_path = db_path.as_ref().to_path_buf();
        let metadata = SqliteMetadataStore::connect(
            db_path.with_file_name("metadata.db"),
            1_024,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let services = Arc::new(ServiceRegistry::load(Arc::new(metadata)).await.unwrap());
        SqliteMetricsStore::connect(db_path, services, 1_024, Duration::from_secs(5), 40)
            .await
            .unwrap()
    }

    fn host_sample(ts: i64) -> HostSample {
        HostSample {
            ts,
            cpu_pct_mill: 12_500,
            mem_used: 100,
            storage_used: 700,
            metrics_size: 900,
            logs_size: 1_000,
            net_rx_rate_mill: Some(300_000),
            net_tx_rate_mill: Some(400_000),
            disk_read_rate_mill: Some(500_000),
            disk_write_rate_mill: Some(600_000),
        }
    }

    fn container_sample(ts: i64, service: ServiceId) -> ContainerSample {
        ContainerSample {
            ts,
            service,
            cid: ContainerId::parse("abc123abc123").unwrap(),
            cpu_pct_mill: 25_000,
            mem_used: 1_000,
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
        let web = store.services.id_of("web").await.unwrap();
        let worker = store.services.id_of("worker").await.unwrap();

        store.insert_host(host_sample(10_000)).await.unwrap();
        store.insert_host(host_sample(20_000)).await.unwrap();
        store
            .insert_containers(vec![
                container_sample(10_000, web),
                container_sample(20_000, worker),
                container_sample(30_000, web),
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
        let cid = ContainerId::parse("abc123abc123").unwrap();
        assert_eq!(containers.containers[&cid][0].ts, 10_000);
        assert_eq!(containers.containers[&cid][0].service, "web");
        assert_eq!(containers.containers[&cid][0].mem_used, 1_000);

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
        let web = store.services.id_of("web").await.unwrap();

        store
            .insert_containers(vec![ContainerSample {
                net_rx_rate_mill: None,
                net_tx_rate_mill: None,
                blk_read_rate_mill: None,
                blk_write_rate_mill: None,
                ..container_sample(10_000, web)
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

        let cid = ContainerId::parse("abc123abc123").unwrap();
        assert_eq!(metrics.containers[&cid][0].net_rx_rate, None);
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
    async fn reports_nonzero_database_size() {
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
        let web = store.services.id_of("web").await.unwrap();

        store.insert_host(host_sample(10_000)).await.unwrap();
        store
            .insert_containers(vec![container_sample(10_000, web)])
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
    async fn coarse_queries_return_empty_without_persisted_rollups() {
        let directory = test_directory("rollup-empty");
        let store = test_store(directory.join("metrics.db")).await;

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

        assert!(minutes.is_empty());
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
