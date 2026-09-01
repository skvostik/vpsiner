use std::path::Path;
use std::time::Duration;
use std::{collections::BTreeMap, path::PathBuf};

use async_trait::async_trait;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogCheckpoint {
    pub ts: i64,
    /// Cheap content hash of the line at `ts`, kept to futureproof exact-boundary dedup — unused for now.
    pub line_hash: u64,
}

/// Persistence for `metadata.db` — a per-container checkpoint of the last log line received.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait LogMetadataStore: Send + Sync + 'static {
    async fn record_received(
        &self,
        service: &str,
        container_id: &str,
        ts: i64,
        line_hash: u64,
    ) -> AppResult<()>;

    async fn checkpoint(
        &self,
        service: &str,
        container_id: &str,
    ) -> AppResult<Option<LogCheckpoint>>;

    /// MAX(last_received) per service, across its containers — backs the services listing.
    async fn list_last_received(&self) -> AppResult<BTreeMap<String, i64>>;

    /// Every known (service, container_id) checkpoint — used to preload dedup state on startup.
    async fn list_checkpoints(&self) -> AppResult<Vec<(String, String, LogCheckpoint)>>;

    /// Releases the persistent database connection.
    async fn close(&self);
}

pub struct SqliteLogMetadataStore {
    db_path: PathBuf,
    pool: SqlitePool,
}

impl SqliteLogMetadataStore {
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

        tracing::info!(database = %db_path.display(), "opening metadata database connection");

        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Delete)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(busy_timeout)
            .foreign_keys(false)
            .statement_cache_capacity(32)
            .analysis_limit(400)
            .optimize_on_close(true, 400)
            // Negative values are interpreted as KiB rather than pages.
            .pragma("cache_size", format!("-{cache_size_kb}"));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(options)
            .await
            .map_err(storage)?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS container_last_received (
                service TEXT NOT NULL,
                container_id TEXT NOT NULL,
                last_received INTEGER NOT NULL,
                last_line_hash INTEGER NOT NULL,
                PRIMARY KEY (service, container_id)
            )",
        )
        .execute(&pool)
        .await
        .map_err(storage)?;

        Ok(Self { db_path, pool })
    }
}

#[async_trait]
impl LogMetadataStore for SqliteLogMetadataStore {
    async fn record_received(
        &self,
        service: &str,
        container_id: &str,
        ts: i64,
        line_hash: u64,
    ) -> AppResult<()> {
        // last_line_hash is only ever paired with the ts it belongs to.
        sqlx::query(
            "INSERT INTO container_last_received (service, container_id, last_received, last_line_hash)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(service, container_id) DO UPDATE SET
                last_line_hash = CASE WHEN excluded.last_received >= last_received THEN excluded.last_line_hash ELSE last_line_hash END,
                last_received = MAX(last_received, excluded.last_received)",
        )
        .bind(service)
        .bind(container_id)
        .bind(ts)
        .bind(line_hash as i64)
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    async fn checkpoint(
        &self,
        service: &str,
        container_id: &str,
    ) -> AppResult<Option<LogCheckpoint>> {
        let row = sqlx::query(
            "SELECT last_received, last_line_hash FROM container_last_received
             WHERE service = ? AND container_id = ?",
        )
        .bind(service)
        .bind(container_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?;
        Ok(row.map(|row| LogCheckpoint {
            ts: row.get("last_received"),
            line_hash: row.get::<i64, _>("last_line_hash") as u64,
        }))
    }

    async fn list_last_received(&self) -> AppResult<BTreeMap<String, i64>> {
        let rows = sqlx::query(
            "SELECT service, MAX(last_received) AS last_received
             FROM container_last_received
             GROUP BY service",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("service"), row.get("last_received")))
            .collect())
    }

    async fn list_checkpoints(&self) -> AppResult<Vec<(String, String, LogCheckpoint)>> {
        let rows = sqlx::query(
            "SELECT service, container_id, last_received, last_line_hash
             FROM container_last_received",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get("service"),
                    row.get("container_id"),
                    LogCheckpoint {
                        ts: row.get("last_received"),
                        line_hash: row.get::<i64, _>("last_line_hash") as u64,
                    },
                )
            })
            .collect())
    }

    async fn close(&self) {
        tracing::info!(database = %self.db_path.display(), "closing metadata database connection");
        self.pool.close().await;
    }
}

fn storage(error: impl std::fmt::Display) -> AppError {
    AppError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_db(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vpsiner-metadata-{name}-{suffix}.db"))
    }

    async fn test_store(name: &str) -> SqliteLogMetadataStore {
        SqliteLogMetadataStore::connect(test_db(name), 1_024, Duration::from_secs(5))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn uses_rollback_journal_and_full_synchronous_mode() {
        let store = test_store("settings").await;

        let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
            .fetch_one(&store.pool)
            .await
            .unwrap();

        assert_eq!(journal_mode, "delete");
        assert_eq!(synchronous, 2);
    }

    #[tokio::test]
    async fn records_and_reads_back_a_checkpoint() {
        let store = test_store("record").await;

        assert_eq!(store.checkpoint("group", "abc123").await.unwrap(), None);

        store
            .record_received("group", "abc123", 1_700_000_000_000, 42)
            .await
            .unwrap();

        assert_eq!(
            store.checkpoint("group", "abc123").await.unwrap(),
            Some(LogCheckpoint {
                ts: 1_700_000_000_000,
                line_hash: 42,
            })
        );
    }

    #[tokio::test]
    async fn upsert_keeps_the_max_ts_and_its_paired_hash() {
        let store = test_store("upsert-max").await;

        store
            .record_received("group", "abc123", 1_700_000_000_000, 1)
            .await
            .unwrap();
        // A stale write with a lower ts must not clobber the newer ts/hash pair.
        store
            .record_received("group", "abc123", 1_600_000_000_000, 999)
            .await
            .unwrap();

        assert_eq!(
            store.checkpoint("group", "abc123").await.unwrap(),
            Some(LogCheckpoint {
                ts: 1_700_000_000_000,
                line_hash: 1,
            })
        );

        store
            .record_received("group", "abc123", 1_800_000_000_000, 2)
            .await
            .unwrap();

        assert_eq!(
            store.checkpoint("group", "abc123").await.unwrap(),
            Some(LogCheckpoint {
                ts: 1_800_000_000_000,
                line_hash: 2,
            })
        );
    }

    #[tokio::test]
    async fn lists_the_max_last_received_per_group_across_containers() {
        let store = test_store("list").await;

        store
            .record_received("shop-web", "abc123", 1_700_000_000_000, 1)
            .await
            .unwrap();
        store
            .record_received("shop-web", "def456", 1_700_000_005_000, 2)
            .await
            .unwrap();
        store
            .record_received("shop-worker", "ghi789", 1_650_000_000_000, 3)
            .await
            .unwrap();

        assert_eq!(
            store.list_last_received().await.unwrap(),
            BTreeMap::from([
                ("shop-web".to_string(), 1_700_000_005_000),
                ("shop-worker".to_string(), 1_650_000_000_000),
            ])
        );
    }
}
