use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions};

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogCheckpoint {
    pub ts: i64,
    /// Cheap content hash of the line at `ts`, kept to futureproof exact-boundary dedup — unused for now.
    pub line_hash: u64,
}

/// Persistence for `metadata.db` — a per-container checkpoint of the last log line received.
#[allow(dead_code)] // consumed by ingestion and the docker log task added in later steps
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait LogMetadataStore: Send + Sync + 'static {
    async fn record_received(
        &self,
        log_group: &str,
        container_id: &str,
        ts: i64,
        line_hash: u64,
    ) -> AppResult<()>;

    async fn checkpoint(
        &self,
        log_group: &str,
        container_id: &str,
    ) -> AppResult<Option<LogCheckpoint>>;

    /// MAX(last_received) per log_group, across its containers — backs the groups listing.
    async fn list_last_received(&self) -> AppResult<BTreeMap<String, i64>>;

    /// Every known (log_group, container_id) checkpoint — used to preload dedup state on startup.
    async fn list_checkpoints(&self) -> AppResult<Vec<(String, String, LogCheckpoint)>>;
}

pub struct SqliteLogMetadataStore {
    db_path: PathBuf,
}

impl SqliteLogMetadataStore {
    pub fn new(db_path: impl AsRef<Path>) -> Self {
        Self {
            db_path: db_path.as_ref().to_path_buf(),
        }
    }

    async fn pool(&self) -> AppResult<SqlitePool> {
        if let Some(parent) = self.db_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(storage)?;
        }

        let options = SqliteConnectOptions::new()
            .filename(&self.db_path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(storage)?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS container_last_received (
                log_group TEXT NOT NULL,
                container_id TEXT NOT NULL,
                last_received INTEGER NOT NULL,
                last_line_hash INTEGER NOT NULL,
                PRIMARY KEY (log_group, container_id)
            )",
        )
        .execute(&pool)
        .await
        .map_err(storage)?;

        Ok(pool)
    }
}

#[async_trait]
impl LogMetadataStore for SqliteLogMetadataStore {
    async fn record_received(
        &self,
        log_group: &str,
        container_id: &str,
        ts: i64,
        line_hash: u64,
    ) -> AppResult<()> {
        let pool = self.pool().await?;
        // last_line_hash is only ever paired with the ts it belongs to.
        sqlx::query(
            "INSERT INTO container_last_received (log_group, container_id, last_received, last_line_hash)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(log_group, container_id) DO UPDATE SET
                last_line_hash = CASE WHEN excluded.last_received >= last_received THEN excluded.last_line_hash ELSE last_line_hash END,
                last_received = MAX(last_received, excluded.last_received)",
        )
        .bind(log_group)
        .bind(container_id)
        .bind(ts)
        .bind(line_hash as i64)
        .execute(&pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    async fn checkpoint(
        &self,
        log_group: &str,
        container_id: &str,
    ) -> AppResult<Option<LogCheckpoint>> {
        let pool = self.pool().await?;
        let row = sqlx::query(
            "SELECT last_received, last_line_hash FROM container_last_received
             WHERE log_group = ? AND container_id = ?",
        )
        .bind(log_group)
        .bind(container_id)
        .fetch_optional(&pool)
        .await
        .map_err(storage)?;
        Ok(row.map(|row| LogCheckpoint {
            ts: row.get("last_received"),
            line_hash: row.get::<i64, _>("last_line_hash") as u64,
        }))
    }

    async fn list_last_received(&self) -> AppResult<BTreeMap<String, i64>> {
        let pool = self.pool().await?;
        let rows = sqlx::query(
            "SELECT log_group, MAX(last_received) AS last_received
             FROM container_last_received
             GROUP BY log_group",
        )
        .fetch_all(&pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("log_group"), row.get("last_received")))
            .collect())
    }

    async fn list_checkpoints(&self) -> AppResult<Vec<(String, String, LogCheckpoint)>> {
        let pool = self.pool().await?;
        let rows = sqlx::query(
            "SELECT log_group, container_id, last_received, last_line_hash
             FROM container_last_received",
        )
        .fetch_all(&pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get("log_group"),
                    row.get("container_id"),
                    LogCheckpoint {
                        ts: row.get("last_received"),
                        line_hash: row.get::<i64, _>("last_line_hash") as u64,
                    },
                )
            })
            .collect())
    }
}

fn storage(error: impl std::fmt::Display) -> AppError {
    AppError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_db(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vpsiner-metadata-{name}-{suffix}.db"))
    }

    #[tokio::test]
    async fn records_and_reads_back_a_checkpoint() {
        let store = SqliteLogMetadataStore::new(test_db("record"));

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
        let store = SqliteLogMetadataStore::new(test_db("upsert-max"));

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
        let store = SqliteLogMetadataStore::new(test_db("list"));

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
