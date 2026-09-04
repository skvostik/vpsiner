use std::path::Path;
use std::time::Duration;
use std::{collections::BTreeMap, path::PathBuf};

use async_trait::async_trait;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

use crate::error::{AppError, AppResult};
use crate::model::container_id::ContainerId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogCheckpoint {
    pub ts: i64,
    /// Cheap content hash of the line at `ts`, kept to futureproof exact-boundary dedup — unused for now.
    pub line_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogCheckpointEntry {
    pub service: String,
    pub cid: ContainerId,
    pub checkpoint: LogCheckpoint,
}

/// Persistence for `metadata.db` — per-service and per-container log checkpoints.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait MetadataStore: Send + Sync + 'static {
    async fn advance_log_checkpoint(
        &self,
        service: &str,
        cid: ContainerId,
        checkpoint: LogCheckpoint,
    ) -> AppResult<()>;

    async fn load_log_checkpoint(
        &self,
        service: &str,
        cid: ContainerId,
    ) -> AppResult<Option<LogCheckpoint>>;

    /// MAX(ts) per service, across its containers — backs the services listing.
    async fn list_service_log_watermarks(&self) -> AppResult<BTreeMap<String, i64>>;

    /// Every known (service, cid) checkpoint — used to preload dedup state on startup.
    async fn list_log_checkpoints(&self) -> AppResult<Vec<LogCheckpointEntry>>;

    /// Releases the persistent database connection.
    async fn close(&self);
}

pub struct SqliteMetadataStore {
    db_path: PathBuf,
    pool: SqlitePool,
}

impl SqliteMetadataStore {
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
            "CREATE TABLE IF NOT EXISTS log_checkpoints (
                service TEXT NOT NULL,
                cid BLOB NOT NULL,
                ts INTEGER NOT NULL,
                line_hash INTEGER NOT NULL,
                PRIMARY KEY (service, cid)
            )",
        )
        .execute(&pool)
        .await
        .map_err(storage)?;

        Ok(Self { db_path, pool })
    }
}

#[async_trait]
impl MetadataStore for SqliteMetadataStore {
    async fn advance_log_checkpoint(
        &self,
        service: &str,
        cid: ContainerId,
        checkpoint: LogCheckpoint,
    ) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO log_checkpoints (service, cid, ts, line_hash)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(service, cid) DO UPDATE SET
                     ts = excluded.ts,
                     line_hash = excluded.line_hash
                 WHERE excluded.ts >= log_checkpoints.ts",
        )
        .bind(service)
        .bind(cid.as_bytes().as_slice())
        .bind(checkpoint.ts)
        .bind(checkpoint.line_hash as i64)
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    async fn load_log_checkpoint(
        &self,
        service: &str,
        cid: ContainerId,
    ) -> AppResult<Option<LogCheckpoint>> {
        let row = sqlx::query(
            "SELECT ts, line_hash FROM log_checkpoints
             WHERE service = ? AND cid = ?",
        )
        .bind(service)
        .bind(cid.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?;
        Ok(row.map(|row| LogCheckpoint {
            ts: row.get("ts"),
            line_hash: row.get::<i64, _>("line_hash") as u64,
        }))
    }

    async fn list_service_log_watermarks(&self) -> AppResult<BTreeMap<String, i64>> {
        let rows = sqlx::query(
            "SELECT service, MAX(ts) AS ts
             FROM log_checkpoints
             GROUP BY service",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("service"), row.get("ts")))
            .collect())
    }

    async fn list_log_checkpoints(&self) -> AppResult<Vec<LogCheckpointEntry>> {
        let rows = sqlx::query(
            "SELECT service, cid, ts, line_hash
             FROM log_checkpoints",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let cid: Vec<u8> = row.get("cid");
                let cid = ContainerId::from_bytes(&cid)?;
                Some(LogCheckpointEntry {
                    service: row.get("service"),
                    cid,
                    checkpoint: LogCheckpoint {
                        ts: row.get("ts"),
                        line_hash: row.get::<i64, _>("line_hash") as u64,
                    },
                })
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

    async fn test_store(name: &str) -> SqliteMetadataStore {
        SqliteMetadataStore::connect(test_db(name), 1_024, Duration::from_secs(5))
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
    async fn advances_and_loads_a_log_checkpoint() {
        let store = test_store("record").await;
        let cid = ContainerId::parse("abc123abc123").unwrap();

        assert_eq!(store.load_log_checkpoint("group", cid).await.unwrap(), None);

        store
            .advance_log_checkpoint(
                "group",
                cid,
                LogCheckpoint {
                    ts: 1_700_000_000_000,
                    line_hash: 42,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            store.load_log_checkpoint("group", cid).await.unwrap(),
            Some(LogCheckpoint {
                ts: 1_700_000_000_000,
                line_hash: 42,
            })
        );
    }

    #[tokio::test]
    async fn upsert_keeps_the_max_ts_and_its_paired_hash() {
        let store = test_store("upsert-max").await;
        let cid = ContainerId::parse("abc123abc123").unwrap();

        store
            .advance_log_checkpoint(
                "group",
                cid,
                LogCheckpoint {
                    ts: 1_700_000_000_000,
                    line_hash: 1,
                },
            )
            .await
            .unwrap();
        // A stale write with a lower ts must not clobber the newer ts/hash pair.
        store
            .advance_log_checkpoint(
                "group",
                cid,
                LogCheckpoint {
                    ts: 1_600_000_000_000,
                    line_hash: 999,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            store.load_log_checkpoint("group", cid).await.unwrap(),
            Some(LogCheckpoint {
                ts: 1_700_000_000_000,
                line_hash: 1,
            })
        );

        store
            .advance_log_checkpoint(
                "group",
                cid,
                LogCheckpoint {
                    ts: 1_800_000_000_000,
                    line_hash: 2,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            store.load_log_checkpoint("group", cid).await.unwrap(),
            Some(LogCheckpoint {
                ts: 1_800_000_000_000,
                line_hash: 2,
            })
        );
    }

    #[tokio::test]
    async fn lists_service_log_watermarks_across_cids() {
        let store = test_store("list").await;
        let cid_a = ContainerId::parse("abc123abc123").unwrap();
        let cid_b = ContainerId::parse("def456def456").unwrap();
        let cid_c = ContainerId::parse("789abc789abc").unwrap();

        store
            .advance_log_checkpoint(
                "shop-web",
                cid_a,
                LogCheckpoint {
                    ts: 1_700_000_000_000,
                    line_hash: 1,
                },
            )
            .await
            .unwrap();
        store
            .advance_log_checkpoint(
                "shop-web",
                cid_b,
                LogCheckpoint {
                    ts: 1_700_000_005_000,
                    line_hash: 2,
                },
            )
            .await
            .unwrap();
        store
            .advance_log_checkpoint(
                "shop-worker",
                cid_c,
                LogCheckpoint {
                    ts: 1_650_000_000_000,
                    line_hash: 3,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            store.list_service_log_watermarks().await.unwrap(),
            BTreeMap::from([
                ("shop-web".to_string(), 1_700_000_005_000),
                ("shop-worker".to_string(), 1_650_000_000_000),
            ])
        );
    }
}
