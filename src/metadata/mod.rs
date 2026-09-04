use std::path::Path;
use std::time::Duration;
use std::{collections::BTreeMap, path::PathBuf};

use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

use crate::error::{AppError, AppResult};
use crate::model::container_id::ContainerId;
use crate::model::service_id::ServiceId;
use crate::sqlite::open_pool;

pub mod service_registry;

pub use service_registry::ServiceRegistry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogCheckpoint {
    pub ts: i64,
    /// Cheap content hash of the line at `ts`, kept to futureproof exact-boundary dedup — unused for now.
    pub line_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogCheckpointEntry {
    pub sid: ServiceId,
    pub cid: ContainerId,
    pub checkpoint: LogCheckpoint,
}

/// Persistence for `metadata.db` — the service name dictionary plus log checkpoints.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait MetadataStore: Send + Sync + 'static {
    /// Interns `name`, refreshing its last-seen watermark; returns the stable id.
    async fn resolve_service(&self, name: &str, now_ms: i64) -> AppResult<ServiceId>;

    /// The whole dictionary — used to preload the in-memory registry on startup.
    async fn list_services(&self) -> AppResult<Vec<(ServiceId, String)>>;

    /// Refreshes the last-seen watermark of an already interned service.
    async fn touch_service(&self, sid: ServiceId, now_ms: i64) -> AppResult<()>;

    /// Drops dictionary entries unseen since `cutoff_ms`; returns the ids that were removed.
    async fn delete_services_before(&self, cutoff_ms: i64) -> AppResult<Vec<ServiceId>>;

    async fn advance_log_checkpoint(
        &self,
        sid: ServiceId,
        cid: ContainerId,
        checkpoint: LogCheckpoint,
    ) -> AppResult<()>;

    async fn load_log_checkpoint(
        &self,
        sid: ServiceId,
        cid: ContainerId,
    ) -> AppResult<Option<LogCheckpoint>>;

    /// MAX(ts) per service, across its containers — backs the services listing.
    async fn list_service_log_watermarks(&self) -> AppResult<BTreeMap<ServiceId, i64>>;

    /// Every known (sid, cid) checkpoint — used to preload dedup state on startup.
    async fn list_log_checkpoints(&self) -> AppResult<Vec<LogCheckpointEntry>>;

    /// Drops checkpoints whose latest ts predates `cutoff_ms`; returns the number removed.
    async fn delete_before(&self, cutoff_ms: i64) -> AppResult<u64>;

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
        tracing::info!(database = %db_path.display(), "opening metadata database connection");

        let pool = open_pool(&db_path, cache_size_kb, busy_timeout, false).await?;

        // AUTOINCREMENT so a reclaimed sid is never handed to a different service.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS services (
                sid INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                last_seen_ms INTEGER NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .map_err(storage)?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS log_checkpoints (
                sid INTEGER NOT NULL,
                cid BLOB NOT NULL,
                ts INTEGER NOT NULL,
                line_hash INTEGER NOT NULL,
                PRIMARY KEY (sid, cid)
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
    async fn resolve_service(&self, name: &str, now_ms: i64) -> AppResult<ServiceId> {
        let sid: i64 = sqlx::query_scalar(
            "INSERT INTO services (name, last_seen_ms)
             VALUES (?, ?)
             ON CONFLICT(name) DO UPDATE SET
                 last_seen_ms = MAX(services.last_seen_ms, excluded.last_seen_ms)
             RETURNING sid",
        )
        .bind(name)
        .bind(now_ms)
        .fetch_one(&self.pool)
        .await
        .map_err(storage)?;
        Ok(ServiceId::from_u32(sid as u32))
    }

    async fn list_services(&self) -> AppResult<Vec<(ServiceId, String)>> {
        let rows = sqlx::query("SELECT sid, name FROM services")
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    ServiceId::from_u32(row.get::<i64, _>("sid") as u32),
                    row.get("name"),
                )
            })
            .collect())
    }

    async fn touch_service(&self, sid: ServiceId, now_ms: i64) -> AppResult<()> {
        sqlx::query("UPDATE services SET last_seen_ms = ? WHERE sid = ? AND last_seen_ms < ?")
            .bind(now_ms)
            .bind(i64::from(sid.as_u32()))
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(())
    }

    async fn delete_services_before(&self, cutoff_ms: i64) -> AppResult<Vec<ServiceId>> {
        let rows: Vec<i64> =
            sqlx::query_scalar("DELETE FROM services WHERE last_seen_ms < ? RETURNING sid")
                .bind(cutoff_ms)
                .fetch_all(&self.pool)
                .await
                .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|sid| ServiceId::from_u32(sid as u32))
            .collect())
    }

    async fn advance_log_checkpoint(
        &self,
        sid: ServiceId,
        cid: ContainerId,
        checkpoint: LogCheckpoint,
    ) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO log_checkpoints (sid, cid, ts, line_hash)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(sid, cid) DO UPDATE SET
                     ts = excluded.ts,
                     line_hash = excluded.line_hash
                 WHERE excluded.ts >= log_checkpoints.ts",
        )
        .bind(i64::from(sid.as_u32()))
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
        sid: ServiceId,
        cid: ContainerId,
    ) -> AppResult<Option<LogCheckpoint>> {
        let row = sqlx::query(
            "SELECT ts, line_hash FROM log_checkpoints
             WHERE sid = ? AND cid = ?",
        )
        .bind(i64::from(sid.as_u32()))
        .bind(cid.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?;
        Ok(row.map(|row| LogCheckpoint {
            ts: row.get("ts"),
            line_hash: row.get::<i64, _>("line_hash") as u64,
        }))
    }

    async fn list_service_log_watermarks(&self) -> AppResult<BTreeMap<ServiceId, i64>> {
        let rows = sqlx::query(
            "SELECT sid, MAX(ts) AS ts
             FROM log_checkpoints
             GROUP BY sid",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    ServiceId::from_u32(row.get::<i64, _>("sid") as u32),
                    row.get("ts"),
                )
            })
            .collect())
    }

    async fn list_log_checkpoints(&self) -> AppResult<Vec<LogCheckpointEntry>> {
        let rows = sqlx::query(
            "SELECT sid, cid, ts, line_hash
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
                    sid: ServiceId::from_u32(row.get::<i64, _>("sid") as u32),
                    cid,
                    checkpoint: LogCheckpoint {
                        ts: row.get("ts"),
                        line_hash: row.get::<i64, _>("line_hash") as u64,
                    },
                })
            })
            .collect())
    }

    async fn delete_before(&self, cutoff_ms: i64) -> AppResult<u64> {
        let result = sqlx::query("DELETE FROM log_checkpoints WHERE ts < ?")
            .bind(cutoff_ms)
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(result.rows_affected())
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
        let sid = store.resolve_service("group", 0).await.unwrap();

        assert_eq!(store.load_log_checkpoint(sid, cid).await.unwrap(), None);

        store
            .advance_log_checkpoint(
                sid,
                cid,
                LogCheckpoint {
                    ts: 1_700_000_000_000,
                    line_hash: 42,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            store.load_log_checkpoint(sid, cid).await.unwrap(),
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
        let sid = store.resolve_service("group", 0).await.unwrap();

        store
            .advance_log_checkpoint(
                sid,
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
                sid,
                cid,
                LogCheckpoint {
                    ts: 1_600_000_000_000,
                    line_hash: 999,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            store.load_log_checkpoint(sid, cid).await.unwrap(),
            Some(LogCheckpoint {
                ts: 1_700_000_000_000,
                line_hash: 1,
            })
        );

        store
            .advance_log_checkpoint(
                sid,
                cid,
                LogCheckpoint {
                    ts: 1_800_000_000_000,
                    line_hash: 2,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            store.load_log_checkpoint(sid, cid).await.unwrap(),
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
        let web = store.resolve_service("shop-web", 0).await.unwrap();
        let worker = store.resolve_service("shop-worker", 0).await.unwrap();

        store
            .advance_log_checkpoint(
                web,
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
                web,
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
                worker,
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
            BTreeMap::from([(web, 1_700_000_005_000), (worker, 1_650_000_000_000),])
        );
    }

    #[tokio::test]
    async fn interning_the_same_name_returns_the_same_id() {
        let store = test_store("intern").await;

        let first = store.resolve_service("shop-web", 100).await.unwrap();
        let second = store.resolve_service("shop-web", 50).await.unwrap();
        let other = store.resolve_service("shop-worker", 100).await.unwrap();

        assert_eq!(first, second);
        assert_ne!(first, other);
        // A stale resolve must not drag the watermark backwards.
        assert_eq!(
            store.delete_services_before(75).await.unwrap(),
            Vec::<ServiceId>::new()
        );
    }

    #[tokio::test]
    async fn reclaimed_ids_are_never_reused() {
        let store = test_store("reclaim").await;

        let old = store.resolve_service("gone", 100).await.unwrap();
        assert_eq!(store.delete_services_before(200).await.unwrap(), vec![old]);

        let fresh = store.resolve_service("new", 300).await.unwrap();
        assert_ne!(old, fresh);
        assert_eq!(
            store.list_services().await.unwrap(),
            vec![(fresh, "new".to_string())]
        );
    }

    #[tokio::test]
    async fn touch_only_moves_the_watermark_forward() {
        let store = test_store("touch").await;
        let sid = store.resolve_service("svc", 500).await.unwrap();

        store.touch_service(sid, 100).await.unwrap();
        assert_eq!(
            store.delete_services_before(400).await.unwrap(),
            Vec::<ServiceId>::new()
        );

        store.touch_service(sid, 900).await.unwrap();
        assert_eq!(
            store.delete_services_before(800).await.unwrap(),
            Vec::<ServiceId>::new()
        );
    }
}
