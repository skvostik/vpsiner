//! Shared single-connection SQLite pool setup used by every store.

use std::path::Path;
use std::time::Duration;

use sqlx::SqlitePool;
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};

use crate::error::{AppError, AppResult};

/// Only a handful of distinct statements are ever prepared against these databases.
const STATEMENT_CACHE_CAPACITY: usize = 32;
/// Recommended by SQLite for `PRAGMA optimize`.
const ANALYSIS_LIMIT: u32 = 400;
/// Free pages returned per `incremental_vacuum` step.
const VACUUM_CHUNK_PAGES: u64 = 1_000;

/// Opens the single long-lived connection a store keeps for the lifetime of the process.
///
/// `auto_vacuum` should only be enabled for stores that actually run `PRAGMA incremental_vacuum`.
pub async fn open_pool(
    db_path: &Path,
    cache_size_kb: u64,
    busy_timeout: Duration,
    auto_vacuum: bool,
) -> AppResult<SqlitePool> {
    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(storage)?;
    }

    let auto_vacuum_mode = if auto_vacuum {
        SqliteAutoVacuum::Incremental
    } else {
        SqliteAutoVacuum::None
    };
    let options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Delete)
        .synchronous(SqliteSynchronous::Full)
        .auto_vacuum(auto_vacuum_mode)
        .busy_timeout(busy_timeout)
        .foreign_keys(false)
        .statement_cache_capacity(STATEMENT_CACHE_CAPACITY)
        .analysis_limit(ANALYSIS_LIMIT)
        .optimize_on_close(true, ANALYSIS_LIMIT)
        // Negative values are interpreted as KiB rather than pages.
        .pragma("cache_size", format!("-{cache_size_kb}"));

    // A single connection serialises every reader and writer against a store's database.
    SqlitePoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(options)
        .await
        .map_err(storage)
}

/// Returns free pages to the filesystem in bounded steps so a large retention
/// delete never blocks the connection for one long stretch.
///
/// Only has an effect on a pool opened with `open_pool(.., auto_vacuum: true)`.
pub async fn reclaim_free_pages(pool: &SqlitePool) {
    let free_pages: i64 = match sqlx::query_scalar("PRAGMA freelist_count")
        .fetch_one(pool)
        .await
    {
        Ok(pages) => pages,
        Err(err) => {
            tracing::warn!(error = %err, "failed to read database freelist");
            return;
        }
    };

    for _ in 0..free_pages.unsigned_abs().div_ceil(VACUUM_CHUNK_PAGES) {
        if let Err(err) = sqlx::query(sqlx::AssertSqlSafe(format!(
            "PRAGMA incremental_vacuum({VACUUM_CHUNK_PAGES})"
        )))
        .execute(pool)
        .await
        {
            tracing::warn!(error = %err, "failed to vacuum database");
            return;
        }
    }

    if let Err(err) = sqlx::query("PRAGMA optimize").execute(pool).await {
        tracing::warn!(error = %err, "failed to optimize database");
    }
}

fn storage(err: impl std::fmt::Display) -> AppError {
    AppError::Storage(err.to_string())
}
