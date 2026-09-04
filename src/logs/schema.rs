use sqlx::SqlitePool;

use crate::{error::AppResult, logs::storage};

pub(crate) async fn migrate(pool: &SqlitePool) -> AppResult<()> {
    // Relative to the week's start (see `week_start_ms`), saving ~6 bytes/row across the table
    // and its two ts-keyed indexes versus a full epoch-ms value.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY,
            ts_rel INTEGER NOT NULL,
            cid BLOB NOT NULL CHECK(length(cid) = 6),
            stream INTEGER NOT NULL,
            level INTEGER,
            line TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .map_err(storage)?;
    // Used for the default timeline queries: newest/oldest-first paging, time range filters,
    // and ordering by timestamp across a week database.
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_ts ON logs(ts_rel)")
        .execute(pool)
        .await
        .map_err(storage)?;
    // Used when the frontend filters by level and then reads the matching rows in time order.
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_level_ts ON logs(level, ts_rel)")
        .execute(pool)
        .await
        .map_err(storage)?;
    let fts_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'logs_fts')",
    )
    .fetch_one(pool)
    .await
    .map_err(storage)?;
    if !fts_exists {
        initialize_fts(pool).await?;
    }
    Ok(())
}

/// The trigram tokenizer supports substring search but only matches phrases of 3+ characters.
pub(crate) async fn initialize_fts(pool: &SqlitePool) -> AppResult<()> {
    let mut tx = pool.begin().await.map_err(storage)?;
    sqlx::query(
        "CREATE VIRTUAL TABLE IF NOT EXISTS logs_fts USING fts5(
            line,
            content='logs',
            content_rowid='id',
            tokenize='trigram'
        )",
    )
    .execute(&mut *tx)
    .await
    .map_err(storage)?;
    sqlx::query(
        "CREATE TRIGGER IF NOT EXISTS logs_fts_ai AFTER INSERT ON logs BEGIN
            INSERT INTO logs_fts(rowid, line) VALUES (new.id, new.line);
        END",
    )
    .execute(&mut *tx)
    .await
    .map_err(storage)?;
    sqlx::query(
        "CREATE TRIGGER IF NOT EXISTS logs_fts_bd BEFORE DELETE ON logs BEGIN
            INSERT INTO logs_fts(logs_fts, rowid, line) VALUES ('delete', old.id, old.line);
        END",
    )
    .execute(&mut *tx)
    .await
    .map_err(storage)?;
    sqlx::query("INSERT INTO logs_fts(logs_fts) VALUES ('rebuild')")
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
    tx.commit().await.map_err(storage)
}
