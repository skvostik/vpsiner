//! Table names, SQL text, and pool-level execution helpers for the metrics tables.
//!
//! Keeping SQL text, parameter binding, and row mapping together in one place avoids the
//! mismatch risk of editing a query in one file while its binds/row mapping live in another.
//! Every resolution has its own pair of tables: the `10s` tables are written by the
//! collectors, the coarser ones by the rollup that runs when a bucket closes.

use sqlx::{QueryBuilder, Row, SqlitePool};

use crate::error::{AppError, AppResult};
use crate::model::{
    container_id::ContainerId,
    metrics::{ContainerSample, HostSample, MetricsResolution},
    service_id::ServiceId,
    time::TimeRange,
};

/// Bound on rows per multi-row insert; SQLite allows 32766 bound parameters.
const INSERT_CHUNK_ROWS: usize = 3_000;

/// Rollups recompute whole buckets, so replaying one must overwrite rather than fail.
fn insert_verb(resolution: MetricsResolution) -> &'static str {
    match resolution {
        MetricsResolution::TenSeconds => "INSERT",
        _ => "INSERT OR REPLACE",
    }
}

fn storage(err: impl std::fmt::Display) -> AppError {
    AppError::Storage(err.to_string())
}

fn suffix(resolution: MetricsResolution) -> &'static str {
    match resolution {
        MetricsResolution::TenSeconds => "10s",
        MetricsResolution::OneMinute => "1m",
        MetricsResolution::FiveMinutes => "5m",
        MetricsResolution::OneHour => "1h",
    }
}

fn host_table(resolution: MetricsResolution) -> String {
    format!("host_metrics_{}", suffix(resolution))
}

fn container_table(resolution: MetricsResolution) -> String {
    format!("container_metrics_{}", suffix(resolution))
}

/// Creates the host table for `resolution` if it doesn't already exist.
pub async fn create_host_table(pool: &SqlitePool, resolution: MetricsResolution) -> AppResult<()> {
    // Table name is built only from the fixed MetricsResolution enum, never external input.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {} (
            ts INTEGER PRIMARY KEY,
            cpu_pct_mill INTEGER NOT NULL,
            mem_used INTEGER NOT NULL,
            storage_used INTEGER NOT NULL,
            metrics_size INTEGER NOT NULL,
            logs_size INTEGER NOT NULL,
            net_rx_rate_mill INTEGER,
            net_tx_rate_mill INTEGER,
            disk_read_rate_mill INTEGER,
            disk_write_rate_mill INTEGER
        )",
        host_table(resolution)
    )))
    .execute(pool)
    .await
    .map_err(storage)?;

    Ok(())
}

/// Creates the container table for `resolution` if it doesn't already exist.
pub async fn create_container_table(
    pool: &SqlitePool,
    resolution: MetricsResolution,
) -> AppResult<()> {
    // Table name is built only from the fixed MetricsResolution enum, never external input.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {} (
            ts INTEGER NOT NULL,
            cid BLOB NOT NULL,
            sid INTEGER NOT NULL,
            cpu_pct_mill INTEGER NOT NULL,
            mem_used INTEGER NOT NULL,
            net_rx_rate_mill INTEGER,
            net_tx_rate_mill INTEGER,
            blk_read_rate_mill INTEGER,
            blk_write_rate_mill INTEGER,
            PRIMARY KEY (ts, cid)
        ) WITHOUT ROWID",
        container_table(resolution)
    )))
    .execute(pool)
    .await
    .map_err(storage)?;

    Ok(())
}

pub async fn insert_host(
    pool: &SqlitePool,
    resolution: MetricsResolution,
    sample: HostSample,
) -> AppResult<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "{} INTO {}
                 (ts, cpu_pct_mill, mem_used, storage_used, metrics_size, logs_size, net_rx_rate_mill, net_tx_rate_mill, disk_read_rate_mill, disk_write_rate_mill)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        insert_verb(resolution),
        host_table(resolution)
    )))
    .bind(sample.ts)
    .bind(sample.cpu_pct_mill as i64)
    .bind(sample.mem_used as i64)
    .bind(sample.storage_used as i64)
    .bind(sample.metrics_size as i64)
    .bind(sample.logs_size as i64)
    .bind(sample.net_rx_rate_mill.map(|rate| rate as i64))
    .bind(sample.net_tx_rate_mill.map(|rate| rate as i64))
    .bind(sample.disk_read_rate_mill.map(|rate| rate as i64))
    .bind(sample.disk_write_rate_mill.map(|rate| rate as i64))
    .execute(pool)
    .await
    .map_err(storage)?;
    Ok(())
}

pub async fn insert_containers(
    pool: &SqlitePool,
    resolution: MetricsResolution,
    samples: Vec<ContainerSample>,
) -> AppResult<()> {
    if samples.is_empty() {
        return Ok(());
    }

    let prefix = format!(
        "{} INTO {}
                (ts, cid, sid, cpu_pct_mill, mem_used, net_rx_rate_mill, net_tx_rate_mill, blk_read_rate_mill, blk_write_rate_mill) ",
        insert_verb(resolution),
        container_table(resolution)
    );

    let mut transaction = pool.begin().await.map_err(storage)?;
    for chunk in samples.chunks(INSERT_CHUNK_ROWS) {
        let mut builder = QueryBuilder::new(prefix.clone());
        builder.push_values(chunk, |mut row, sample| {
            row.push_bind(sample.ts)
                .push_bind(sample.cid.as_bytes().as_slice())
                .push_bind(i64::from(sample.service.as_u32()))
                .push_bind(sample.cpu_pct_mill as i64)
                .push_bind(sample.mem_used as i64)
                .push_bind(sample.net_rx_rate_mill.map(|rate| rate as i64))
                .push_bind(sample.net_tx_rate_mill.map(|rate| rate as i64))
                .push_bind(sample.blk_read_rate_mill.map(|rate| rate as i64))
                .push_bind(sample.blk_write_rate_mill.map(|rate| rate as i64));
        });
        builder
            .build()
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
    }
    transaction.commit().await.map_err(storage)?;
    Ok(())
}

fn host_sample_from_row(row: sqlx::sqlite::SqliteRow) -> AppResult<HostSample> {
    fn count(row: &sqlx::sqlite::SqliteRow, column: &str) -> AppResult<u64> {
        Ok(row.try_get::<i64, _>(column).map_err(storage)? as u64)
    }

    fn rate(row: &sqlx::sqlite::SqliteRow, column: &str) -> AppResult<Option<u64>> {
        Ok(row
            .try_get::<Option<i64>, _>(column)
            .map_err(storage)?
            .map(|rate| rate as u64))
    }

    Ok(HostSample {
        ts: row.try_get("ts").map_err(storage)?,
        cpu_pct_mill: count(&row, "cpu_pct_mill")?,
        mem_used: count(&row, "mem_used")?,
        storage_used: count(&row, "storage_used")?,
        metrics_size: count(&row, "metrics_size")?,
        logs_size: count(&row, "logs_size")?,
        net_rx_rate_mill: rate(&row, "net_rx_rate_mill")?,
        net_tx_rate_mill: rate(&row, "net_tx_rate_mill")?,
        disk_read_rate_mill: rate(&row, "disk_read_rate_mill")?,
        disk_write_rate_mill: rate(&row, "disk_write_rate_mill")?,
    })
}

fn container_sample_from_row(row: sqlx::sqlite::SqliteRow) -> AppResult<ContainerSample> {
    fn count(row: &sqlx::sqlite::SqliteRow, column: &str) -> AppResult<u64> {
        Ok(row.try_get::<i64, _>(column).map_err(storage)? as u64)
    }

    fn rate(row: &sqlx::sqlite::SqliteRow, column: &str) -> AppResult<Option<u64>> {
        Ok(row
            .try_get::<Option<i64>, _>(column)
            .map_err(storage)?
            .map(|rate| rate as u64))
    }

    Ok(ContainerSample {
        ts: row.try_get("ts").map_err(storage)?,
        cid: {
            let bytes: Vec<u8> = row.try_get("cid").map_err(storage)?;
            ContainerId::from_bytes(&bytes)
                .ok_or_else(|| AppError::Storage("stored cid is not 6 bytes".into()))?
        },
        service: ServiceId::from_u32(row.try_get::<i64, _>("sid").map_err(storage)? as u32),
        cpu_pct_mill: count(&row, "cpu_pct_mill")?,
        mem_used: count(&row, "mem_used")?,
        net_rx_rate_mill: rate(&row, "net_rx_rate_mill")?,
        net_tx_rate_mill: rate(&row, "net_tx_rate_mill")?,
        blk_read_rate_mill: rate(&row, "blk_read_rate_mill")?,
        blk_write_rate_mill: rate(&row, "blk_write_rate_mill")?,
    })
}

pub async fn select_host(
    pool: &SqlitePool,
    resolution: MetricsResolution,
    range: TimeRange,
) -> AppResult<Vec<HostSample>> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT ts, cpu_pct_mill, mem_used, storage_used, metrics_size, logs_size, net_rx_rate_mill, net_tx_rate_mill, disk_read_rate_mill, disk_write_rate_mill
           FROM {}
           WHERE ts >= ? AND ts <= ?
         ORDER BY ts ASC",
        host_table(resolution)
    )))
    .bind(range.from)
    .bind(range.to)
    .fetch_all(pool)
    .await
    .map_err(storage)?;

    rows.into_iter().map(host_sample_from_row).collect()
}

pub async fn select_containers(
    pool: &SqlitePool,
    resolution: MetricsResolution,
    range: TimeRange,
) -> AppResult<Vec<ContainerSample>> {
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT ts, cid, sid, cpu_pct_mill, mem_used, net_rx_rate_mill, net_tx_rate_mill, blk_read_rate_mill, blk_write_rate_mill
         FROM {}
         WHERE ts >= ? AND ts <= ?
         ORDER BY ts ASC",
        container_table(resolution)
    )))
    .bind(range.from)
    .bind(range.to)
    .fetch_all(pool)
    .await
    .map_err(storage)?;

    rows.into_iter().map(container_sample_from_row).collect()
}

async fn select_bound(pool: &SqlitePool, sql: String) -> AppResult<Option<i64>> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
        .fetch_one(pool)
        .await
        .map_err(storage)
}

/// Newest stored bucket in the host table for `resolution`, or `None` when it is empty.
pub async fn select_host_max_ts(
    pool: &SqlitePool,
    resolution: MetricsResolution,
) -> AppResult<Option<i64>> {
    select_bound(
        pool,
        format!("SELECT MAX(ts) FROM {}", host_table(resolution)),
    )
    .await
}

/// Newest stored bucket in the container table for `resolution`, or `None` when it is empty.
pub async fn select_containers_max_ts(
    pool: &SqlitePool,
    resolution: MetricsResolution,
) -> AppResult<Option<i64>> {
    select_bound(
        pool,
        format!("SELECT MAX(ts) FROM {}", container_table(resolution)),
    )
    .await
}

/// Deletes rows older than `cutoff_ms` from the host table for `resolution`.
pub async fn delete_host_before(
    pool: &SqlitePool,
    resolution: MetricsResolution,
    cutoff_ms: i64,
) -> AppResult<u64> {
    let rows_affected = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {} WHERE ts < ?",
        host_table(resolution)
    )))
    .bind(cutoff_ms)
    .execute(pool)
    .await
    .map_err(storage)?
    .rows_affected();

    Ok(rows_affected)
}

/// Deletes rows older than `cutoff_ms` from the container table for `resolution`.
pub async fn delete_containers_before(
    pool: &SqlitePool,
    resolution: MetricsResolution,
    cutoff_ms: i64,
) -> AppResult<u64> {
    let rows_affected = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {} WHERE ts < ?",
        container_table(resolution)
    )))
    .bind(cutoff_ms)
    .execute(pool)
    .await
    .map_err(storage)?
    .rows_affected();

    Ok(rows_affected)
}
