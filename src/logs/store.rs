use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::{
    QueryBuilder, Row, Sqlite, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
};
use tokio::sync::{Mutex, OnceCell, OwnedMutexGuard};

use super::{
    database_week_start_ms, decode_cursor, detect_level, encode_cursor, safe_service_path,
    sanitize_fts_query, week_database_name,
};
use crate::error::{AppError, AppResult};
use crate::model::{
    container_id::ContainerId,
    logs::{LogCursor, LogFilter, LogLevel, LogLine, LogPage, LogStream},
};

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    async fn database_size_bytes(&self) -> AppResult<u64>;

    async fn delete_before(&self, cutoff_ms: i64) -> AppResult<Vec<String>>;

    async fn append(&self, service: &str, lines: Vec<LogLine>) -> AppResult<()>;
    async fn query(&self, service: &str, filter: LogFilter) -> AppResult<LogPage>;

    /// Releases every cached week-database connection.
    async fn close(&self);
}

/// Only a handful of distinct statements are ever prepared against a week database.
const STATEMENT_CACHE_CAPACITY: usize = 32;
/// Recommended by SQLite for `PRAGMA optimize`.
const ANALYSIS_LIMIT: u32 = 400;
const WEEK_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
const DEFAULT_PAGE_LIMIT: u32 = 100;

pub struct SqliteLogStore {
    root: PathBuf,
    pools: Arc<PoolCache>,
}

impl SqliteLogStore {
    /// Week databases are opened on demand and kept warm for `keep_alive` after last use.
    pub fn new(
        root: impl AsRef<Path>,
        cache_size_kb: u64,
        busy_timeout: Duration,
        keep_alive: Duration,
    ) -> Self {
        let pools = Arc::new(PoolCache {
            cache_size_kb,
            busy_timeout,
            keep_alive,
            entries: Mutex::new(HashMap::new()),
            operation_locks: Mutex::new(HashMap::new()),
        });
        spawn_janitor(&pools);
        Self {
            root: root.as_ref().to_path_buf(),
            pools,
        }
    }
}

struct StoredLog {
    ts: i64,
    week: String,
    id: i64,
    line: LogLine,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    /// Newest first; backs the default page and `before` paging.
    Backward,
    /// Oldest first; backs `after` paging.
    Forward,
}

struct CachedPool {
    /// Kept behind a cell so a slow open never holds the cache lock.
    cell: Arc<OnceCell<SqlitePool>>,
    service: String,
    week: String,
    last_used: Instant,
}

struct PoolCache {
    cache_size_kb: u64,
    busy_timeout: Duration,
    keep_alive: Duration,
    entries: Mutex<HashMap<PathBuf, CachedPool>>,
    /// Kept independently from cached pools so eviction cannot race a new open for the path.
    operation_locks: Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>,
}

impl PoolCache {
    async fn lock_path(&self, path: &Path) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.operation_locks.lock().await;
            locks
                .entry(path.to_path_buf())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }

    async fn forget_path(&self, path: &Path) {
        self.operation_locks.lock().await.remove(path);
    }

    async fn pool(&self, path: &Path, service: &str, week: &str) -> AppResult<SqlitePool> {
        let cell = {
            let mut entries = self.entries.lock().await;
            let entry = entries
                .entry(path.to_path_buf())
                .or_insert_with(|| CachedPool {
                    cell: Arc::new(OnceCell::new()),
                    service: service.to_string(),
                    week: week.to_string(),
                    last_used: Instant::now(),
                });
            entry.last_used = Instant::now();
            entry.cell.clone()
        };
        let pool = cell
            .get_or_try_init(|| {
                open_database(path, service, week, self.cache_size_kb, self.busy_timeout)
            })
            .await?;
        Ok(pool.clone())
    }

    /// Must run before the backing file is unlinked, or the open descriptor keeps it alive.
    async fn evict(&self, path: &Path) {
        let entry = self.entries.lock().await.remove(path);
        if let Some(entry) = entry {
            close_entry(&entry).await;
        }
    }

    async fn sweep(&self) {
        let expired = {
            let mut entries = self.entries.lock().await;
            let keys: Vec<PathBuf> = entries
                .iter()
                .filter(|(_, entry)| entry.last_used.elapsed() >= self.keep_alive)
                .map(|(path, _)| path.clone())
                .collect();
            keys.iter()
                .filter_map(|key| entries.remove(key))
                .collect::<Vec<_>>()
        };
        for entry in expired {
            close_entry(&entry).await;
        }
    }

    async fn close_all(&self) {
        let entries = std::mem::take(&mut *self.entries.lock().await);
        for entry in entries.into_values() {
            close_entry(&entry).await;
        }
    }
}

fn spawn_janitor(cache: &Arc<PoolCache>) {
    let weak = Arc::downgrade(cache);
    let interval = (cache.keep_alive / 2).max(Duration::from_secs(1));
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let Some(cache) = weak.upgrade() else {
                return;
            };
            cache.sweep().await;
        }
    });
}

async fn close_entry(entry: &CachedPool) {
    if let Some(pool) = entry.cell.get() {
        pool.close().await;
        tracing::info!(
            service = %entry.service,
            week = %entry.week,
            "closed log database connection"
        );
    }
}

#[async_trait]
impl LogStore for SqliteLogStore {
    async fn database_size_bytes(&self) -> AppResult<u64> {
        let mut total = 0;
        let mut services = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(total),
            Err(error) => return Err(storage(error)),
        };

        while let Some(service) = services.next_entry().await.map_err(storage)? {
            if !service.file_type().await.map_err(storage)?.is_dir() {
                continue;
            }
            let mut databases = tokio::fs::read_dir(service.path()).await.map_err(storage)?;
            while let Some(database) = databases.next_entry().await.map_err(storage)? {
                let path = database.path();
                if path.extension().and_then(|value| value.to_str()) == Some("db") {
                    total += database.metadata().await.map_err(storage)?.len();
                }
            }
        }

        Ok(total)
    }

    async fn delete_before(&self, cutoff_ms: i64) -> AppResult<Vec<String>> {
        let mut removed = Vec::new();
        let mut services = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(removed),
            Err(error) => return Err(storage(error)),
        };

        while let Some(service) = services.next_entry().await.map_err(storage)? {
            if !service.file_type().await.map_err(storage)?.is_dir() {
                continue;
            }
            let service_name = service.file_name().to_string_lossy().into_owned();
            let mut databases = tokio::fs::read_dir(service.path()).await.map_err(storage)?;
            while let Some(database) = databases.next_entry().await.map_err(storage)? {
                let file_name = database.file_name().to_string_lossy().into_owned();
                let path = database.path();
                if path.extension().and_then(|value| value.to_str()) != Some("db")
                    || database_week_start_ms(&file_name).is_none_or(|start| start >= cutoff_ms)
                {
                    continue;
                }
                let _operation = self.pools.lock_path(&path).await;
                self.pools.evict(&path).await;
                tokio::fs::remove_file(&path).await.map_err(storage)?;
                self.pools.forget_path(&path).await;
                removed.push(format!("{service_name}/{file_name}"));
            }
        }

        removed.sort();
        Ok(removed)
    }

    async fn append(&self, service: &str, lines: Vec<LogLine>) -> AppResult<()> {
        let mut by_week = HashMap::<String, Vec<LogLine>>::new();
        for line in lines {
            if let Some(week) = week_database_name(line.ts) {
                by_week.entry(week).or_default().push(line);
            }
        }
        if by_week.is_empty() {
            return Ok(());
        }

        let service_dir = self.root.join(safe_service_path(service));
        tokio::fs::create_dir_all(&service_dir)
            .await
            .map_err(storage)?;

        for (week, lines) in by_week {
            let path = service_dir.join(&week);
            let _operation = self.pools.lock_path(&path).await;
            let pool = self.pools.pool(&path, service, &week).await?;
            let mut tx = pool.begin().await.map_err(storage)?;
            for line in lines {
                let level = detect_level(&line.line).map(level_name);
                sqlx::query(
                    "INSERT INTO logs (ts, cid, stream, level, line) VALUES (?, ?, ?, ?, ?)",
                )
                .bind(line.ts)
                .bind(line.cid.as_bytes().as_slice())
                .bind(stream_name(line.stream))
                .bind(level)
                .bind(line.line)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            }
            tx.commit().await.map_err(storage)?;
        }
        Ok(())
    }

    async fn query(&self, service: &str, filter: LogFilter) -> AppResult<LogPage> {
        let before = filter
            .before
            .as_deref()
            .map(decode_cursor)
            .transpose()
            .map_err(bad_cursor)?;
        let after = filter
            .after
            .as_deref()
            .map(decode_cursor)
            .transpose()
            .map_err(bad_cursor)?;
        let direction = if after.is_some() {
            Direction::Forward
        } else {
            Direction::Backward
        };
        let cursor = if after.is_some() {
            after.as_ref()
        } else {
            before.as_ref()
        };

        let service_dir = self.root.join(safe_service_path(service));
        let Some(weeks) = read_week_files(&service_dir).await? else {
            return Ok(empty_page(None));
        };

        let sanitized_query = filter.query.as_deref().and_then(sanitize_fts_query);
        let limit = filter.limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, 100) as usize;
        // The row past the page answers `has_more` in the scan direction without a second scan.
        let target = limit + 1;
        let mut page: Vec<StoredLog> = Vec::new();
        for week in ordered_candidates(&weeks, &filter, cursor, direction) {
            if page.len() >= target {
                break;
            }
            let path = service_dir.join(&week);
            let _operation = self.pools.lock_path(&path).await;
            let pool = self.pools.pool(&path, service, &week).await?;
            let mut builder = if sanitized_query.is_some() {
                QueryBuilder::<Sqlite>::new(
                    "SELECT logs.id, logs.ts, logs.cid, logs.stream, logs.level, logs.line \
                     FROM logs JOIN logs_fts ON logs_fts.rowid = logs.id WHERE logs_fts MATCH ",
                )
            } else {
                QueryBuilder::<Sqlite>::new(
                    "SELECT logs.id, logs.ts, logs.cid, logs.stream, logs.level, logs.line \
                     FROM logs WHERE 1 = 1",
                )
            };
            if let Some(query) = &sanitized_query {
                builder.push_bind(query);
            }
            push_filters(&mut builder, &filter);
            if let Some(cursor) = cursor {
                push_cursor(&mut builder, cursor, &week, direction);
            }
            builder.push(match direction {
                Direction::Backward => " ORDER BY ts DESC, id DESC LIMIT ",
                Direction::Forward => " ORDER BY ts ASC, id ASC LIMIT ",
            });
            builder.push_bind((target - page.len()) as i64);
            for row in builder.build().fetch_all(&pool).await.map_err(storage)? {
                if let Some(entry) = decode_row(&row, service, &week) {
                    page.push(entry);
                }
            }
        }

        let has_beyond = page.len() > limit;
        // The surplus row is last in scan order, so it has to go before the page is reordered.
        page.truncate(limit);
        if direction == Direction::Backward {
            page.reverse();
        }

        if page.is_empty() {
            // An `after` page that ran dry echoes its cursor back so clients can resume from it.
            return Ok(empty_page(filter.after));
        }

        let older_anchor = cursor_of(page.first().expect("non-empty page"));
        let newer_anchor = cursor_of(page.last().expect("non-empty page"));
        // The untouched side is only known via the cursor itself: its presence proves a
        // matching row existed there under the same filter (API contract requires clients to
        // discard cursors on filter change), so no extra probing query is needed.
        let (has_older, has_newer) = match direction {
            Direction::Backward => (has_beyond, filter.before.is_some()),
            Direction::Forward => (filter.after.is_some(), has_beyond),
        };

        Ok(LogPage {
            items: page.into_iter().map(|value| value.line).collect(),
            older_cursor: encode_cursor(&older_anchor).ok(),
            newer_cursor: encode_cursor(&newer_anchor).ok(),
            has_older,
            has_newer,
        })
    }

    async fn close(&self) {
        tracing::info!("closing logs database connections");
        self.pools.close_all().await;
    }
}

fn storage(error: impl std::fmt::Display) -> AppError {
    AppError::Storage(error.to_string())
}

fn bad_cursor(error: String) -> AppError {
    AppError::BadRequest(format!("invalid log cursor: {error}"))
}

fn empty_page(newer_cursor: Option<String>) -> LogPage {
    LogPage {
        items: Vec::new(),
        older_cursor: None,
        newer_cursor,
        has_older: false,
        has_newer: false,
    }
}

fn cursor_of(entry: &StoredLog) -> LogCursor {
    LogCursor {
        ts: entry.ts,
        week: entry.week.clone(),
        id: entry.id,
    }
}

fn stream_name(stream: LogStream) -> &'static str {
    match stream {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
    }
}

fn level_name(level: LogLevel) -> String {
    format!("{level:?}").to_ascii_lowercase()
}

/// Every `*.db` week file in `service_dir` with its week start, ascending. `None` when absent.
async fn read_week_files(service_dir: &Path) -> AppResult<Option<Vec<(String, i64)>>> {
    let mut entries = match tokio::fs::read_dir(service_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(storage(err)),
    };
    let mut weeks = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(storage)? {
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(start) = database_week_start_ms(&name) {
            weeks.push((name, start));
        }
    }
    weeks.sort_by_key(|(_, start)| *start);
    Ok(Some(weeks))
}

/// Week files that can hold a matching row, in the order `direction` visits them.
fn ordered_candidates(
    weeks: &[(String, i64)],
    filter: &LogFilter,
    cursor: Option<&LogCursor>,
    direction: Direction,
) -> Vec<String> {
    let mut selected: Vec<String> = weeks
        .iter()
        .filter(|(_, start)| week_matches(*start, filter, cursor, direction))
        .map(|(week, _)| week.clone())
        .collect();
    if direction == Direction::Backward {
        selected.reverse();
    }
    selected
}

fn week_matches(
    start: i64,
    filter: &LogFilter,
    cursor: Option<&LogCursor>,
    direction: Direction,
) -> bool {
    let end = start + WEEK_MS;
    if filter.to.is_some_and(|to| to < start) || filter.from.is_some_and(|from| from >= end) {
        return false;
    }
    match (cursor, direction) {
        // A week starting after the cursor holds only newer rows.
        (Some(cursor), Direction::Backward) => start <= cursor.ts,
        (Some(cursor), Direction::Forward) => end > cursor.ts,
        (None, _) => true,
    }
}

fn push_filters(builder: &mut QueryBuilder<Sqlite>, filter: &LogFilter) {
    if let Some(from) = filter.from {
        builder.push(" AND logs.ts >= ").push_bind(from);
    }
    if let Some(to) = filter.to {
        builder.push(" AND logs.ts <= ").push_bind(to);
    }
    if !filter.levels.is_empty() {
        builder.push(" AND logs.level IN (");
        for (index, level) in filter.levels.iter().enumerate() {
            if index > 0 {
                builder.push(", ");
            }
            builder.push_bind(level_name(*level));
        }
        builder.push(")");
    }
    if !filter.streams.is_empty() {
        builder.push(" AND logs.stream IN (");
        for (index, stream) in filter.streams.iter().enumerate() {
            if index > 0 {
                builder.push(", ");
            }
            builder.push_bind(stream_name(*stream));
        }
        builder.push(")");
    }
}

/// Week files partition the timeline, so only the cursor's own week needs the id tiebreak.
fn push_cursor(
    builder: &mut QueryBuilder<Sqlite>,
    cursor: &LogCursor,
    week: &str,
    direction: Direction,
) {
    if week != cursor.week {
        match direction {
            Direction::Backward => builder.push(" AND logs.ts <= ").push_bind(cursor.ts),
            Direction::Forward => builder.push(" AND logs.ts >= ").push_bind(cursor.ts),
        };
        return;
    }
    let comparison = match direction {
        Direction::Backward => "<",
        Direction::Forward => ">",
    };
    builder
        .push(" AND (logs.ts ")
        .push(comparison)
        .push(" ")
        .push_bind(cursor.ts)
        .push(" OR (logs.ts = ")
        .push_bind(cursor.ts)
        .push(" AND logs.id ")
        .push(comparison)
        .push(" ")
        .push_bind(cursor.id)
        .push("))");
}

fn decode_row(row: &SqliteRow, service: &str, week: &str) -> Option<StoredLog> {
    let stream = match row.get::<String, _>("stream").as_str() {
        "stdout" => LogStream::Stdout,
        "stderr" => LogStream::Stderr,
        _ => return None,
    };
    let level = row
        .get::<Option<String>, _>("level")
        .and_then(|value| match value.as_str() {
            "debug" => Some(LogLevel::Debug),
            "info" => Some(LogLevel::Info),
            "warn" => Some(LogLevel::Warn),
            "error" => Some(LogLevel::Error),
            _ => None,
        });
    let cid: Vec<u8> = row.get("cid");
    let cid = ContainerId::from_bytes(&cid)?;
    let ts = row.get("ts");
    Some(StoredLog {
        ts,
        week: week.to_string(),
        id: row.get("id"),
        line: LogLine {
            ts,
            service: service.to_string(),
            cid,
            stream,
            level,
            line: row.get("line"),
        },
    })
}

async fn open_database(
    path: &Path,
    service: &str,
    week: &str,
    cache_size_kb: u64,
    busy_timeout: Duration,
) -> AppResult<SqlitePool> {
    tracing::info!(
        service = %service,
        week = %week,
        "opening log database connection"
    );

    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .busy_timeout(busy_timeout)
        .foreign_keys(false)
        .statement_cache_capacity(STATEMENT_CACHE_CAPACITY)
        .analysis_limit(ANALYSIS_LIMIT)
        .optimize_on_close(true, ANALYSIS_LIMIT)
        // Negative values are interpreted as KiB rather than pages.
        .pragma("cache_size", format!("-{cache_size_kb}"));
    // Lifetime is owned by `PoolCache`, so sqlx's own idle reaping stays off.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(options)
        .await
        .map_err(storage)?;
    migrate(&pool).await?;
    Ok(pool)
}

async fn migrate(pool: &SqlitePool) -> AppResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY,
            ts INTEGER NOT NULL,
            cid BLOB NOT NULL,
            stream TEXT NOT NULL,
            level TEXT,
            line TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .map_err(storage)?;
    // Used for the default timeline queries: newest/oldest-first paging, time range filters,
    // and ordering by timestamp across a week database.
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_ts ON logs(ts)")
        .execute(pool)
        .await
        .map_err(storage)?;
    // Used when the frontend filters by level and then reads the matching rows in time order.
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_level_ts ON logs(level, ts)")
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
async fn initialize_fts(pool: &SqlitePool) -> AppResult<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::logs::LogFilter;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_store(name: &str) -> SqliteLogStore {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("vpsiner-logs-{name}-{suffix}"));
        SqliteLogStore::new(
            root,
            1_024,
            Duration::from_secs(5),
            Duration::from_secs(300),
        )
    }

    fn line(ts: i64, line: &str) -> LogLine {
        LogLine {
            ts,
            service: "group".into(),
            cid: ContainerId::parse("abc123abc123").unwrap(),
            stream: LogStream::Stdout,
            level: None,
            line: line.to_string(),
        }
    }

    /// Three timestamps in three consecutive ISO weeks, oldest first.
    fn weekly_timestamps() -> [i64; 3] {
        let base = 1_700_000_000_000;
        [base - 2 * WEEK_MS, base - WEEK_MS, base]
    }

    #[tokio::test]
    async fn stores_and_returns_line() {
        let store = test_store("line");
        let plain = line(1_700_000_000_000, "plain message");
        store.append("group", vec![plain]).await.unwrap();

        let page = store.query("group", LogFilter::default()).await.unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].line, "plain message");
    }

    #[tokio::test]
    async fn text_search_matches_line() {
        let store = test_store("search");
        let colored = line(1_700_000_000_000, "duration_ms=42");
        store.append("group", vec![colored]).await.unwrap();

        let filter = LogFilter {
            query: Some("duration_ms=42".to_string()),
            ..Default::default()
        };
        let page = store.query("group", filter).await.unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].line, "duration_ms=42");
    }

    #[tokio::test]
    async fn text_search_supports_or_tokens_and_phrases() {
        let store = test_store("fts-operators");
        store
            .append(
                "group",
                vec![
                    line(1_700_000_000_000, "connection timeout"),
                    line(1_700_000_000_001, "connection refused"),
                    line(1_700_000_000_002, "healthy response"),
                ],
            )
            .await
            .unwrap();

        let page = store
            .query(
                "group",
                LogFilter {
                    query: Some(r#""connection timeout" refused"#.to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].line, "connection timeout");
        assert_eq!(page.items[1].line, "connection refused");
    }

    #[tokio::test]
    async fn text_search_auto_closes_unclosed_quotes_and_sanitizes_syntax() {
        let store = test_store("fts-syntax");
        store
            .append("group", vec![line(1_700_000_000_000, "connection timeout")])
            .await
            .unwrap();

        let page = store
            .query(
                "group",
                LogFilter {
                    query: Some(r#""connection timeout"#.to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].line, "connection timeout");

        let page_or = store
            .query(
                "group",
                LogFilter {
                    query: Some("timeout OR".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(page_or.items.len(), 1);
        assert_eq!(page_or.items[0].line, "connection timeout");
    }

    #[tokio::test]
    async fn trigram_matches_substring_across_punctuation() {
        let store = test_store("fts-trigram-substring");
        store
            .append(
                "group",
                vec![
                    line(1_700_000_000_000, "duration_ms=42"),
                    line(1_700_000_000_001, "duration ms=42"),
                ],
            )
            .await
            .unwrap();

        let page = store
            .query(
                "group",
                LogFilter {
                    query: Some("\"duration_ms\"".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].line, "duration_ms=42");
    }

    #[tokio::test]
    async fn default_page_returns_newest_across_weeks() {
        let store = test_store("newest");
        let [old, middle, new] = weekly_timestamps();
        store
            .append(
                "group",
                vec![line(old, "old"), line(middle, "middle"), line(new, "new")],
            )
            .await
            .unwrap();

        let filter = LogFilter {
            limit: Some(1),
            ..Default::default()
        };
        let page = store.query("group", filter).await.unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].line, "new");
        assert!(page.has_older);
        assert!(!page.has_newer);
    }

    #[tokio::test]
    async fn time_range_selects_a_single_week() {
        let store = test_store("range");
        let [old, middle, new] = weekly_timestamps();
        store
            .append(
                "group",
                vec![line(old, "old"), line(middle, "middle"), line(new, "new")],
            )
            .await
            .unwrap();

        let filter = LogFilter {
            from: Some(middle - 1),
            to: Some(middle + 1),
            ..Default::default()
        };
        let page = store.query("group", filter).await.unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].line, "middle");
        assert!(!page.has_older);
        assert!(!page.has_newer);
    }

    #[tokio::test]
    async fn pages_backward_then_forward_without_gaps() {
        let store = test_store("paging");
        let [old, middle, new] = weekly_timestamps();
        store
            .append(
                "group",
                vec![line(old, "old"), line(middle, "middle"), line(new, "new")],
            )
            .await
            .unwrap();

        let newest = store
            .query(
                "group",
                LogFilter {
                    limit: Some(1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let older = store
            .query(
                "group",
                LogFilter {
                    limit: Some(1),
                    before: newest.older_cursor.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(older.items[0].line, "middle");
        assert!(older.has_older);
        assert!(older.has_newer);

        let forward = store
            .query(
                "group",
                LogFilter {
                    limit: Some(1),
                    after: older.newer_cursor.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(forward.items[0].line, "new");
        assert!(!forward.has_newer);
    }

    #[tokio::test]
    async fn exhausted_forward_page_echoes_its_cursor() {
        let store = test_store("exhausted");
        store
            .append("group", vec![line(1_700_000_000_000, "only")])
            .await
            .unwrap();
        let page = store.query("group", LogFilter::default()).await.unwrap();

        let next = store
            .query(
                "group",
                LogFilter {
                    after: page.newer_cursor.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(next.items.is_empty());
        assert_eq!(next.newer_cursor, page.newer_cursor);
    }

    #[tokio::test]
    async fn delete_before_drops_cached_pool() {
        let store = test_store("retention");
        let ts = 1_700_000_000_000;
        store
            .append("group", vec![line(ts, "doomed")])
            .await
            .unwrap();
        assert_eq!(
            store
                .query("group", LogFilter::default())
                .await
                .unwrap()
                .items
                .len(),
            1
        );

        let removed = store.delete_before(ts + WEEK_MS).await.unwrap();

        assert_eq!(removed.len(), 1);
        assert!(
            store
                .query("group", LogFilter::default())
                .await
                .unwrap()
                .items
                .is_empty()
        );
    }

    #[tokio::test]
    async fn delete_before_waits_for_an_active_week_operation() {
        let store = Arc::new(test_store("retention-lock"));
        let ts = 1_700_000_000_000;
        store
            .append("group", vec![line(ts, "doomed")])
            .await
            .unwrap();
        let path = store
            .root
            .join(safe_service_path("group"))
            .join(week_database_name(ts).unwrap());

        let operation = store.pools.lock_path(&path).await;
        let deletion_store = store.clone();
        let mut deletion =
            tokio::spawn(async move { deletion_store.delete_before(ts + WEEK_MS).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut deletion)
                .await
                .is_err()
        );
        assert!(path.exists());

        drop(operation);
        assert_eq!(deletion.await.unwrap().unwrap().len(), 1);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn concurrent_stores_initialize_fts_idempotently() {
        let root = std::env::temp_dir().join(format!(
            "vpsiner-logs-fts-concurrent-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let first = SqliteLogStore::new(&root, 1_024, Duration::from_secs(5), Duration::ZERO);
        let second = SqliteLogStore::new(&root, 1_024, Duration::from_secs(5), Duration::ZERO);
        let ts = 1_700_000_000_000;

        let (first_result, second_result) = tokio::join!(
            first.append("group", vec![line(ts, "first")]),
            second.append("group", vec![line(ts + 1, "second")]),
        );

        first_result.unwrap();
        second_result.unwrap();
        let page = first.query("group", LogFilter::default()).await.unwrap();
        assert_eq!(page.items.len(), 2);
    }
}
