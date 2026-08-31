use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::{
    QueryBuilder, Row, Sqlite, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
};
use tokio::sync::{Mutex, OnceCell};

use super::{
    database_week_start_ms, decode_cursor, detect_level, encode_cursor, safe_group_path,
    week_database_name,
};
use crate::error::{AppError, AppResult};
use crate::model::{LogCursor, LogFilter, LogLevel, LogLine, LogPage, LogStream};

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    async fn database_size_bytes(&self) -> AppResult<u64>;

    async fn delete_before(&self, cutoff_ms: i64) -> AppResult<Vec<String>>;

    async fn append(&self, log_group: &str, lines: Vec<LogLine>) -> AppResult<()>;
    async fn query(&self, log_group: &str, filter: LogFilter) -> AppResult<LogPage>;

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
        });
        spawn_janitor(&pools);
        Self {
            root: root.as_ref().to_path_buf(),
            pools,
        }
    }

    /// Whether any row beyond `anchor` in `direction` still matches `filter`.
    async fn exists_beyond(
        &self,
        group_dir: &Path,
        log_group: &str,
        weeks: &[(String, i64)],
        filter: &LogFilter,
        anchor: &LogCursor,
        direction: Direction,
    ) -> AppResult<bool> {
        for week in ordered_candidates(weeks, filter, Some(anchor), direction) {
            let pool = self
                .pools
                .pool(&group_dir.join(&week), log_group, &week)
                .await?;
            let mut builder = QueryBuilder::<Sqlite>::new("SELECT 1 FROM logs WHERE 1 = 1");
            push_filters(&mut builder, filter);
            push_cursor(&mut builder, anchor, &week, direction);
            builder.push(" LIMIT 1");
            if !builder
                .build()
                .fetch_all(&pool)
                .await
                .map_err(storage)?
                .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
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
    log_group: String,
    week: String,
    last_used: Instant,
}

struct PoolCache {
    cache_size_kb: u64,
    busy_timeout: Duration,
    keep_alive: Duration,
    entries: Mutex<HashMap<PathBuf, CachedPool>>,
}

impl PoolCache {
    async fn pool(&self, path: &Path, log_group: &str, week: &str) -> AppResult<SqlitePool> {
        let cell = {
            let mut entries = self.entries.lock().await;
            let entry = entries
                .entry(path.to_path_buf())
                .or_insert_with(|| CachedPool {
                    cell: Arc::new(OnceCell::new()),
                    log_group: log_group.to_string(),
                    week: week.to_string(),
                    last_used: Instant::now(),
                });
            entry.last_used = Instant::now();
            entry.cell.clone()
        };
        let pool = cell
            .get_or_try_init(|| {
                open_database(path, log_group, week, self.cache_size_kb, self.busy_timeout)
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
            log_group = %entry.log_group,
            week = %entry.week,
            "closed log database connection"
        );
    }
}

#[async_trait]
impl LogStore for SqliteLogStore {
    async fn database_size_bytes(&self) -> AppResult<u64> {
        let mut total = 0;
        let mut groups = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(total),
            Err(error) => return Err(storage(error)),
        };

        while let Some(group) = groups.next_entry().await.map_err(storage)? {
            if !group.file_type().await.map_err(storage)?.is_dir() {
                continue;
            }
            let mut databases = tokio::fs::read_dir(group.path()).await.map_err(storage)?;
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
        let mut groups = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(removed),
            Err(error) => return Err(storage(error)),
        };

        while let Some(group) = groups.next_entry().await.map_err(storage)? {
            if !group.file_type().await.map_err(storage)?.is_dir() {
                continue;
            }
            let group_name = group.file_name().to_string_lossy().into_owned();
            let mut databases = tokio::fs::read_dir(group.path()).await.map_err(storage)?;
            while let Some(database) = databases.next_entry().await.map_err(storage)? {
                let file_name = database.file_name().to_string_lossy().into_owned();
                let path = database.path();
                if path.extension().and_then(|value| value.to_str()) != Some("db")
                    || database_week_start_ms(&file_name).is_none_or(|start| start >= cutoff_ms)
                {
                    continue;
                }
                self.pools.evict(&path).await;
                tokio::fs::remove_file(&path).await.map_err(storage)?;
                removed.push(format!("{group_name}/{file_name}"));
            }
        }

        removed.sort();
        Ok(removed)
    }

    async fn append(&self, log_group: &str, lines: Vec<LogLine>) -> AppResult<()> {
        let mut by_week = HashMap::<String, Vec<LogLine>>::new();
        for line in lines {
            if let Some(week) = week_database_name(line.ts) {
                by_week.entry(week).or_default().push(line);
            }
        }
        if by_week.is_empty() {
            return Ok(());
        }

        let group_dir = self.root.join(safe_group_path(log_group));
        tokio::fs::create_dir_all(&group_dir)
            .await
            .map_err(storage)?;

        for (week, lines) in by_week {
            let pool = self
                .pools
                .pool(&group_dir.join(&week), log_group, &week)
                .await?;
            let mut tx = pool.begin().await.map_err(storage)?;
            for line in lines {
                let level = detect_level(&line.line).map(level_name);
                sqlx::query(
                    "INSERT INTO logs (ts, cid, stream, level, line) VALUES (?, ?, ?, ?, ?)",
                )
                .bind(line.ts)
                .bind(line.cid)
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

    async fn query(&self, log_group: &str, filter: LogFilter) -> AppResult<LogPage> {
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

        let group_dir = self.root.join(safe_group_path(log_group));
        let Some(weeks) = read_week_files(&group_dir).await? else {
            return Ok(empty_page(None));
        };

        let limit = filter.limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, 100) as usize;
        // The row past the page answers `has_more` in the scan direction without a second scan.
        let target = limit + 1;
        let mut page: Vec<StoredLog> = Vec::new();
        for week in ordered_candidates(&weeks, &filter, cursor, direction) {
            if page.len() >= target {
                break;
            }
            let pool = self
                .pools
                .pool(&group_dir.join(&week), log_group, &week)
                .await?;
            let mut builder = QueryBuilder::<Sqlite>::new(
                "SELECT id, ts, cid, stream, level, line FROM logs WHERE 1 = 1",
            );
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
                if let Some(entry) = decode_row(&row, log_group, &week) {
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
        // Only the side the scan did not cover still needs probing.
        let (has_older, has_newer) = match direction {
            Direction::Backward => (
                has_beyond,
                self.exists_beyond(
                    &group_dir,
                    log_group,
                    &weeks,
                    &filter,
                    &newer_anchor,
                    Direction::Forward,
                )
                .await?,
            ),
            Direction::Forward => (
                self.exists_beyond(
                    &group_dir,
                    log_group,
                    &weeks,
                    &filter,
                    &older_anchor,
                    Direction::Backward,
                )
                .await?,
                has_beyond,
            ),
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

/// Every `*.db` week file in `group_dir` with its week start, ascending. `None` when absent.
async fn read_week_files(group_dir: &Path) -> AppResult<Option<Vec<(String, i64)>>> {
    let mut entries = match tokio::fs::read_dir(group_dir).await {
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
        builder.push(" AND ts >= ").push_bind(from);
    }
    if let Some(to) = filter.to {
        builder.push(" AND ts <= ").push_bind(to);
    }
    if let Some(text) = &filter.query {
        builder
            .push(" AND line LIKE ")
            .push_bind(format!("%{text}%"));
    }
    if !filter.levels.is_empty() {
        builder.push(" AND level IN (");
        for (index, level) in filter.levels.iter().enumerate() {
            if index > 0 {
                builder.push(", ");
            }
            builder.push_bind(level_name(*level));
        }
        builder.push(")");
    }
    if !filter.streams.is_empty() {
        builder.push(" AND stream IN (");
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
            Direction::Backward => builder.push(" AND ts <= ").push_bind(cursor.ts),
            Direction::Forward => builder.push(" AND ts >= ").push_bind(cursor.ts),
        };
        return;
    }
    let comparison = match direction {
        Direction::Backward => "<",
        Direction::Forward => ">",
    };
    builder
        .push(" AND (ts ")
        .push(comparison)
        .push(" ")
        .push_bind(cursor.ts)
        .push(" OR (ts = ")
        .push_bind(cursor.ts)
        .push(" AND id ")
        .push(comparison)
        .push(" ")
        .push_bind(cursor.id)
        .push("))");
}

fn decode_row(row: &SqliteRow, log_group: &str, week: &str) -> Option<StoredLog> {
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
    let ts = row.get("ts");
    Some(StoredLog {
        ts,
        week: week.to_string(),
        id: row.get("id"),
        line: LogLine {
            ts,
            log_group: log_group.to_string(),
            cid: row.get("cid"),
            stream,
            level,
            line: row.get("line"),
        },
    })
}

async fn open_database(
    path: &Path,
    log_group: &str,
    week: &str,
    cache_size_kb: u64,
    busy_timeout: Duration,
) -> AppResult<SqlitePool> {
    tracing::info!(
        log_group = %log_group,
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
            cid TEXT NOT NULL DEFAULT '',
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::LogFilter;
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
            log_group: "group".into(),
            cid: "abc123".into(),
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
}
