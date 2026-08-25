use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sqlx::{
    QueryBuilder, Row, Sqlite, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

use super::{
    LogCursor, database_week_start_ms, decode_cursor, detect_level, encode_cursor, safe_group_path,
    week_database_name,
};
use crate::error::{AppError, AppResult};
use crate::model::{LogFilter, LogGroupSummary, LogLine, LogPage, LogStream};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogResumeBoundary {
    pub ts: i64,
    pub occurrences: HashMap<(LogStream, String), usize>,
}

#[allow(dead_code)]
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    async fn database_size_bytes(&self) -> AppResult<u64>;

    async fn delete_before(&self, cutoff_ms: i64) -> AppResult<Vec<String>>;

    async fn append(&self, log_group: &str, lines: Vec<LogLine>) -> AppResult<()>;
    async fn query(&self, log_group: &str, filter: LogFilter) -> AppResult<LogPage>;
    async fn list_groups(&self) -> AppResult<Vec<LogGroupSummary>>;
    async fn resume_boundary(
        &self,
        log_group: &str,
        container_id: &str,
    ) -> AppResult<Option<LogResumeBoundary>>;
}

pub struct SqliteLogStore {
    root: PathBuf,
}

impl SqliteLogStore {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }
}

struct StoredLog {
    ts: i64,
    week: String,
    id: i64,
    line: LogLine,
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
                tokio::fs::remove_file(path).await.map_err(storage)?;
                removed.push(format!("{group_name}/{file_name}"));
            }
        }

        removed.sort();
        Ok(removed)
    }

    async fn append(&self, log_group: &str, lines: Vec<LogLine>) -> AppResult<()> {
        let mut by_week = std::collections::HashMap::<String, Vec<LogLine>>::new();
        for line in lines {
            if let Some(week) = week_database_name(line.ts) {
                by_week.entry(week).or_default().push(line);
            }
        }
        for (week, lines) in by_week {
            let group_dir = self.root.join(safe_group_path(log_group));
            tokio::fs::create_dir_all(&group_dir)
                .await
                .map_err(storage)?;
            let pool = open_pool(group_dir.join(week)).await?;
            let mut tx = pool.begin().await.map_err(storage)?;
            for line in lines {
                let level =
                    detect_level(&line.line).map(|value| format!("{value:?}").to_ascii_lowercase());
                sqlx::query(
                    "INSERT INTO logs (ts, cid, stream, level, line) VALUES (?, ?, ?, ?, ?)",
                )
                .bind(line.ts)
                .bind(line.cid)
                .bind(match line.stream {
                    crate::model::LogStream::Stdout => "stdout",
                    crate::model::LogStream::Stderr => "stderr",
                })
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
        let before_cursor = filter
            .before
            .as_deref()
            .map(decode_cursor)
            .transpose()
            .map_err(|err| AppError::BadRequest(format!("invalid log cursor: {err}")))?;
        let after_cursor = filter
            .after
            .as_deref()
            .map(decode_cursor)
            .transpose()
            .map_err(|err| AppError::BadRequest(format!("invalid log cursor: {err}")))?;
        let group_dir = self.root.join(safe_group_path(log_group));
        let mut entries = match tokio::fs::read_dir(group_dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(LogPage {
                    items: Vec::new(),
                    older_cursor: None,
                    newer_cursor: None,
                    has_older: false,
                    has_newer: false,
                });
            }
            Err(err) => return Err(storage(err)),
        };
        let limit = filter.limit.unwrap_or(100).clamp(1, 100) as i64;
        let mut all = Vec::new();
        while let Some(entry) = entries.next_entry().await.map_err(storage)? {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("db") {
                continue;
            }
            let week = entry.file_name().to_string_lossy().into_owned();
            let pool = open_pool(path).await?;
            let mut query = QueryBuilder::<Sqlite>::new(
                "SELECT id, ts, cid, stream, level, line FROM logs WHERE 1 = 1",
            );
            if let Some(from) = filter.from {
                query.push(" AND ts >= ").push_bind(from);
            }
            if let Some(to) = filter.to {
                query.push(" AND ts <= ").push_bind(to);
            }
            if let Some(text) = &filter.query {
                query.push(" AND line LIKE ").push_bind(format!("%{text}%"));
            }
            if !filter.levels.is_empty() {
                query.push(" AND level IN (");
                for (index, level) in filter.levels.iter().enumerate() {
                    if index > 0 {
                        query.push(", ");
                    }
                    query.push_bind(format!("{level:?}").to_ascii_lowercase());
                }
                query.push(")");
            }
            if !filter.streams.is_empty() {
                query.push(" AND stream IN (");
                for (index, stream) in filter.streams.iter().enumerate() {
                    if index > 0 {
                        query.push(", ");
                    }
                    query.push_bind(match stream {
                        crate::model::LogStream::Stdout => "stdout",
                        crate::model::LogStream::Stderr => "stderr",
                    });
                }
                query.push(")");
            }
            query.push(" ORDER BY ts ASC, id ASC");
            for row in query.build().fetch_all(&pool).await.map_err(storage)? {
                let stream = match row.get::<String, _>("stream").as_str() {
                    "stdout" => crate::model::LogStream::Stdout,
                    "stderr" => crate::model::LogStream::Stderr,
                    _ => continue,
                };
                let level =
                    row.get::<Option<String>, _>("level")
                        .and_then(|value| match value.as_str() {
                            "debug" => Some(crate::model::LogLevel::Debug),
                            "info" => Some(crate::model::LogLevel::Info),
                            "warn" => Some(crate::model::LogLevel::Warn),
                            "error" => Some(crate::model::LogLevel::Error),
                            _ => None,
                        });
                let ts = row.get("ts");
                let id = row.get("id");
                let line: String = row.get("line");
                all.push(StoredLog {
                    ts,
                    week: week.clone(),
                    id,
                    line: LogLine {
                        ts,
                        log_group: log_group.to_string(),
                        cid: row.get("cid"),
                        stream,
                        level,
                        line,
                    },
                });
            }
        }
        all.sort_by(|left, right| {
            left.ts
                .cmp(&right.ts)
                .then_with(|| left.week.cmp(&right.week))
                .then_with(|| left.id.cmp(&right.id))
        });

        let key_of = |entry: &StoredLog| LogCursor {
            ts: entry.ts,
            week: entry.week.clone(),
            id: entry.id,
        };
        let cmp_key = |entry: &StoredLog, cursor: &LogCursor| {
            entry
                .ts
                .cmp(&cursor.ts)
                .then_with(|| entry.week.cmp(&cursor.week))
                .then_with(|| entry.id.cmp(&cursor.id))
        };

        let mut filtered: Vec<StoredLog> = if let Some(before) = &before_cursor {
            all.into_iter()
                .filter(|entry| cmp_key(entry, before).is_lt())
                .collect()
        } else if let Some(after) = &after_cursor {
            all.into_iter()
                .filter(|entry| cmp_key(entry, after).is_gt())
                .collect()
        } else {
            all
        };

        if after_cursor.is_some() && filtered.is_empty() {
            return Ok(LogPage {
                items: Vec::new(),
                older_cursor: None,
                newer_cursor: filter.after,
                has_older: false,
                has_newer: false,
            });
        }

        let take = limit as usize;
        if after_cursor.is_some() {
            filtered.truncate(take);
        } else if filtered.len() > take {
            filtered = filtered.split_off(filtered.len() - take);
        }

        if filtered.is_empty() {
            return Ok(LogPage {
                items: Vec::new(),
                older_cursor: None,
                newer_cursor: None,
                has_older: false,
                has_newer: false,
            });
        }

        let older_anchor = key_of(filtered.first().expect("non-empty page"));
        let newer_anchor = key_of(filtered.last().expect("non-empty page"));
        let older_cursor = encode_cursor(&older_anchor).ok();
        let newer_cursor = encode_cursor(&newer_anchor).ok();

        let all_rows = {
            let group_dir = self.root.join(safe_group_path(log_group));
            let mut entries = tokio::fs::read_dir(group_dir).await.map_err(storage)?;
            let mut rows = Vec::new();
            while let Some(entry) = entries.next_entry().await.map_err(storage)? {
                let path = entry.path();
                if path.extension().and_then(|value| value.to_str()) != Some("db") {
                    continue;
                }
                let week = entry.file_name().to_string_lossy().into_owned();
                let pool = open_pool(path).await?;
                let mut query = QueryBuilder::<Sqlite>::new("SELECT id, ts FROM logs WHERE 1 = 1");
                if let Some(from) = filter.from {
                    query.push(" AND ts >= ").push_bind(from);
                }
                if let Some(to) = filter.to {
                    query.push(" AND ts <= ").push_bind(to);
                }
                if let Some(text) = &filter.query {
                    query.push(" AND line LIKE ").push_bind(format!("%{text}%"));
                }
                if !filter.levels.is_empty() {
                    query.push(" AND level IN (");
                    for (index, level) in filter.levels.iter().enumerate() {
                        if index > 0 {
                            query.push(", ");
                        }
                        query.push_bind(format!("{level:?}").to_ascii_lowercase());
                    }
                    query.push(")");
                }
                if !filter.streams.is_empty() {
                    query.push(" AND stream IN (");
                    for (index, stream) in filter.streams.iter().enumerate() {
                        if index > 0 {
                            query.push(", ");
                        }
                        query.push_bind(match stream {
                            crate::model::LogStream::Stdout => "stdout",
                            crate::model::LogStream::Stderr => "stderr",
                        });
                    }
                    query.push(")");
                }
                for row in query.build().fetch_all(&pool).await.map_err(storage)? {
                    rows.push(LogCursor {
                        ts: row.get("ts"),
                        week: week.clone(),
                        id: row.get("id"),
                    });
                }
            }
            rows
        };

        let has_older = all_rows.iter().any(|entry| {
            entry
                .ts
                .cmp(&older_anchor.ts)
                .then_with(|| entry.week.cmp(&older_anchor.week))
                .then_with(|| entry.id.cmp(&older_anchor.id))
                .is_lt()
        });
        let has_newer = all_rows.iter().any(|entry| {
            entry
                .ts
                .cmp(&newer_anchor.ts)
                .then_with(|| entry.week.cmp(&newer_anchor.week))
                .then_with(|| entry.id.cmp(&newer_anchor.id))
                .is_gt()
        });

        Ok(LogPage {
            items: filtered.into_iter().map(|value| value.line).collect(),
            older_cursor,
            newer_cursor,
            has_older,
            has_newer,
        })
    }

    async fn list_groups(&self) -> AppResult<Vec<LogGroupSummary>> {
        let mut groups = Vec::new();
        let mut entries = match tokio::fs::read_dir(&self.root).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(groups),
            Err(err) => return Err(storage(err)),
        };
        while let Some(entry) = entries.next_entry().await.map_err(storage)? {
            if entry.file_type().await.map_err(storage)?.is_dir() {
                let log_group = entry.file_name().to_string_lossy().into_owned();
                let last_received = self.last_received(&entry.path()).await?;
                groups.push(LogGroupSummary {
                    log_group,
                    last_received,
                });
            }
        }
        groups.sort_by(|left, right| left.log_group.cmp(&right.log_group));
        Ok(groups)
    }

    async fn resume_boundary(
        &self,
        log_group: &str,
        container_id: &str,
    ) -> AppResult<Option<LogResumeBoundary>> {
        let group_dir = self.root.join(safe_group_path(log_group));
        let mut entries = match tokio::fs::read_dir(group_dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(storage(error)),
        };
        let mut databases = Vec::new();
        let mut latest = None;
        while let Some(entry) = entries.next_entry().await.map_err(storage)? {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("db") {
                continue;
            }
            let pool = open_pool(path).await?;
            let row = sqlx::query("SELECT MAX(ts) AS max_ts FROM logs WHERE cid = ?")
                .bind(container_id)
                .fetch_one(&pool)
                .await
                .map_err(storage)?;
            if let Some(ts) = row.try_get::<Option<i64>, _>("max_ts").map_err(storage)? {
                latest = Some(latest.map_or(ts, |current: i64| current.max(ts)));
            }
            databases.push(pool);
        }
        let Some(ts) = latest else {
            return Ok(None);
        };
        let mut occurrences = HashMap::new();
        for pool in databases {
            let rows = sqlx::query("SELECT stream, line FROM logs WHERE cid = ? AND ts = ?")
                .bind(container_id)
                .bind(ts)
                .fetch_all(&pool)
                .await
                .map_err(storage)?;
            for row in rows {
                let stream = match row
                    .try_get::<String, _>("stream")
                    .map_err(storage)?
                    .as_str()
                {
                    "stderr" => LogStream::Stderr,
                    _ => LogStream::Stdout,
                };
                let line = row.try_get::<String, _>("line").map_err(storage)?;
                *occurrences.entry((stream, line)).or_default() += 1;
            }
        }
        Ok(Some(LogResumeBoundary { ts, occurrences }))
    }
}

impl SqliteLogStore {
    /// Latest `ts` across all weekly databases in a log group directory.
    async fn last_received(&self, group_dir: &Path) -> AppResult<Option<i64>> {
        let mut entries = tokio::fs::read_dir(group_dir).await.map_err(storage)?;
        let mut last: Option<i64> = None;
        while let Some(entry) = entries.next_entry().await.map_err(storage)? {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("db") {
                continue;
            }
            let pool = open_pool(path).await?;
            let row = sqlx::query("SELECT MAX(ts) as max_ts FROM logs")
                .fetch_one(&pool)
                .await
                .map_err(storage)?;
            if let Some(max_ts) = row.try_get::<Option<i64>, _>("max_ts").map_err(storage)? {
                last = Some(last.map_or(max_ts, |current| current.max(max_ts)));
            }
        }
        Ok(last)
    }
}

fn storage(error: impl std::fmt::Display) -> AppError {
    AppError::Storage(error.to_string())
}

async fn open_pool(path: PathBuf) -> AppResult<SqlitePool> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(storage)?;
    sqlx::query("CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, cid TEXT NOT NULL DEFAULT '', stream TEXT NOT NULL, level TEXT, line TEXT NOT NULL)")
        .execute(&pool)
        .await
        .map_err(storage)?;
    for statement in [
        "ALTER TABLE logs ADD COLUMN cid TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE logs ADD COLUMN line TEXT",
    ] {
        if let Err(err) = sqlx::query(statement).execute(&pool).await
            && !err.to_string().contains("duplicate column name")
        {
            return Err(storage(err));
        }
    }
    sqlx::query("UPDATE logs SET line = COALESCE(line, '')")
        .execute(&pool)
        .await
        .map_err(storage)?;
    if let Err(err) = sqlx::query("UPDATE logs SET line = COALESCE(line, sanitized)")
        .execute(&pool)
        .await
        && !err.to_string().contains("no such column: sanitized")
    {
        return Err(storage(err));
    }
    if let Err(err) = sqlx::query("UPDATE logs SET line = COALESCE(line, raw)")
        .execute(&pool)
        .await
        && !err.to_string().contains("no such column: raw")
    {
        return Err(storage(err));
    }
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_ts ON logs(ts)")
        .execute(&pool)
        .await
        .map_err(storage)?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_cid_ts ON logs(cid, ts)")
        .execute(&pool)
        .await
        .map_err(storage)?;
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{LogFilter, LogStream};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_directory(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vpsiner-logs-{name}-{suffix}"))
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

    #[tokio::test]
    async fn stores_and_returns_line() {
        let store = SqliteLogStore::new(test_directory("line"));
        let plain = line(1_700_000_000_000, "plain message");
        store.append("group", vec![plain]).await.unwrap();

        let page = store.query("group", LogFilter::default()).await.unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].line, "plain message");
    }

    #[tokio::test]
    async fn text_search_matches_line() {
        let store = SqliteLogStore::new(test_directory("search"));
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
    async fn resume_boundary_is_scoped_to_container() {
        let store = SqliteLogStore::new(test_directory("resume-container"));
        let first = line(1_700_000_000_000, "repeated");
        let mut second = first.clone();
        let mut other = line(1_700_000_001_000, "other container");
        second.stream = LogStream::Stderr;
        other.cid = "def456".into();
        store
            .append("group", vec![first.clone(), first.clone(), second, other])
            .await
            .unwrap();

        let boundary = store
            .resume_boundary("group", "abc123")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(boundary.ts, first.ts);
        assert_eq!(
            boundary
                .occurrences
                .get(&(LogStream::Stdout, "repeated".into())),
            Some(&2)
        );
        assert_eq!(
            boundary
                .occurrences
                .get(&(LogStream::Stderr, "repeated".into())),
            Some(&1)
        );
    }

    #[tokio::test]
    async fn resume_boundary_is_none_without_container_history() {
        let store = SqliteLogStore::new(test_directory("resume-empty"));

        assert_eq!(
            store.resume_boundary("group", "missing").await.unwrap(),
            None
        );
    }
}
