use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::container_id::ContainerId;
use super::time::TimestampMs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    pub ts: TimestampMs,
    pub service: String,
    pub cid: ContainerId,
    pub stream: LogStream,
    pub level: Option<LogLevel>,
    /// Log text with ANSI escape codes stripped.
    pub line: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogCursor {
    pub ts: i64,
    pub week: String,
    pub id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogPage {
    pub items: Vec<LogLine>,
    pub older_cursor: Option<String>,
    pub newer_cursor: Option<String>,
    pub has_older: bool,
    pub has_newer: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceStatus {
    pub last_received: Option<TimestampMs>,
    pub live: bool,
}

/// Incremental update for `/api/stream/logs`, relative to what a client has already seen.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceDiff {
    pub added: BTreeMap<String, ServiceStatus>,
    pub updated: BTreeMap<String, ServiceStatus>,
    pub removed: Vec<String>,
}

/// One batch of newly-flushed lines pushed by `/api/stream/logs/{service}`, carrying the
/// cursor to resume from so clients can keep their own pagination cursors consistent.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogTailAppend {
    pub items: Vec<LogLine>,
    pub newer_cursor: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogFilter {
    pub from: Option<TimestampMs>,
    pub to: Option<TimestampMs>,
    pub query: Option<String>,
    pub levels: Vec<LogLevel>,
    pub streams: Vec<LogStream>,
    pub limit: Option<u32>,
    pub before: Option<String>,
    pub after: Option<String>,
}
