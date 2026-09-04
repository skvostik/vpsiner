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

impl LogStream {
    pub(crate) fn storage_code(self) -> i64 {
        match self {
            Self::Stdout => 0,
            Self::Stderr => 1,
        }
    }

    pub(crate) fn from_storage_code(code: i64) -> Option<Self> {
        match code {
            0 => Some(Self::Stdout),
            1 => Some(Self::Stderr),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub(crate) fn storage_code(self) -> i64 {
        match self {
            Self::Debug => 20,
            Self::Info => 30,
            Self::Warn => 40,
            Self::Error => 50,
        }
    }

    pub(crate) fn from_storage_code(code: i64) -> Option<Self> {
        match code {
            20 => Some(Self::Debug),
            30 => Some(Self::Info),
            40 => Some(Self::Warn),
            50 => Some(Self::Error),
            _ => None,
        }
    }
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
