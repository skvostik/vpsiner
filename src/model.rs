//! Domain types shared across the service boundaries.
//! Nothing from bollard, sqlx or sysinfo may leak through a trait — it is mapped here first.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Unix timestamp in milliseconds.
pub type TimestampMs = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContainerState {
    Created,
    Restarting,
    Running,
    Removing,
    Paused,
    Exited,
    Dead,
    Stopping,
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerSummary {
    pub id: String,
    pub name: String,
    pub log_group: String,
    pub image: String,
    pub image_sha: String,
    pub ports: Vec<String>,
    pub labels: Vec<String>,
    pub state: Option<ContainerState>,
    pub started_at: Option<TimestampMs>,
}

/// Incremental update for `/api/stream/containers`, relative to what a client has already seen.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerDiff {
    pub added: Vec<ContainerSummary>,
    pub updated: Vec<ContainerSummary>,
    pub removed: Vec<String>,
}

pub fn container_short_id(container_id: &str) -> &str {
    container_id.get(..12).unwrap_or(container_id)
}

pub fn container_log_id(log_group: &str, container_id: &str) -> String {
    format!(
        "{}@{}",
        &container_id.get(..12).unwrap_or(container_id),
        log_group
    )
}

impl ContainerSummary {
    pub fn short_id(&self) -> &str {
        container_short_id(&self.id)
    }
    pub fn log_id(&self) -> String {
        container_log_id(&self.log_group, &self.short_id())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerCommandResult {
    Submitted,
    Noop,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HostSample {
    pub ts: TimestampMs,
    pub cpu_pct: f64,
    pub mem_used: u64,
    pub mem_total: u64,
    pub storage_used: u64,
    pub storage_total: u64,
    pub metrics_size: u64,
    pub logs_size: u64,
    pub net_rx: u64,
    pub net_tx: u64,
    pub disk_read: u64,
    pub disk_write: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainerSample {
    pub ts: TimestampMs,
    pub log_group: String,
    pub cid: String,
    pub cpu_pct: f64,
    pub mem_used: u64,
    pub mem_limit: u64,
    pub net_rx: u64,
    pub net_tx: u64,
    pub blk_read: u64,
    pub blk_write: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainerGroupMetrics {
    pub sum: Vec<GroupPoint>,
    pub containers: HashMap<String, Vec<ContainerPoint>>,
}

pub type ContainerMetricsByLogGroup = HashMap<String, Vec<GroupPoint>>;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HostPoint {
    pub ts: TimestampMs,
    pub cpu_pct: f64,
    pub mem_used: u64,
    pub mem_total: u64,
    pub storage_used: u64,
    pub storage_total: u64,
    pub metrics_size: u64,
    pub logs_size: u64,
    pub net_rx_rate: f64,
    pub net_tx_rate: f64,
    pub disk_read_rate: f64,
    pub disk_write_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainerPoint {
    pub ts: TimestampMs,
    pub log_group: String,
    pub cpu_pct: f64,
    pub mem_used: u64,
    pub mem_limit: u64,
    pub net_rx_rate: f64,
    pub net_tx_rate: f64,
    pub blk_read_rate: f64,
    pub blk_write_rate: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct GroupPoint {
    pub ts: TimestampMs,
    pub cpu_pct: f64,
    pub mem_used: u64,
    pub mem_limit: u64,
    pub net_rx_rate: f64,
    pub net_tx_rate: f64,
    pub blk_read_rate: f64,
    pub blk_write_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub host: Option<HostPoint>,
    pub containers: HashMap<String, ContainerPoint>,
    pub log_groups: HashMap<String, GroupPoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContainerStats {
    pub ts: TimestampMs,
    pub cid: String,
    pub cpu_pct: f64,
    pub mem_used: u64,
    pub mem_limit: u64,
    pub net_rx: u64,
    pub net_tx: u64,
    pub blk_read: u64,
    pub blk_write: u64,
}

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
    pub log_group: String,
    pub cid: String,
    pub stream: LogStream,
    pub level: Option<LogLevel>,
    /// Log text with ANSI escape codes stripped.
    pub line: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
pub struct LogGroupStatus {
    pub last_received: Option<TimestampMs>,
    pub live: bool,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    pub from: TimestampMs,
    pub to: TimestampMs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsResolution {
    TenSeconds,
    OneMinute,
    FiveMinutes,
    OneHour,
}

impl MetricsResolution {
    pub fn bucket_ms(self) -> i64 {
        match self {
            MetricsResolution::TenSeconds => 10_000,
            MetricsResolution::OneMinute => 60_000,
            MetricsResolution::FiveMinutes => 300_000,
            MetricsResolution::OneHour => 3_600_000,
        }
    }
}
