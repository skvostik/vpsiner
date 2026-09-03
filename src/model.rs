//! Domain types shared across the service boundaries.
//! Nothing from bollard, sqlx or sysinfo may leak through a trait — it is mapped here first.

use std::collections::{BTreeMap, HashMap};

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
    pub service: String,
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

pub fn container_log_id(service: &str, container_id: &str) -> String {
    format!(
        "{}@{}",
        &container_id.get(..12).unwrap_or(container_id),
        service
    )
}

impl ContainerSummary {
    pub fn short_id(&self) -> &str {
        container_short_id(&self.id)
    }
    pub fn log_id(&self) -> String {
        container_log_id(&self.service, &self.short_id())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerCommandResult {
    Submitted,
    Noop,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HostRawSample {
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

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HostSample {
    pub ts: TimestampMs,
    pub cpu_pct_mill: u64,
    pub mem_used: u64,
    pub mem_total: u64,
    pub storage_used: u64,
    pub storage_total: u64,
    pub metrics_size: u64,
    pub logs_size: u64,
    pub net_rx_rate_mill: Option<u64>,
    pub net_tx_rate_mill: Option<u64>,
    pub disk_read_rate_mill: Option<u64>,
    pub disk_write_rate_mill: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainerRawSample {
    pub ts: TimestampMs,
    pub service: String,
    pub cid: String,
    /// Cumulative CPU time consumed by the container, from `cpu_stats.cpu_usage.total_usage`.
    pub cpu_usage_ns: u64,
    /// Cumulative CPU time consumed by the whole host, from `cpu_stats.system_cpu_usage`.
    pub system_cpu_usage_ns: u64,
    pub cpu_count: u32,
    pub mem_used: u64,
    pub mem_limit: u64,
    pub net_rx: u64,
    pub net_tx: u64,
    pub blk_read: u64,
    pub blk_write: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainerSample {
    pub ts: TimestampMs,
    pub service: String,
    pub cid: String,
    pub cpu_pct_mill: u64,
    pub mem_used: u64,
    pub mem_limit: u64,
    pub net_rx_rate_mill: Option<u64>,
    pub net_tx_rate_mill: Option<u64>,
    pub blk_read_rate_mill: Option<u64>,
    pub blk_write_rate_mill: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerGroupMetrics {
    pub sum: Vec<GroupPoint>,
    pub containers: HashMap<String, Vec<ContainerPoint>>,
}

pub type ContainerMetricsByService = HashMap<String, Vec<GroupPoint>>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricsResponse<T> {
    pub resolution: String,
    pub data: T,
}

/// One newly-completed bucket's cross-section, pushed by `/api/stream/metrics/containers/{service}`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerGroupMetricsAppend {
    pub sum: Option<GroupPoint>,
    pub containers: HashMap<String, ContainerPoint>,
}

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
    pub net_rx_rate: Option<f64>,
    pub net_tx_rate: Option<f64>,
    pub disk_read_rate: Option<f64>,
    pub disk_write_rate: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainerPoint {
    pub ts: TimestampMs,
    pub service: String,
    pub cpu_pct: f64,
    pub mem_used: u64,
    pub mem_limit: u64,
    pub net_rx_rate: Option<f64>,
    pub net_tx_rate: Option<f64>,
    pub blk_read_rate: Option<f64>,
    pub blk_write_rate: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct GroupPoint {
    pub ts: TimestampMs,
    pub cpu_pct: f64,
    pub mem_used: u64,
    pub mem_limit: u64,
    pub net_rx_rate: Option<f64>,
    pub net_tx_rate: Option<f64>,
    pub blk_read_rate: Option<f64>,
    pub blk_write_rate: Option<f64>,
}

/// Milli-units are the stored form; the API exposes the same value scaled back down.
fn from_mill(value: Option<u64>) -> Option<f64> {
    value.map(|value| value as f64 / 1_000.0)
}

impl From<HostSample> for HostPoint {
    fn from(sample: HostSample) -> Self {
        Self {
            ts: sample.ts,
            cpu_pct: sample.cpu_pct_mill as f64 / 1_000.0,
            mem_used: sample.mem_used,
            mem_total: sample.mem_total,
            storage_used: sample.storage_used,
            storage_total: sample.storage_total,
            metrics_size: sample.metrics_size,
            logs_size: sample.logs_size,
            net_rx_rate: from_mill(sample.net_rx_rate_mill),
            net_tx_rate: from_mill(sample.net_tx_rate_mill),
            disk_read_rate: from_mill(sample.disk_read_rate_mill),
            disk_write_rate: from_mill(sample.disk_write_rate_mill),
        }
    }
}

impl From<ContainerSample> for ContainerPoint {
    fn from(sample: ContainerSample) -> Self {
        Self {
            ts: sample.ts,
            service: sample.service,
            cpu_pct: sample.cpu_pct_mill as f64 / 1_000.0,
            mem_used: sample.mem_used,
            mem_limit: sample.mem_limit,
            net_rx_rate: from_mill(sample.net_rx_rate_mill),
            net_tx_rate: from_mill(sample.net_tx_rate_mill),
            blk_read_rate: from_mill(sample.blk_read_rate_mill),
            blk_write_rate: from_mill(sample.blk_write_rate_mill),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub host: Option<HostPoint>,
    pub containers: HashMap<String, ContainerPoint>,
    pub services: HashMap<String, GroupPoint>,
}

/// Container half of `MetricsSnapshot`, pushed on its own SSE event.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ContainersSnapshot {
    pub containers: HashMap<String, ContainerPoint>,
    pub services: HashMap<String, GroupPoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContainerStats {
    pub ts: TimestampMs,
    pub cid: String,
    pub cpu_usage_ns: u64,
    pub system_cpu_usage_ns: u64,
    pub cpu_count: u32,
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
    pub service: String,
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

const RESOLUTION_BOUNDARY_TOLERANCE_MS: TimestampMs = 60_000;

impl MetricsResolution {
    /// Resolutions materialised by rolling up the `10s` tables.
    pub const COARSE: [MetricsResolution; 3] = [
        MetricsResolution::OneMinute,
        MetricsResolution::FiveMinutes,
        MetricsResolution::OneHour,
    ];

    pub fn bucket_ms(self) -> u64 {
        match self {
            MetricsResolution::TenSeconds => 10_000,
            MetricsResolution::OneMinute => 60_000,
            MetricsResolution::FiveMinutes => 300_000,
            MetricsResolution::OneHour => 3_600_000,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            MetricsResolution::TenSeconds => "10s",
            MetricsResolution::OneMinute => "1m",
            MetricsResolution::FiveMinutes => "5m",
            MetricsResolution::OneHour => "1h",
        }
    }

    pub fn for_range(range: TimeRange) -> Self {
        let span = range.to.saturating_sub(range.from);
        if span <= 30 * 60 * 1000 + RESOLUTION_BOUNDARY_TOLERANCE_MS {
            MetricsResolution::TenSeconds
        } else if span <= 3 * 60 * 60 * 1000 + RESOLUTION_BOUNDARY_TOLERANCE_MS {
            MetricsResolution::OneMinute
        } else if span <= 24 * 60 * 60 * 1000 + RESOLUTION_BOUNDARY_TOLERANCE_MS {
            MetricsResolution::FiveMinutes
        } else {
            MetricsResolution::OneHour
        }
    }
}
