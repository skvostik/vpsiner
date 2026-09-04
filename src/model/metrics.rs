use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::container_id::ContainerId;
use super::service_id::ServiceId;
use super::time::{TimeRange, TimestampMs};

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
    /// Peak fill of the docker log channel since the previous sample, as a percentage in
    /// milli-units to match `cpu_pct_mill`.
    pub log_pressure_pct_mill: u64,
    /// Resident set size of this process; `None` when the platform can't report it.
    pub app_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HostSample {
    pub ts: TimestampMs,
    pub cpu_pct_mill: u64,
    pub mem_used: u64,
    pub storage_used: u64,
    pub metrics_size: u64,
    pub logs_size: u64,
    pub net_rx_rate_mill: Option<u64>,
    pub net_tx_rate_mill: Option<u64>,
    pub disk_read_rate_mill: Option<u64>,
    pub disk_write_rate_mill: Option<u64>,
    pub log_pressure_pct_mill: Option<u64>,
    pub app_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainerRawSample {
    pub ts: TimestampMs,
    #[serde(skip)]
    pub service: ServiceId,
    pub cid: ContainerId,
    /// Cumulative CPU time consumed by the container, from `cpu_stats.cpu_usage.total_usage`.
    pub cpu_usage_ns: u64,
    /// Cumulative CPU time consumed by the whole host, from `cpu_stats.system_cpu_usage`.
    pub system_cpu_usage_ns: u64,
    pub cpu_count: u32,
    pub mem_used: u64,
    pub net_rx: u64,
    pub net_tx: u64,
    pub blk_read: u64,
    pub blk_write: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainerSample {
    pub ts: TimestampMs,
    #[serde(skip)]
    pub service: ServiceId,
    pub cid: ContainerId,
    pub cpu_pct_mill: u64,
    pub mem_used: u64,
    pub net_rx_rate_mill: Option<u64>,
    pub net_tx_rate_mill: Option<u64>,
    pub blk_read_rate_mill: Option<u64>,
    pub blk_write_rate_mill: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerGroupMetrics {
    pub sum: Vec<GroupPoint>,
    pub containers: HashMap<ContainerId, Vec<ContainerPoint>>,
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
    pub containers: HashMap<ContainerId, ContainerPoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HostPoint {
    pub ts: TimestampMs,
    pub cpu_pct: f64,
    pub mem_used: u64,
    pub storage_used: u64,
    pub metrics_size: u64,
    pub logs_size: u64,
    pub net_rx_rate: Option<f64>,
    pub net_tx_rate: Option<f64>,
    pub disk_read_rate: Option<f64>,
    pub disk_write_rate: Option<f64>,
    pub log_pressure_pct: Option<f64>,
    pub app_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CurrentHostPoint {
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
    pub log_pressure_pct: Option<f64>,
    pub app_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainerPoint {
    pub ts: TimestampMs,
    /// Resolved from `ServiceId` at the API boundary; empty until then.
    pub service: String,
    pub cpu_pct: f64,
    pub mem_used: u64,
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
            storage_used: sample.storage_used,
            metrics_size: sample.metrics_size,
            logs_size: sample.logs_size,
            net_rx_rate: from_mill(sample.net_rx_rate_mill),
            net_tx_rate: from_mill(sample.net_tx_rate_mill),
            disk_read_rate: from_mill(sample.disk_read_rate_mill),
            disk_write_rate: from_mill(sample.disk_write_rate_mill),
            log_pressure_pct: from_mill(sample.log_pressure_pct_mill),
            app_rss_bytes: sample.app_rss_bytes,
        }
    }
}

impl From<HostRawSample> for CurrentHostPoint {
    fn from(sample: HostRawSample) -> Self {
        Self {
            ts: sample.ts,
            cpu_pct: sample.cpu_pct,
            mem_used: sample.mem_used,
            mem_total: sample.mem_total,
            storage_used: sample.storage_used,
            storage_total: sample.storage_total,
            metrics_size: sample.metrics_size,
            logs_size: sample.logs_size,
            net_rx_rate: None,
            net_tx_rate: None,
            disk_read_rate: None,
            disk_write_rate: None,
            log_pressure_pct: Some(sample.log_pressure_pct_mill as f64 / 1_000.0),
            app_rss_bytes: sample.app_rss_bytes,
        }
    }
}

impl ContainerPoint {
    /// `service` must be the name the sample's `ServiceId` resolves to.
    pub fn from_sample(sample: ContainerSample, service: String) -> Self {
        Self {
            ts: sample.ts,
            service,
            cpu_pct: sample.cpu_pct_mill as f64 / 1_000.0,
            mem_used: sample.mem_used,
            net_rx_rate: from_mill(sample.net_rx_rate_mill),
            net_tx_rate: from_mill(sample.net_tx_rate_mill),
            blk_read_rate: from_mill(sample.blk_read_rate_mill),
            blk_write_rate: from_mill(sample.blk_write_rate_mill),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub host: Option<CurrentHostPoint>,
    pub containers: HashMap<ContainerId, ContainerPoint>,
    pub services: HashMap<String, GroupPoint>,
}

/// Container half of `MetricsSnapshot`, pushed on its own SSE event.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ContainersSnapshot {
    pub containers: HashMap<ContainerId, ContainerPoint>,
    pub services: HashMap<String, GroupPoint>,
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

    /// Widest bucket a row can be stamped into; a stored `ts` leads its samples by up to this.
    pub fn max_bucket_ms() -> u64 {
        Self::COARSE
            .into_iter()
            .fold(0, |max, resolution| max.max(resolution.bucket_ms()))
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
