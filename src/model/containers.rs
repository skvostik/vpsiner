use serde::{Deserialize, Serialize};

use super::container_id::ContainerId;
use super::time::TimestampMs;

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
    pub id: ContainerId,
    /// The real Docker id; only ever read by `src/docker/*` to make Docker API calls.
    #[serde(skip)]
    pub full_id: String,
    pub name: String,
    pub service: String,
    pub image: String,
    pub image_sha: String,
    pub ports: Vec<String>,
    pub labels: Vec<String>,
    pub state: Option<ContainerState>,
    pub started_at: Option<TimestampMs>,
}

impl ContainerSummary {
    pub fn log_id(&self) -> String {
        format!("{}@{}", self.id, self.service)
    }
}

/// Incremental update for `/api/stream/containers`, relative to what a client has already seen.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerDiff {
    pub added: Vec<ContainerSummary>,
    pub updated: Vec<ContainerSummary>,
    pub removed: Vec<ContainerId>,
}

/// For raw ids straight from bollard (e.g. docker events) where a full `ContainerId` isn't needed.
pub fn container_short_id(container_id: &str) -> &str {
    container_id.get(..12).unwrap_or(container_id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerCommandResult {
    Submitted,
    Noop,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContainerStats {
    pub ts: TimestampMs,
    pub cid: ContainerId,
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
