use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use sysinfo::{CpuRefreshKind, Disks, MemoryRefreshKind, Networks, RefreshKind, System};

use crate::error::{AppError, AppResult};
use crate::model::metrics::HostRawSample;

/// Only CPU usage and memory are read from `System`, so the (expensive) process list is
/// never collected.
fn host_refresh_kind() -> RefreshKind {
    RefreshKind::nothing()
        .with_cpu(CpuRefreshKind::everything())
        .with_memory(MemoryRefreshKind::everything())
}

/// Host-level metrics source (sysinfo in production).
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait HostMetricsSource: Send + Sync + 'static {
    async fn sample(&self) -> AppResult<HostRawSample>;
}

/// sysinfo-backed implementation.
pub struct SysinfoHost {
    system: Arc<Mutex<System>>,
    networks: Arc<Mutex<Networks>>,
    disks: Arc<Mutex<Disks>>,
}

impl Default for SysinfoHost {
    fn default() -> Self {
        Self {
            system: Arc::new(Mutex::new(System::new_with_specifics(host_refresh_kind()))),
            networks: Arc::new(Mutex::new(Networks::new())),
            disks: Arc::new(Mutex::new(Disks::new())),
        }
    }
}

#[async_trait]
impl HostMetricsSource for SysinfoHost {
    async fn sample(&self) -> AppResult<HostRawSample> {
        let system = self.system.clone();
        let networks = self.networks.clone();
        let disks = self.disks.clone();
        tokio::task::spawn_blocking(move || {
            let mut system = system
                .lock()
                .map_err(|err| AppError::Host(format!("sysinfo lock poisoned: {err}")))?;
            system.refresh_specifics(host_refresh_kind());

            let mut networks = networks
                .lock()
                .map_err(|err| AppError::Host(format!("sysinfo networks lock poisoned: {err}")))?;
            networks.refresh(true);
            let (net_rx, net_tx) = networks.values().fold((0_u64, 0_u64), |totals, network| {
                (
                    totals.0.saturating_add(network.total_received()),
                    totals.1.saturating_add(network.total_transmitted()),
                )
            });

            let mut disks = disks
                .lock()
                .map_err(|err| AppError::Host(format!("sysinfo disks lock poisoned: {err}")))?;
            disks.refresh(true);
            let (storage_used, storage_total) = disks
                .list()
                .iter()
                .find(|disk| disk.mount_point().to_string_lossy() == "/")
                .map(|disk| {
                    let total = disk.total_space();
                    (total.saturating_sub(disk.available_space()), total)
                })
                .unwrap_or((0, 0));
            let (disk_read, disk_write) =
                disks.list().iter().fold((0_u64, 0_u64), |totals, disk| {
                    let usage = disk.usage();
                    (
                        totals.0.saturating_add(usage.total_read_bytes),
                        totals.1.saturating_add(usage.total_written_bytes),
                    )
                });

            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|err| AppError::Host(format!("system clock is before unix epoch: {err}")))?
                .as_millis() as i64;

            Ok(HostRawSample {
                ts,
                cpu_pct: f64::from(system.global_cpu_usage()),
                mem_used: system.used_memory(),
                mem_total: system.total_memory(),
                storage_used,
                storage_total,
                metrics_size: 0,
                logs_size: 0,
                net_rx,
                net_tx,
                disk_read,
                disk_write,
            })
        })
        .await
        .map_err(|err| AppError::Host(format!("sysinfo sampling task failed: {err}")))?
    }
}
