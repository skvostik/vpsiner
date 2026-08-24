use async_trait::async_trait;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use sysinfo::{Disks, Networks, System};

use crate::error::{AppError, AppResult};
use crate::model::HostSample;

/// Host-level metrics source (sysinfo in production).
#[allow(dead_code)] // consumed by the metrics collector added in a later step
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait HostMetricsSource: Send + Sync + 'static {
    async fn sample(&self) -> AppResult<HostSample>;
}

/// sysinfo-backed implementation.
pub struct SysinfoHost {
    system: Mutex<System>,
}

impl Default for SysinfoHost {
    fn default() -> Self {
        Self {
            system: Mutex::new(System::new_all()),
        }
    }
}

#[async_trait]
impl HostMetricsSource for SysinfoHost {
    async fn sample(&self) -> AppResult<HostSample> {
        let mut system = self
            .system
            .lock()
            .map_err(|err| AppError::Host(format!("sysinfo lock poisoned: {err}")))?;
        system.refresh_all();

        let networks = Networks::new_with_refreshed_list();
        let (net_rx, net_tx) = networks.values().fold((0_u64, 0_u64), |totals, network| {
            (
                totals.0.saturating_add(network.total_received()),
                totals.1.saturating_add(network.total_transmitted()),
            )
        });

        let disks = Disks::new_with_refreshed_list();
        let (storage_used, storage_total) = disks
            .list()
            .iter()
            .find(|disk| disk.mount_point().to_string_lossy() == "/")
            .map(|disk| {
                let total = disk.total_space();
                (total.saturating_sub(disk.available_space()), total)
            })
            .unwrap_or((0, 0));
        let (disk_read, disk_write) = disks.list().iter().fold((0_u64, 0_u64), |totals, disk| {
            let usage = disk.usage();
            (
                totals.0.saturating_add(usage.read_bytes),
                totals.1.saturating_add(usage.written_bytes),
            )
        });

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| AppError::Host(format!("system clock is before unix epoch: {err}")))?
            .as_millis() as i64;

        Ok(HostSample {
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
    }
}
