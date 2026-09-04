//! Domain types shared across the service boundaries.
//! Nothing from bollard, sqlx or sysinfo may leak through a trait — it is mapped here first.

pub mod container_id;
pub mod containers;
pub mod logs;
pub mod metrics;
pub mod service_id;
pub mod time;
