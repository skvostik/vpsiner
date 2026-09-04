pub mod buffer;
pub mod flush_watcher;
mod helpers;
mod schema;
pub mod store;

use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Duration;

use crate::logs::buffer::LogBuffer;
use crate::logs::flush_watcher::LogFlushWatcher;
use crate::logs::store::LogStore;
use crate::metadata::MetadataStore;
use crate::{docker::DockerService, error::AppError};

pub use helpers::{
    database_week_start_ms, decode_cursor, detect_level, encode_cursor, format_timestamp_ms,
    parse_docker_timestamp, safe_service_path, sanitize_fts_query, strip_ansi_escape_codes,
    week_database_name,
};

pub(crate) fn storage(error: impl std::fmt::Display) -> AppError {
    AppError::Storage(error.to_string())
}

pub async fn run_ingestion(
    docker: Arc<dyn DockerService>,
    logs: Arc<dyn LogStore>,
    metadata: Arc<dyn MetadataStore>,
    services: Arc<crate::metadata::ServiceRegistry>,
    flush_watcher: Arc<LogFlushWatcher>,
    flush_debounce: Duration,
    flush_keep_alive: Duration,
) {
    tracing::info!("log ingestion initialized");
    let buffer = match LogBuffer::new(
        logs,
        metadata,
        services,
        flush_watcher,
        flush_debounce,
        flush_keep_alive,
    )
    .await
    {
        Ok(buffer) => buffer,
        Err(err) => {
            tracing::error!(error = %err, "failed to initialize log buffer; log ingestion disabled");
            return;
        }
    };

    let mut receiver = docker.logs();
    while let Some(line) = receiver.next().await {
        buffer.push(line);
    }
    buffer.flush_all().await;
}
