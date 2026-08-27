use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

/// Whether container start/stop/restart endpoints are exposed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DockerControlsMode {
    /// Probe the Docker socket/proxy at startup and periodically to decide.
    Auto,
    /// Always report controls as available, skipping detection.
    Enabled,
    /// Always report controls as unavailable, skipping detection.
    Disabled,
}

impl FromStr for DockerControlsMode {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "enabled" | "true" | "on" => Ok(Self::Enabled),
            "disabled" | "false" | "off" => Ok(Self::Disabled),
            _ => Err(()),
        }
    }
}

#[allow(dead_code)] // interval settings are read by the collectors added in later steps
#[derive(Debug, Clone)]
pub struct Config {
    pub docker_host: String,
    pub docker_timeout_secs: u64,
    pub docker_request_timeout_secs: u64,
    pub data_path: PathBuf,
    pub static_dir: Option<PathBuf>,
    pub port: u16,
    pub retention_weeks: u32,
    pub collect_interval: Duration,
    pub log_flush_interval: Duration,
    pub docker_controls_mode: DockerControlsMode,
    pub docker_probe_interval: Duration,
    pub docker_retry_delay: Duration,
    pub docker_request_concurrency: usize,
    pub docker_debounce: Duration,
    pub log_channel_capacity: usize,
    pub samples_channel_capacity: usize,
    pub log_flush_lines: usize,
    pub docker_events_channel_capacity: usize,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            docker_host: env_or("VPSINER_DOCKER_HOST", "unix:///var/run/docker.sock"),
            docker_timeout_secs: parse_or("VPSINER_DOCKER_TIMEOUT_SECS", 60),
            docker_request_timeout_secs: parse_or("VPSINER_DOCKER_REQUEST_TIMEOUT_SECS", 5),
            data_path: PathBuf::from(env_or("VPSINER_DATA_PATH", "data")),
            static_dir: env::var_os("VPSINER_STATIC_DIR").map(PathBuf::from),
            port: parse_or("VPSINER_PORT", 3000),
            retention_weeks: parse_or("VPSINER_RETENTION_WEEKS", 4),
            collect_interval: Duration::from_secs(parse_or("VPSINER_COLLECT_INTERVAL_SECS", 10)),
            log_flush_interval: Duration::from_millis(parse_or(
                "VPSINER_LOG_FLUSH_INTERVAL_MS",
                500,
            )),
            docker_controls_mode: env::var("VPSINER_DOCKER_CONTROLS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DockerControlsMode::Auto),
            docker_probe_interval: Duration::from_secs(parse_or(
                "VPSINER_DOCKER_PROBE_INTERVAL_SECS",
                60,
            )),
            docker_retry_delay: Duration::from_secs(parse_or("VPSINER_DOCKER_RETRY_SECS", 5)),
            docker_request_concurrency: parse_or("VPSINER_DOCKER_REQUEST_CONCURRENCY", 8),
            docker_debounce: Duration::from_millis(parse_or("VPSINER_DOCKER_DEBOUNCE_MS", 1_000)),
            log_channel_capacity: parse_or("VPSINER_LOG_CHANNEL_CAPACITY", 10_000),
            samples_channel_capacity: parse_or("VPSINER_SAMPLES_CHANNEL_CAPACITY", 32),
            log_flush_lines: parse_or("VPSINER_LOG_FLUSH_LINES", 100),
            docker_events_channel_capacity: parse_or("VPSINER_DOCKER_EVENTS_CHANNEL_CAPACITY", 256),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
