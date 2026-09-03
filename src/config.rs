use std::env;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use serde::Serialize;

const ENV_DOCKER_HOST: &str = "VPSINER_DOCKER_HOST";
const ENV_DOCKER_TIMEOUT_SECS: &str = "VPSINER_DOCKER_TIMEOUT_SECS";
const ENV_DOCKER_REQUEST_TIMEOUT_SECS: &str = "VPSINER_DOCKER_REQUEST_TIMEOUT_SECS";
const ENV_DATA_PATH: &str = "VPSINER_DATA_PATH";
const ENV_CONFIG_PATH: &str = "VPSINER_CONFIG_PATH";
const ENV_STATIC_DIR: &str = "VPSINER_STATIC_DIR";
const ENV_PORT: &str = "VPSINER_PORT";
const ENV_RETENTION_WEEKS: &str = "VPSINER_RETENTION_WEEKS";
const ENV_COLLECT_INTERVAL_SECS: &str = "VPSINER_COLLECT_INTERVAL_SECS";
const ENV_LOG_FLUSH_DEBOUNCE_MS: &str = "VPSINER_LOG_FLUSH_DEBOUNCE_MS";
const ENV_LOG_FLUSH_KEEP_ALIVE_SECS: &str = "VPSINER_LOG_FLUSH_KEEP_ALIVE_SECS";
const ENV_DOCKER_CONTROLS: &str = "VPSINER_DOCKER_CONTROLS";
const ENV_DOCKER_PROBE_INTERVAL_SECS: &str = "VPSINER_DOCKER_PROBE_INTERVAL_SECS";
const ENV_DOCKER_RETRY_SECS: &str = "VPSINER_DOCKER_RETRY_SECS";
const ENV_DOCKER_REQUEST_CONCURRENCY: &str = "VPSINER_DOCKER_REQUEST_CONCURRENCY";
const ENV_DOCKER_DEBOUNCE_MS: &str = "VPSINER_DOCKER_DEBOUNCE_MS";
const ENV_LOG_CHANNEL_CAPACITY: &str = "VPSINER_LOG_CHANNEL_CAPACITY";
const ENV_SAMPLES_CHANNEL_CAPACITY: &str = "VPSINER_SAMPLES_CHANNEL_CAPACITY";
const ENV_DOCKER_EVENTS_CHANNEL_CAPACITY: &str = "VPSINER_DOCKER_EVENTS_CHANNEL_CAPACITY";
const ENV_SQLITE_CACHE_SIZE_KB: &str = "VPSINER_SQLITE_CACHE_SIZE_KB";
const ENV_SQLITE_BUSY_TIMEOUT_MS: &str = "VPSINER_SQLITE_BUSY_TIMEOUT_MS";
const ENV_SQLITE_KEEP_ALIVE_SECS: &str = "VPSINER_SQLITE_KEEP_ALIVE_SECS";
/// Read by the Tokio runtime builder in `main`, not stored on [`Config`].
const ENV_WORKER_THREADS: &str = "VPSINER_WORKER_THREADS";
/// Read by the tracing subscriber in `main`, not stored on [`Config`].
const ENV_RUST_LOG: &str = "RUST_LOG";
const DEFAULT_RUST_LOG: &str = "info";

const DEFAULT_DOCKER_HOST: &str = "unix:///var/run/docker.sock";
const DEFAULT_DOCKER_TIMEOUT_SECS: u64 = 60;
const DEFAULT_DOCKER_REQUEST_TIMEOUT_SECS: u64 = 5;
const DEFAULT_DATA_PATH: &str = "data";
const DEFAULT_CONFIG_PATH: &str = "config";
const DEFAULT_PORT: u16 = 3000;
const DEFAULT_RETENTION_WEEKS: u32 = 4;
const DEFAULT_COLLECT_INTERVAL_SECS: u64 = 5;
/// Matches the finest stored bucket: a slower interval would leave buckets with no samples.
const MAX_COLLECT_INTERVAL_SECS: u64 = 10;
const DEFAULT_LOG_FLUSH_DEBOUNCE_MS: u64 = 500;
const DEFAULT_LOG_FLUSH_KEEP_ALIVE_SECS: u64 = 60;
const DEFAULT_DOCKER_CONTROLS: DockerControlsMode = DockerControlsMode::Auto;
const DEFAULT_DOCKER_PROBE_INTERVAL_SECS: u64 = 60;
const DEFAULT_DOCKER_RETRY_SECS: u64 = 5;
const DEFAULT_DOCKER_REQUEST_CONCURRENCY: usize = 8;
const DEFAULT_DOCKER_DEBOUNCE_MS: u64 = 1_000;
const DEFAULT_LOG_CHANNEL_CAPACITY: usize = 10_000;
const DEFAULT_SAMPLES_CHANNEL_CAPACITY: usize = 32;
const DEFAULT_DOCKER_EVENTS_CHANNEL_CAPACITY: usize = 256;
const DEFAULT_SQLITE_CACHE_SIZE_KB: u64 = 1_024;
const DEFAULT_SQLITE_BUSY_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_SQLITE_KEEP_ALIVE_SECS: u64 = 300;

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

impl fmt::Display for DockerControlsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Auto => "auto",
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        };
        f.write_str(value)
    }
}

/// One environment-variable-backed setting, as exposed by the read-only settings API.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingEntry {
    pub name: &'static str,
    pub value: String,
    pub default: String,
    pub description: &'static str,
    pub category: &'static str,
    pub overridden: bool,
}

// interval settings are read by the collectors added in later steps
#[derive(Debug, Clone)]
pub struct Config {
    pub docker_host: String,
    pub docker_timeout_secs: u64,
    pub docker_request_timeout_secs: u64,
    pub data_path: PathBuf,
    pub config_path: PathBuf,
    pub static_dir: Option<PathBuf>,
    pub port: u16,
    pub retention_weeks: u32,
    pub collect_interval: Duration,
    pub log_flush_debounce: Duration,
    pub log_flush_keep_alive: Duration,
    pub docker_controls_mode: DockerControlsMode,
    pub docker_probe_interval: Duration,
    pub docker_retry_delay: Duration,
    pub docker_request_concurrency: usize,
    pub docker_debounce: Duration,
    pub log_channel_capacity: usize,
    pub samples_channel_capacity: usize,
    pub docker_events_channel_capacity: usize,
    pub sqlite_cache_size_kb: u64,
    pub sqlite_busy_timeout: Duration,
    pub sqlite_keep_alive: Duration,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            docker_host: env_or(ENV_DOCKER_HOST, DEFAULT_DOCKER_HOST),
            docker_timeout_secs: parse_positive_u64_or(
                ENV_DOCKER_TIMEOUT_SECS,
                DEFAULT_DOCKER_TIMEOUT_SECS,
            ),
            docker_request_timeout_secs: parse_positive_u64_or(
                ENV_DOCKER_REQUEST_TIMEOUT_SECS,
                DEFAULT_DOCKER_REQUEST_TIMEOUT_SECS,
            ),
            data_path: PathBuf::from(env_or(ENV_DATA_PATH, DEFAULT_DATA_PATH)),
            config_path: PathBuf::from(env_or(ENV_CONFIG_PATH, DEFAULT_CONFIG_PATH)),
            static_dir: env::var_os(ENV_STATIC_DIR).map(PathBuf::from),
            port: parse_positive_u16_or(ENV_PORT, DEFAULT_PORT),
            retention_weeks: parse_or(ENV_RETENTION_WEEKS, DEFAULT_RETENTION_WEEKS),
            collect_interval: Duration::from_secs(parse_bounded_u64_or(
                ENV_COLLECT_INTERVAL_SECS,
                DEFAULT_COLLECT_INTERVAL_SECS,
                MAX_COLLECT_INTERVAL_SECS,
            )),
            log_flush_debounce: Duration::from_millis(parse_or(
                ENV_LOG_FLUSH_DEBOUNCE_MS,
                DEFAULT_LOG_FLUSH_DEBOUNCE_MS,
            )),
            log_flush_keep_alive: Duration::from_secs(parse_positive_u64_or(
                ENV_LOG_FLUSH_KEEP_ALIVE_SECS,
                DEFAULT_LOG_FLUSH_KEEP_ALIVE_SECS,
            )),
            docker_controls_mode: parse_or(ENV_DOCKER_CONTROLS, DEFAULT_DOCKER_CONTROLS),
            docker_probe_interval: Duration::from_secs(parse_positive_u64_or(
                ENV_DOCKER_PROBE_INTERVAL_SECS,
                DEFAULT_DOCKER_PROBE_INTERVAL_SECS,
            )),
            docker_retry_delay: Duration::from_secs(parse_positive_u64_or(
                ENV_DOCKER_RETRY_SECS,
                DEFAULT_DOCKER_RETRY_SECS,
            )),
            docker_request_concurrency: parse_positive_usize_or(
                ENV_DOCKER_REQUEST_CONCURRENCY,
                DEFAULT_DOCKER_REQUEST_CONCURRENCY,
            ),
            docker_debounce: Duration::from_millis(parse_or(
                ENV_DOCKER_DEBOUNCE_MS,
                DEFAULT_DOCKER_DEBOUNCE_MS,
            )),
            log_channel_capacity: parse_positive_usize_or(
                ENV_LOG_CHANNEL_CAPACITY,
                DEFAULT_LOG_CHANNEL_CAPACITY,
            ),
            samples_channel_capacity: parse_positive_usize_or(
                ENV_SAMPLES_CHANNEL_CAPACITY,
                DEFAULT_SAMPLES_CHANNEL_CAPACITY,
            ),
            docker_events_channel_capacity: parse_positive_usize_or(
                ENV_DOCKER_EVENTS_CHANNEL_CAPACITY,
                DEFAULT_DOCKER_EVENTS_CHANNEL_CAPACITY,
            ),
            sqlite_cache_size_kb: parse_positive_u64_or(
                ENV_SQLITE_CACHE_SIZE_KB,
                DEFAULT_SQLITE_CACHE_SIZE_KB,
            ),
            sqlite_busy_timeout: Duration::from_millis(parse_positive_u64_or(
                ENV_SQLITE_BUSY_TIMEOUT_MS,
                DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
            )),
            sqlite_keep_alive: Duration::from_secs(parse_positive_u64_or(
                ENV_SQLITE_KEEP_ALIVE_SECS,
                DEFAULT_SQLITE_KEEP_ALIVE_SECS,
            )),
        }
    }

    /// Lists every supported environment variable with its effective and default value.
    pub fn describe(&self) -> Vec<SettingEntry> {
        let entry = |name: &'static str,
                     value: String,
                     default: String,
                     description: &'static str,
                     category: &'static str| SettingEntry {
            name,
            value,
            default,
            description,
            category,
            overridden: env::var_os(name).is_some(),
        };
        let path = |value: &PathBuf| value.display().to_string();

        vec![
            entry(
                ENV_DOCKER_HOST,
                self.docker_host.clone(),
                DEFAULT_DOCKER_HOST.to_string(),
                "Docker socket or socket-proxy endpoint, for example http://docker-proxy:2375",
                "common",
            ),
            entry(
                ENV_RETENTION_WEEKS,
                self.retention_weeks.to_string(),
                DEFAULT_RETENTION_WEEKS.to_string(),
                "Number of weeks of metrics and logs to retain",
                "common",
            ),
            entry(
                ENV_DOCKER_CONTROLS,
                self.docker_controls_mode.to_string(),
                DEFAULT_DOCKER_CONTROLS.to_string(),
                "Container controls mode: auto, enabled, or disabled",
                "common",
            ),
            entry(
                ENV_PORT,
                self.port.to_string(),
                DEFAULT_PORT.to_string(),
                "HTTP listen port inside the container",
                "common",
            ),
            entry(
                ENV_WORKER_THREADS,
                env::var(ENV_WORKER_THREADS).unwrap_or_default(),
                String::new(),
                "Overrides Tokio runtime worker-thread count; by default Tokio uses available CPU parallelism",
                "advanced",
            ),
            entry(
                ENV_DATA_PATH,
                path(&self.data_path),
                DEFAULT_DATA_PATH.to_string(),
                "Directory containing metrics and log databases",
                "advanced",
            ),
            entry(
                ENV_CONFIG_PATH,
                path(&self.config_path),
                DEFAULT_CONFIG_PATH.to_string(),
                "Directory containing UI configuration (ui.json)",
                "advanced",
            ),
            entry(
                ENV_COLLECT_INTERVAL_SECS,
                self.collect_interval.as_secs().to_string(),
                DEFAULT_COLLECT_INTERVAL_SECS.to_string(),
                "Host and container metrics collection interval, at most 10 seconds",
                "advanced",
            ),
            entry(
                ENV_LOG_FLUSH_DEBOUNCE_MS,
                self.log_flush_debounce.as_millis().to_string(),
                DEFAULT_LOG_FLUSH_DEBOUNCE_MS.to_string(),
                "Delay used to coalesce buffered log lines per service before writing them to storage",
                "advanced",
            ),
            entry(
                ENV_LOG_FLUSH_KEEP_ALIVE_SECS,
                self.log_flush_keep_alive.as_secs().to_string(),
                DEFAULT_LOG_FLUSH_KEEP_ALIVE_SECS.to_string(),
                "How long an idle per-service log flush worker stays alive before exiting",
                "advanced",
            ),
            entry(
                ENV_LOG_CHANNEL_CAPACITY,
                self.log_channel_capacity.to_string(),
                DEFAULT_LOG_CHANNEL_CAPACITY.to_string(),
                "Maximum number of log lines buffered before backpressure",
                "advanced",
            ),
            entry(
                ENV_SAMPLES_CHANNEL_CAPACITY,
                self.samples_channel_capacity.to_string(),
                DEFAULT_SAMPLES_CHANNEL_CAPACITY.to_string(),
                "Maximum number of container sample batches buffered before backpressure",
                "advanced",
            ),
            entry(
                ENV_DOCKER_PROBE_INTERVAL_SECS,
                self.docker_probe_interval.as_secs().to_string(),
                DEFAULT_DOCKER_PROBE_INTERVAL_SECS.to_string(),
                "Interval for Docker write-capability probing, log observer fallback reconciliation, and registry refresh workers",
                "advanced",
            ),
            entry(
                ENV_DOCKER_RETRY_SECS,
                self.docker_retry_delay.as_secs().to_string(),
                DEFAULT_DOCKER_RETRY_SECS.to_string(),
                "Delay before retrying the Docker container event observer after its stream ends or fails",
                "advanced",
            ),
            entry(
                ENV_DOCKER_REQUEST_CONCURRENCY,
                self.docker_request_concurrency.to_string(),
                DEFAULT_DOCKER_REQUEST_CONCURRENCY.to_string(),
                "Maximum number of concurrent Docker inspect and stats requests",
                "advanced",
            ),
            entry(
                ENV_DOCKER_EVENTS_CHANNEL_CAPACITY,
                self.docker_events_channel_capacity.to_string(),
                DEFAULT_DOCKER_EVENTS_CHANNEL_CAPACITY.to_string(),
                "Maximum number of container observe events buffered before new events are dropped",
                "advanced",
            ),
            entry(
                ENV_DOCKER_DEBOUNCE_MS,
                self.docker_debounce.as_millis().to_string(),
                DEFAULT_DOCKER_DEBOUNCE_MS.to_string(),
                "Delay used to coalesce container observation and container info refresh requests",
                "advanced",
            ),
            entry(
                ENV_DOCKER_TIMEOUT_SECS,
                self.docker_timeout_secs.to_string(),
                DEFAULT_DOCKER_TIMEOUT_SECS.to_string(),
                "Internal timeout for Docker API requests",
                "advanced",
            ),
            entry(
                ENV_DOCKER_REQUEST_TIMEOUT_SECS,
                self.docker_request_timeout_secs.to_string(),
                DEFAULT_DOCKER_REQUEST_TIMEOUT_SECS.to_string(),
                "Timeout for fetch requests",
                "advanced",
            ),
            entry(
                ENV_SQLITE_CACHE_SIZE_KB,
                self.sqlite_cache_size_kb.to_string(),
                DEFAULT_SQLITE_CACHE_SIZE_KB.to_string(),
                "Page cache limit in KiB for each SQLite database connection",
                "advanced",
            ),
            entry(
                ENV_SQLITE_BUSY_TIMEOUT_MS,
                self.sqlite_busy_timeout.as_millis().to_string(),
                DEFAULT_SQLITE_BUSY_TIMEOUT_MS.to_string(),
                "How long SQLite waits for a locked database before failing",
                "advanced",
            ),
            entry(
                ENV_SQLITE_KEEP_ALIVE_SECS,
                self.sqlite_keep_alive.as_secs().to_string(),
                DEFAULT_SQLITE_KEEP_ALIVE_SECS.to_string(),
                "How long an idle log database connection is kept open before it is closed",
                "advanced",
            ),
            entry(
                ENV_STATIC_DIR,
                self.static_dir.as_ref().map(path).unwrap_or_default(),
                String::new(),
                "Directory from which the backend serves the frontend",
                "advanced",
            ),
            entry(
                ENV_RUST_LOG,
                env_or(ENV_RUST_LOG, DEFAULT_RUST_LOG),
                DEFAULT_RUST_LOG.to_string(),
                "Backend log filter, such as debug or vpsiner=debug",
                "common",
            ),
        ]
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_or<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Debug,
{
    match env::var(key) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|err| panic!("{key} must be a valid value, got '{value}': {err:?}")),
        Err(_) => default,
    }
}

fn parse_positive_u64_or(key: &str, default: u64) -> u64 {
    let parsed = parse_or(key, default);
    assert!(
        parsed > 0,
        "{key} must be a positive integer, got: {parsed}"
    );
    parsed
}

fn parse_bounded_u64_or(key: &str, default: u64, max: u64) -> u64 {
    let parsed = parse_positive_u64_or(key, default);
    assert!(parsed <= max, "{key} must be at most {max}, got: {parsed}");
    parsed
}

fn parse_positive_usize_or(key: &str, default: usize) -> usize {
    let parsed = parse_or(key, default);
    assert!(
        parsed > 0,
        "{key} must be a positive integer, got: {parsed}"
    );
    parsed
}

fn parse_positive_u16_or(key: &str, default: u16) -> u16 {
    let parsed = parse_or(key, default);
    assert!(
        parsed > 0,
        "{key} must be a positive integer, got: {parsed}"
    );
    parsed
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn defaults() -> Config {
        Config {
            docker_host: DEFAULT_DOCKER_HOST.to_string(),
            docker_timeout_secs: DEFAULT_DOCKER_TIMEOUT_SECS,
            docker_request_timeout_secs: DEFAULT_DOCKER_REQUEST_TIMEOUT_SECS,
            data_path: PathBuf::from(DEFAULT_DATA_PATH),
            config_path: PathBuf::from(DEFAULT_CONFIG_PATH),
            static_dir: None,
            port: DEFAULT_PORT,
            retention_weeks: DEFAULT_RETENTION_WEEKS,
            collect_interval: Duration::from_secs(DEFAULT_COLLECT_INTERVAL_SECS),
            log_flush_debounce: Duration::from_millis(DEFAULT_LOG_FLUSH_DEBOUNCE_MS),
            log_flush_keep_alive: Duration::from_secs(DEFAULT_LOG_FLUSH_KEEP_ALIVE_SECS),
            docker_controls_mode: DEFAULT_DOCKER_CONTROLS,
            docker_probe_interval: Duration::from_secs(DEFAULT_DOCKER_PROBE_INTERVAL_SECS),
            docker_retry_delay: Duration::from_secs(DEFAULT_DOCKER_RETRY_SECS),
            docker_request_concurrency: DEFAULT_DOCKER_REQUEST_CONCURRENCY,
            docker_debounce: Duration::from_millis(DEFAULT_DOCKER_DEBOUNCE_MS),
            log_channel_capacity: DEFAULT_LOG_CHANNEL_CAPACITY,
            samples_channel_capacity: DEFAULT_SAMPLES_CHANNEL_CAPACITY,
            docker_events_channel_capacity: DEFAULT_DOCKER_EVENTS_CHANNEL_CAPACITY,
            sqlite_cache_size_kb: DEFAULT_SQLITE_CACHE_SIZE_KB,
            sqlite_busy_timeout: Duration::from_millis(DEFAULT_SQLITE_BUSY_TIMEOUT_MS),
            sqlite_keep_alive: Duration::from_secs(DEFAULT_SQLITE_KEEP_ALIVE_SECS),
        }
    }

    #[test]
    fn describe_lists_every_supported_variable_once() {
        let entries = defaults().describe();
        let names: HashSet<_> = entries.iter().map(|entry| entry.name).collect();

        assert_eq!(names.len(), entries.len(), "duplicate setting names");
        assert_eq!(entries.len(), 24);
        assert!(names.contains(ENV_WORKER_THREADS));
        assert!(names.contains(ENV_RUST_LOG));
        assert!(names.contains(ENV_DOCKER_HOST));
    }

    #[test]
    fn describe_reports_defaults_for_a_default_config() {
        for entry in defaults().describe() {
            // These two are read straight from the environment, not from Config.
            if entry.name == ENV_WORKER_THREADS || entry.name == ENV_RUST_LOG || entry.overridden {
                continue;
            }
            assert_eq!(
                entry.value, entry.default,
                "{} should report its default",
                entry.name
            );
        }
    }

    #[test]
    fn describe_categorises_and_documents_every_entry() {
        for entry in defaults().describe() {
            assert!(
                matches!(entry.category, "common" | "advanced"),
                "{} has unexpected category {}",
                entry.name,
                entry.category
            );
            assert!(
                !entry.description.is_empty(),
                "{} is missing a description",
                entry.name
            );
        }
    }

    #[test]
    fn docker_controls_mode_round_trips_through_strings() {
        for mode in [
            DockerControlsMode::Auto,
            DockerControlsMode::Enabled,
            DockerControlsMode::Disabled,
        ] {
            assert_eq!(mode.to_string().parse::<DockerControlsMode>(), Ok(mode));
        }
    }
}
