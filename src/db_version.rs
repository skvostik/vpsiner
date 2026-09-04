//! Startup guard that refuses to run against databases written by an incompatible schema.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const METADATA_DB_VERSION: u32 = 2;
pub const LOGS_DB_VERSION: u32 = 2;
pub const METRICS_DB_VERSION: u32 = 2;

const VERSIONS_FILE: &str = "versions.json";
const ENV_FORCE: &str = "VPSINER_FORCE_DB_MIGRATION";

const METADATA_DIR: &str = "metadata";
const METRICS_DIR: &str = "metrics";
const LOGS_DIR: &str = "logs";

pub fn metadata_dir(data_path: &Path) -> PathBuf {
    data_path.join(METADATA_DIR)
}

pub fn metrics_dir(data_path: &Path) -> PathBuf {
    data_path.join(METRICS_DIR)
}

pub fn logs_dir(data_path: &Path) -> PathBuf {
    data_path.join(LOGS_DIR)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbKind {
    Metadata,
    Logs,
    Metrics,
}

impl DbKind {
    const ALL: [Self; 3] = [Self::Metadata, Self::Logs, Self::Metrics];

    fn dir_name(self) -> &'static str {
        match self {
            Self::Metadata => METADATA_DIR,
            Self::Logs => LOGS_DIR,
            Self::Metrics => METRICS_DIR,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Logs => "logs",
            Self::Metrics => "metrics",
        }
    }

    fn expected(self) -> u32 {
        match self {
            Self::Metadata => METADATA_DB_VERSION,
            Self::Logs => LOGS_DB_VERSION,
            Self::Metrics => METRICS_DB_VERSION,
        }
    }

    fn recorded(self, versions: &DbVersions) -> u32 {
        match self {
            Self::Metadata => versions.metadata,
            Self::Logs => versions.logs,
            Self::Metrics => versions.metrics,
        }
    }

    fn contents_description(self) -> &'static str {
        match self {
            Self::Metadata => "internal bookkeeping vpsiner keeps about the data it collects",
            Self::Logs => "all archived container logs",
            Self::Metrics => "all historical host and container metrics",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct DbVersions {
    metadata: u32,
    logs: u32,
    metrics: u32,
}

impl DbVersions {
    fn current() -> Self {
        Self {
            metadata: METADATA_DB_VERSION,
            logs: LOGS_DB_VERSION,
            metrics: METRICS_DB_VERSION,
        }
    }
}

/// Verifies the on-disk databases against the compiled schema versions, wiping the incompatible
/// ones when `VPSINER_FORCE_DB_MIGRATION` is set. `Err` carries a ready-to-print fatal message.
pub async fn ensure_compatible(data_path: &Path) -> Result<(), String> {
    evaluate(data_path, force_enabled()).await
}

fn force_enabled() -> bool {
    match std::env::var(ENV_FORCE) {
        Ok(value) => matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true"),
        Err(_) => false,
    }
}

async fn evaluate(data_path: &Path, force: bool) -> Result<(), String> {
    tokio::fs::create_dir_all(data_path).await.map_err(|err| {
        format!(
            "failed to create data directory {}: {err}",
            data_path.display()
        )
    })?;

    let Some(recorded) = read_versions(data_path).await else {
        for kind in DbKind::ALL {
            remove_dir(&data_path.join(kind.dir_name())).await?;
        }
        return write_versions(data_path, DbVersions::current()).await;
    };

    // A mismatch for a database that was never created, or was deleted by hand, needs no wipe.
    let stale: Vec<DbKind> = DbKind::ALL
        .into_iter()
        .filter(|kind| kind.recorded(&recorded) != kind.expected())
        .filter(|kind| data_path.join(kind.dir_name()).exists())
        .collect();

    if stale.is_empty() {
        if recorded != DbVersions::current() {
            write_versions(data_path, DbVersions::current()).await?;
        }
        return Ok(());
    }

    if !force {
        return Err(incompatible_message(data_path, recorded, &stale));
    }

    tracing::warn!(
        databases = %labels(&stale),
        "{ENV_FORCE} is set: destroying incompatible databases"
    );

    for kind in &stale {
        let dir = data_path.join(kind.dir_name());
        remove_dir(&dir).await?;
        tracing::warn!(database = kind.label(), path = %dir.display(), "database deleted");
    }

    write_versions(data_path, DbVersions::current()).await?;

    tracing::warn!(
        "database migration complete: {} starts empty, previous contents are permanently gone",
        labels(&stale)
    );

    Ok(())
}

fn labels(kinds: &[DbKind]) -> String {
    kinds
        .iter()
        .copied()
        .map(DbKind::label)
        .collect::<Vec<_>>()
        .join(", ")
}

fn versions_path(data_path: &Path) -> PathBuf {
    data_path.join(VERSIONS_FILE)
}

/// `None` when the manifest is absent or unreadable, which means this is a first run.
async fn read_versions(data_path: &Path) -> Option<DbVersions> {
    let path = versions_path(data_path);
    let raw = tokio::fs::read(&path).await.ok()?;

    match serde_json::from_slice::<DbVersions>(&raw) {
        Ok(versions) => Some(versions),
        Err(err) => {
            tracing::warn!(
                path = %path.display(),
                "unreadable version manifest, starting over: {err}"
            );
            None
        }
    }
}

async fn write_versions(data_path: &Path, versions: DbVersions) -> Result<(), String> {
    let path = versions_path(data_path);
    let body = serde_json::to_vec_pretty(&versions)
        .map_err(|err| format!("failed to serialize version manifest: {err}"))?;

    tokio::fs::write(&path, body)
        .await
        .map_err(|err| format!("failed to write {}: {err}", path.display()))
}

async fn remove_dir(path: &Path) -> Result<(), String> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("failed to delete {}: {err}", path.display())),
    }
}

fn incompatible_message(data_path: &Path, recorded: DbVersions, stale: &[DbKind]) -> String {
    let mut message = String::new();

    message.push_str("incompatible database schema, refusing to start.\n\n");
    message.push_str("The following databases were written by a different version of vpsiner:\n");

    for kind in stale {
        let _ = writeln!(
            message,
            "  - {}: on disk v{}, this build requires v{}",
            kind.label(),
            kind.recorded(&recorded),
            kind.expected()
        );
    }

    message
        .push_str("\nThis version requires a clean slate: the affected data has to be deleted\n");
    message.push_str("and collection started from scratch. That means PERMANENTLY LOSING, with\n");
    message.push_str("no backup:\n");

    for kind in stale {
        let _ = writeln!(
            message,
            "  - {} - {}",
            data_path.join(kind.dir_name()).display(),
            kind.contents_description()
        );
    }

    message.push_str("\nBack up the data directory first if you want to keep any of it, then\n");
    message.push_str("either restart vpsiner once with:\n\n");
    let _ = writeln!(message, "    {ENV_FORCE}=1\n");
    message.push_str("which deletes the paths above for you, or delete them yourself while\n");
    message.push_str("vpsiner is stopped and start it again normally.");

    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "vpsiner-db-version-{}-{unique}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn write_manifest(dir: &Path, metadata: u32, logs: u32, metrics: u32) {
        let versions = DbVersions {
            metadata,
            logs,
            metrics,
        };
        std::fs::write(
            dir.join(VERSIONS_FILE),
            serde_json::to_vec(&versions).unwrap(),
        )
        .unwrap();
    }

    fn read_manifest(dir: &Path) -> DbVersions {
        let raw = std::fs::read(dir.join(VERSIONS_FILE)).expect("manifest exists");
        serde_json::from_slice(&raw).expect("manifest parses")
    }

    fn seed_db(dir: &Path, kind: DbKind) {
        let path = dir.join(kind.dir_name());
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("seed.db"), b"data").unwrap();
    }

    fn db_seeded(dir: &Path, kind: DbKind) -> bool {
        dir.join(kind.dir_name()).join("seed.db").exists()
    }

    #[tokio::test]
    async fn fresh_directory_is_initialised_without_force() {
        let dir = TempDir::new();

        evaluate(dir.path(), false).await.expect("fresh install");

        assert_eq!(read_manifest(dir.path()), DbVersions::current());
    }

    #[tokio::test]
    async fn missing_data_directory_is_created() {
        let dir = TempDir::new();
        let nested = dir.path().join("nested").join("data");

        evaluate(&nested, false).await.expect("fresh install");

        assert_eq!(read_manifest(&nested), DbVersions::current());
    }

    #[tokio::test]
    async fn matching_manifest_leaves_data_untouched() {
        let dir = TempDir::new();
        write_manifest(
            dir.path(),
            METADATA_DB_VERSION,
            LOGS_DB_VERSION,
            METRICS_DB_VERSION,
        );
        seed_db(dir.path(), DbKind::Metrics);

        evaluate(dir.path(), false).await.expect("compatible");

        assert!(db_seeded(dir.path(), DbKind::Metrics));
    }

    #[tokio::test]
    async fn first_run_wipes_leftover_directories() {
        let dir = TempDir::new();
        seed_db(dir.path(), DbKind::Logs);
        seed_db(dir.path(), DbKind::Metrics);

        evaluate(dir.path(), false).await.expect("first run");

        assert!(!db_seeded(dir.path(), DbKind::Logs));
        assert!(!db_seeded(dir.path(), DbKind::Metrics));
        assert_eq!(read_manifest(dir.path()), DbVersions::current());
    }

    #[tokio::test]
    async fn corrupt_manifest_is_treated_as_first_run() {
        let dir = TempDir::new();
        std::fs::write(dir.path().join(VERSIONS_FILE), b"{ not json").unwrap();
        seed_db(dir.path(), DbKind::Logs);

        evaluate(dir.path(), false).await.expect("first run");

        assert!(!db_seeded(dir.path(), DbKind::Logs));
        assert_eq!(read_manifest(dir.path()), DbVersions::current());
    }

    #[tokio::test]
    async fn single_stale_version_without_force_names_only_that_database() {
        let dir = TempDir::new();
        write_manifest(dir.path(), METADATA_DB_VERSION, LOGS_DB_VERSION, 0);
        seed_db(dir.path(), DbKind::Metrics);

        let message = evaluate(dir.path(), false).await.expect_err("must refuse");

        assert!(message.contains("metrics"));
        assert!(!message.contains("  - logs"));
        assert!(!message.contains("  - metadata"));
    }

    #[tokio::test]
    async fn single_stale_version_with_force_wipes_only_that_database() {
        let dir = TempDir::new();
        write_manifest(dir.path(), METADATA_DB_VERSION, LOGS_DB_VERSION, 0);
        for kind in DbKind::ALL {
            seed_db(dir.path(), kind);
        }

        evaluate(dir.path(), true).await.expect("forced wipe");

        assert!(!db_seeded(dir.path(), DbKind::Metrics));
        assert!(db_seeded(dir.path(), DbKind::Logs));
        assert!(db_seeded(dir.path(), DbKind::Metadata));
        assert_eq!(read_manifest(dir.path()), DbVersions::current());
    }

    #[tokio::test]
    async fn stale_version_without_folder_on_disk_is_recorded_silently() {
        let dir = TempDir::new();
        write_manifest(dir.path(), METADATA_DB_VERSION, LOGS_DB_VERSION, 0);
        seed_db(dir.path(), DbKind::Logs);

        evaluate(dir.path(), false)
            .await
            .expect("nothing to delete");

        assert_eq!(read_manifest(dir.path()), DbVersions::current());
        assert!(db_seeded(dir.path(), DbKind::Logs));
    }

    #[tokio::test]
    async fn newer_version_is_treated_as_incompatible() {
        let dir = TempDir::new();
        write_manifest(
            dir.path(),
            METADATA_DB_VERSION + 1,
            LOGS_DB_VERSION,
            METRICS_DB_VERSION,
        );
        seed_db(dir.path(), DbKind::Metadata);

        let message = evaluate(dir.path(), false).await.expect_err("must refuse");
        assert!(message.contains("metadata"));

        evaluate(dir.path(), true).await.expect("forced wipe");
        assert!(!db_seeded(dir.path(), DbKind::Metadata));
    }

    #[test]
    fn force_flag_accepts_only_one_and_true() {
        for value in ["1", "true", "TRUE", " true "] {
            unsafe { std::env::set_var(ENV_FORCE, value) };
            assert!(force_enabled(), "expected {value} to enable");
        }

        for value in ["0", "false", "yes", "on", ""] {
            unsafe { std::env::set_var(ENV_FORCE, value) };
            assert!(!force_enabled(), "expected {value} to disable");
        }

        unsafe { std::env::remove_var(ENV_FORCE) };
        assert!(!force_enabled());
    }
}
