//! Turns "a raw sample was persisted" into "a resolution's bucket just finished", so SSE
//! time-series endpoints can push newly-completed buckets instead of polling on a timer.
//! Tracked per source, since host and container samples are collected independently.

use std::sync::Mutex;

use tokio::sync::watch;

use crate::metrics::downsampling::bucket_end;
use crate::model::{metrics::MetricsResolution, time::TimestampMs};

struct ResolutionChannel {
    last_bucket_end: Mutex<TimestampMs>,
    tx: watch::Sender<TimestampMs>,
}

impl ResolutionChannel {
    fn new() -> Self {
        let (tx, _) = watch::channel(0);
        Self {
            last_bucket_end: Mutex::new(0),
            tx,
        }
    }

    fn observe(&self, ts: TimestampMs, bucket_ms: u64) {
        let end = bucket_end(ts, bucket_ms);
        let mut last = self
            .last_bucket_end
            .lock()
            .expect("bucket watcher lock poisoned");
        if end > *last {
            *last = end;
            drop(last);
            self.tx.send_modify(|current| *current = end);
        }
    }
}

struct ResolutionChannels {
    ten_seconds: ResolutionChannel,
    one_minute: ResolutionChannel,
    five_minutes: ResolutionChannel,
    one_hour: ResolutionChannel,
}

impl ResolutionChannels {
    fn new() -> Self {
        Self {
            ten_seconds: ResolutionChannel::new(),
            one_minute: ResolutionChannel::new(),
            five_minutes: ResolutionChannel::new(),
            one_hour: ResolutionChannel::new(),
        }
    }

    fn channel(&self, resolution: MetricsResolution) -> &ResolutionChannel {
        match resolution {
            MetricsResolution::TenSeconds => &self.ten_seconds,
            MetricsResolution::OneMinute => &self.one_minute,
            MetricsResolution::FiveMinutes => &self.five_minutes,
            MetricsResolution::OneHour => &self.one_hour,
        }
    }
}

/// Host and container samples are collected at independent rates, so each drives its own
/// notifications; a stream must never be woken by — or have its cursor advanced by — the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsSource {
    Host,
    Containers,
}

/// Bumped whenever a raw metrics sample is durably persisted; fans out into per-source,
/// per-resolution "bucket just completed" notifications shared by every SSE connection.
pub struct BucketWatcher {
    host: ResolutionChannels,
    containers: ResolutionChannels,
}

impl Default for BucketWatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketWatcher {
    pub fn new() -> Self {
        Self {
            host: ResolutionChannels::new(),
            containers: ResolutionChannels::new(),
        }
    }

    fn channels(&self, source: MetricsSource) -> &ResolutionChannels {
        match source {
            MetricsSource::Host => &self.host,
            MetricsSource::Containers => &self.containers,
        }
    }

    /// Called once a raw sample at `ts` is confirmed written to the metrics store. A single
    /// sample can complete buckets across multiple resolutions at once (e.g. top of the hour).
    pub fn observe_sample(&self, source: MetricsSource, ts: TimestampMs) {
        let channels = self.channels(source);
        for resolution in [
            MetricsResolution::TenSeconds,
            MetricsResolution::OneMinute,
            MetricsResolution::FiveMinutes,
            MetricsResolution::OneHour,
        ] {
            channels
                .channel(resolution)
                .observe(ts, resolution.bucket_ms());
        }
    }

    pub fn subscribe(
        &self,
        source: MetricsSource,
        resolution: MetricsResolution,
    ) -> watch::Receiver<TimestampMs> {
        self.channels(source).channel(resolution).tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bumps_only_resolutions_whose_bucket_advanced() {
        let watcher = BucketWatcher::new();
        let mut ten_seconds = watcher.subscribe(MetricsSource::Host, MetricsResolution::TenSeconds);
        let mut one_hour = watcher.subscribe(MetricsSource::Host, MetricsResolution::OneHour);

        watcher.observe_sample(MetricsSource::Host, 5_000);
        assert!(ten_seconds.has_changed().unwrap());
        assert_eq!(*ten_seconds.borrow_and_update(), 10_000);
        assert!(one_hour.has_changed().unwrap());
        assert_eq!(*one_hour.borrow_and_update(), 3_600_000);

        // Still within the same 10s and 1h buckets: no further notification.
        watcher.observe_sample(MetricsSource::Host, 6_000);
        assert!(!ten_seconds.has_changed().unwrap());
        assert!(!one_hour.has_changed().unwrap());

        // Crosses into the next 10s bucket, but not the next 1h bucket.
        watcher.observe_sample(MetricsSource::Host, 11_000);
        assert!(ten_seconds.has_changed().unwrap());
        assert_eq!(*ten_seconds.borrow_and_update(), 20_000);
        assert!(!one_hour.has_changed().unwrap());
    }

    #[test]
    fn sources_do_not_notify_each_other() {
        let watcher = BucketWatcher::new();
        let host = watcher.subscribe(MetricsSource::Host, MetricsResolution::TenSeconds);
        let mut containers =
            watcher.subscribe(MetricsSource::Containers, MetricsResolution::TenSeconds);

        watcher.observe_sample(MetricsSource::Host, 5_000);
        assert!(host.has_changed().unwrap());
        assert!(!containers.has_changed().unwrap());

        // The host already completed this bucket; containers must still be notified for it.
        watcher.observe_sample(MetricsSource::Containers, 5_000);
        assert!(containers.has_changed().unwrap());
        assert_eq!(*containers.borrow_and_update(), 10_000);
    }
}
