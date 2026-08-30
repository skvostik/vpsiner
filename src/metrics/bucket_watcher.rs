//! Turns "a raw sample was persisted" into "a resolution's bucket just finished", so SSE
//! time-series endpoints can push newly-completed buckets instead of polling on a timer.

use std::sync::Mutex;

use tokio::sync::watch;

use crate::metrics::downsampling::bucket_end;
use crate::model::{MetricsResolution, TimestampMs};

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

    fn observe(&self, ts: TimestampMs, bucket_ms: i64) {
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

/// Bumped whenever a raw metrics sample is durably persisted; fans out into per-resolution
/// "bucket just completed" notifications shared by every SSE connection at that resolution.
pub struct BucketWatcher {
    ten_seconds: ResolutionChannel,
    one_minute: ResolutionChannel,
    five_minutes: ResolutionChannel,
    one_hour: ResolutionChannel,
}

impl Default for BucketWatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketWatcher {
    pub fn new() -> Self {
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

    /// Called once a raw sample at `ts` is confirmed written to the metrics store. A single
    /// sample can complete buckets across multiple resolutions at once (e.g. top of the hour).
    pub fn observe_sample(&self, ts: TimestampMs) {
        for resolution in [
            MetricsResolution::TenSeconds,
            MetricsResolution::OneMinute,
            MetricsResolution::FiveMinutes,
            MetricsResolution::OneHour,
        ] {
            self.channel(resolution).observe(ts, resolution.bucket_ms());
        }
    }

    pub fn subscribe(&self, resolution: MetricsResolution) -> watch::Receiver<TimestampMs> {
        self.channel(resolution).tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bumps_only_resolutions_whose_bucket_advanced() {
        let watcher = BucketWatcher::new();
        let mut ten_seconds = watcher.subscribe(MetricsResolution::TenSeconds);
        let mut one_hour = watcher.subscribe(MetricsResolution::OneHour);

        watcher.observe_sample(5_000);
        assert!(ten_seconds.has_changed().unwrap());
        assert_eq!(*ten_seconds.borrow_and_update(), 10_000);
        assert!(one_hour.has_changed().unwrap());
        assert_eq!(*one_hour.borrow_and_update(), 3_600_000);

        // Still within the same 10s and 1h buckets: no further notification.
        watcher.observe_sample(6_000);
        assert!(!ten_seconds.has_changed().unwrap());
        assert!(!one_hour.has_changed().unwrap());

        // Crosses into the next 10s bucket, but not the next 1h bucket.
        watcher.observe_sample(11_000);
        assert!(ten_seconds.has_changed().unwrap());
        assert_eq!(*ten_seconds.borrow_and_update(), 20_000);
        assert!(!one_hour.has_changed().unwrap());
    }
}
