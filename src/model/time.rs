use serde::{Deserialize, Serialize};

/// Unix timestamp in milliseconds.
pub type TimestampMs = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    pub from: TimestampMs,
    pub to: TimestampMs,
}
