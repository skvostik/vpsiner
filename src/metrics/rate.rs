//! Counters are stored raw; every rate the API returns is derived through these helpers.

/// `None` when the counter decreased, which means it was reset by a restart.
pub fn counter_delta(current: u64, previous: u64) -> Option<u64> {
    current.checked_sub(previous)
}

/// Bytes per second over the given elapsed time.
pub fn bytes_per_second(delta: u128, elapsed_ms: i64) -> f64 {
    if elapsed_ms <= 0 {
        return 0.0;
    }
    delta as f64 / (elapsed_ms as f64 / 1000.0)
}

/// Rate between two consecutive counter readings; a reset reads as no traffic.
pub fn rate(current: u64, previous: u64, dt_ms: i64) -> f64 {
    match counter_delta(current, previous) {
        Some(delta) => bytes_per_second(u128::from(delta), dt_ms),
        None => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_bytes_per_second() {
        assert_eq!(rate(25_000, 5_000, 10_000), 2_000.0);
    }

    #[test]
    fn counter_reset_reads_as_no_traffic() {
        assert_eq!(rate(5_000, 25_000, 10_000), 0.0);
        assert_eq!(counter_delta(5_000, 25_000), None);
    }

    #[test]
    fn zero_elapsed_time_yields_zero() {
        assert_eq!(rate(25_000, 5_000, 0), 0.0);
        assert_eq!(bytes_per_second(1_000, -1), 0.0);
    }
}
