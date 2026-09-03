//! Counters are stored as rates; the few places that still see raw counters derive them here.

/// `None` when the counter decreased, which means it was reset by a restart.
fn counter_delta(current: u64, previous: u64) -> Option<u64> {
    current.checked_sub(previous)
}

/// Rate between two consecutive counter readings, in units per second.
///
/// `None` when the counter was reset or no time elapsed, both of which leave the
/// interval's traffic unknowable rather than zero.
pub fn optional_rate(current: u64, previous: u64, dt_ms: i64) -> Option<f64> {
    if dt_ms <= 0 {
        return None;
    }
    let delta = counter_delta(current, previous)?;
    Some(delta as f64 / (dt_ms as f64 / 1000.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_units_per_second() {
        assert_eq!(optional_rate(25_000, 5_000, 10_000), Some(2_000.0));
    }

    #[test]
    fn counter_reset_reads_as_unknown() {
        assert_eq!(optional_rate(5_000, 25_000, 10_000), None);
    }

    #[test]
    fn zero_elapsed_time_reads_as_unknown() {
        assert_eq!(optional_rate(25_000, 5_000, 0), None);
        assert_eq!(optional_rate(25_000, 5_000, -1), None);
    }
}
