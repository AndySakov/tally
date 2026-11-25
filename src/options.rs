/// How `Store::open_with` sets up the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Options {
    /// When to compact without being asked. `None` leaves compaction to
    /// `Store::compact`. The default is [`AutoCompact::default`].
    pub auto_compact: Option<AutoCompact>,
    /// Writes go to a new segment once the current one holds at least this
    /// many bytes (DESIGN.md, Segments). The default is 256 MiB.
    pub segment_bytes: u64,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            auto_compact: Some(AutoCompact::default()),
            segment_bytes: 256 << 20,
        }
    }
}

/// Compact once dead bytes, those of overwritten values and of Delete
/// records, are at least `dead_percent` of the log and the log is at least
/// `min_log_bytes` long. Checked on `open` and after every `set` or
/// `delete` that writes.
///
/// At 50%, each compaction copies no more live bytes than the dead bytes
/// written since the last one, and the log stays under twice its live size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoCompact {
    dead_percent: u8,
    min_log_bytes: u64,
}

impl AutoCompact {
    /// `None` unless `dead_percent` is between 1 and 100.
    pub fn new(dead_percent: u8, min_log_bytes: u64) -> Option<AutoCompact> {
        (1..=100).contains(&dead_percent).then_some(AutoCompact {
            dead_percent,
            min_log_bytes,
        })
    }

    pub fn dead_percent(&self) -> u8 {
        self.dead_percent
    }

    pub fn min_log_bytes(&self) -> u64 {
        self.min_log_bytes
    }

    pub(crate) fn is_due(&self, dead: u64, log_len: u64) -> bool {
        log_len > 0
            && log_len >= self.min_log_bytes
            && u128::from(dead) * 100 >= u128::from(self.dead_percent) * u128::from(log_len)
    }
}

/// 50% dead, in a log of at least 1 MiB.
impl Default for AutoCompact {
    fn default() -> AutoCompact {
        AutoCompact {
            dead_percent: 50,
            min_log_bytes: 1 << 20,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_percent_must_be_between_1_and_100() {
        assert_eq!(AutoCompact::new(0, 0), None);
        assert_eq!(AutoCompact::new(101, 0), None);
        assert_eq!(AutoCompact::new(1, 7).map(|a| a.dead_percent()), Some(1));
        assert_eq!(AutoCompact::new(100, 7).map(|a| a.min_log_bytes()), Some(7));
    }

    #[test]
    fn due_at_the_threshold_and_not_below_it() {
        let auto = AutoCompact::new(50, 100).unwrap();
        assert!(!auto.is_due(49, 100));
        assert!(auto.is_due(50, 100));
        assert!(!auto.is_due(50, 99), "below the minimum size");
        assert!(!AutoCompact::new(50, 0).unwrap().is_due(0, 0), "empty log");
        assert!(AutoCompact::new(100, 0).unwrap().is_due(u64::MAX, u64::MAX));
    }

    #[test]
    fn the_default_is_half_dead_in_a_mebibyte() {
        assert_eq!(AutoCompact::new(50, 1 << 20), Some(AutoCompact::default()));
        assert_eq!(
            Options::default().auto_compact,
            Some(AutoCompact::default())
        );
    }
}
