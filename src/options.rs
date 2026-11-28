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
/// `min_log_bytes` long. Checked on `open` and after every write that
/// writes something; the store's own thread compacts while writes go on.
///
/// If compaction falls behind, writes slow down once dead bytes reach
/// `slow_percent` of the log, and wait for compaction from `stop_percent`,
/// so the log stays under `100 / (100 - stop_percent)` times its live size,
/// twice at the default 50%, or `min_log_bytes`, whichever is larger. Past
/// that go one group of writes, a running compaction's copies, and the
/// Delete records compaction carries while a segment it keeps may hold a
/// Set they hide, which count as live here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoCompact {
    dead_percent: u8,
    slow_percent: u8,
    stop_percent: u8,
    min_log_bytes: u64,
}

impl AutoCompact {
    /// `None` unless `dead_percent` is between 1 and 99. Writes slow down
    /// 5 points above it and stop 10 points above it, at most at 99: writes
    /// stopped at 100% would let the log grow without bound, and stopped
    /// below the trigger would wait for a compaction that never comes.
    pub fn new(dead_percent: u8, min_log_bytes: u64) -> Option<AutoCompact> {
        (1..=99).contains(&dead_percent).then(|| AutoCompact {
            dead_percent,
            slow_percent: (dead_percent + 5).min(99),
            stop_percent: (dead_percent + 10).min(99),
            min_log_bytes,
        })
    }

    /// The same trigger, with writes slowing down at `slow_percent` and
    /// waiting for compaction at `stop_percent`. `None` unless
    /// `dead_percent <= slow_percent <= stop_percent <= 99`.
    pub fn with_stalls(self, slow_percent: u8, stop_percent: u8) -> Option<AutoCompact> {
        (self.dead_percent <= slow_percent && slow_percent <= stop_percent && stop_percent <= 99)
            .then_some(AutoCompact {
                slow_percent,
                stop_percent,
                ..self
            })
    }

    pub fn dead_percent(&self) -> u8 {
        self.dead_percent
    }

    pub fn slow_percent(&self) -> u8 {
        self.slow_percent
    }

    pub fn stop_percent(&self) -> u8 {
        self.stop_percent
    }

    pub fn min_log_bytes(&self) -> u64 {
        self.min_log_bytes
    }

    pub(crate) fn is_due(&self, dead: u64, log_len: u64) -> bool {
        self.reached(self.dead_percent, dead, log_len)
    }

    pub(crate) fn slows(&self, dead: u64, log_len: u64) -> bool {
        self.reached(self.slow_percent, dead, log_len)
    }

    pub(crate) fn stops(&self, dead: u64, log_len: u64) -> bool {
        self.reached(self.stop_percent, dead, log_len)
    }

    fn reached(&self, percent: u8, dead: u64, log_len: u64) -> bool {
        log_len > 0
            && log_len >= self.min_log_bytes
            && u128::from(dead) * 100 >= u128::from(percent) * u128::from(log_len)
    }
}

/// 40% dead in a log of at least 1 MiB; writes slow down at 45% and stop
/// at 50%.
impl Default for AutoCompact {
    fn default() -> AutoCompact {
        AutoCompact::new(40, 1 << 20).expect("40 is a percentage")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_percent_must_be_between_1_and_99() {
        assert_eq!(AutoCompact::new(0, 0), None);
        assert_eq!(AutoCompact::new(101, 0), None);
        assert_eq!(AutoCompact::new(1, 7).map(|a| a.dead_percent()), Some(1));
        assert_eq!(AutoCompact::new(100, 0), None, "writes could never stop");
        assert_eq!(AutoCompact::new(99, 7).map(|a| a.min_log_bytes()), Some(7));
    }

    #[test]
    fn due_at_the_threshold_and_not_below_it() {
        let auto = AutoCompact::new(50, 100).unwrap();
        assert!(!auto.is_due(49, 100));
        assert!(auto.is_due(50, 100));
        assert!(!auto.is_due(50, 99), "below the minimum size");
        assert!(!AutoCompact::new(50, 0).unwrap().is_due(0, 0), "empty log");
        assert!(AutoCompact::new(99, 0).unwrap().is_due(u64::MAX, u64::MAX));
    }

    #[test]
    fn writes_slow_5_points_and_stop_10_points_above_the_trigger() {
        let auto = AutoCompact::new(50, 100).unwrap();
        assert_eq!((auto.slow_percent(), auto.stop_percent()), (55, 60));
        assert!(!auto.slows(54, 100));
        assert!(auto.slows(55, 100));
        assert!(!auto.stops(59, 100));
        assert!(auto.stops(60, 100));
        assert!(!auto.stops(60, 99), "below the minimum size");
        let high = AutoCompact::new(90, 0).unwrap();
        assert_eq!((high.slow_percent(), high.stop_percent()), (95, 99));
        let higher = AutoCompact::new(95, 0).unwrap();
        assert_eq!((higher.slow_percent(), higher.stop_percent()), (99, 99));
        let highest = AutoCompact::new(99, 0).unwrap();
        assert_eq!((highest.slow_percent(), highest.stop_percent()), (99, 99));
    }

    #[test]
    fn stalls_must_not_come_before_the_trigger_or_each_other() {
        let auto = AutoCompact::new(40, 0).unwrap();
        assert_eq!(auto.with_stalls(39, 50), None);
        assert_eq!(auto.with_stalls(46, 45), None);
        assert_eq!(auto.with_stalls(45, 100), None, "no bound");
        assert_eq!(auto.with_stalls(99, 99).map(|a| a.stop_percent()), Some(99));
        let stalls = auto.with_stalls(40, 40).unwrap();
        assert_eq!((stalls.slow_percent(), stalls.stop_percent()), (40, 40));
        assert_eq!(stalls.dead_percent(), 40);
    }

    #[test]
    fn the_default_compacts_at_40_and_stops_writes_at_half_dead() {
        let auto = AutoCompact::default();
        assert_eq!(AutoCompact::new(40, 1 << 20), Some(auto));
        assert_eq!(
            (auto.slow_percent(), auto.stop_percent()),
            (45, 50),
            "stopping at half dead keeps the log under twice its live size"
        );
        assert_eq!(
            Options::default().auto_compact,
            Some(AutoCompact::default())
        );
    }
}
