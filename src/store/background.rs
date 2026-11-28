//! The compaction thread, and the stalls that keep writes from outrunning
//! it (DESIGN.md, When it runs).

use std::sync::atomic::Ordering;
use std::sync::{Condvar, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::{Inner, hold};
use crate::fs::Fs;

/// How long a leader sleeps before writing its group while the log is past
/// `AutoCompact::slow_percent`, once per group, as LevelDB does per write.
const SLOWDOWN: Duration = Duration::from_millis(1);

#[derive(Default)]
pub(super) struct Background {
    signal: Mutex<Signal>,
    /// Signalled when compaction may be due, or the store is closing.
    wake: Condvar,
    /// Signalled when a compaction ends, or the thread panics.
    caught_up: Condvar,
    /// Taken to join the thread when the store is dropped.
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl Background {
    pub(super) fn started(&self, thread: JoinHandle<()>) {
        *hold(&self.thread) = Some(thread);
    }
}

#[derive(Default)]
struct Signal {
    due: bool,
    stop: bool,
}

/// Where a compaction is, for tests that act in the middle of one.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    /// The offsets of the copies are reserved; nothing is copied yet.
    Reserved,
    /// The copies are durable under the segment's name, and in the list.
    Copied,
    /// The index points at the copies; the cleaned segments are still there.
    Repointed,
}

/// Poisons the store and wakes writers waiting for compaction if the
/// thread panics: they would otherwise wait for a compaction that never
/// comes. It ends otherwise only when the store is dropped, so no write is
/// waiting.
struct Exit<'a, F: Fs>(&'a Inner<F>);

impl<F: Fs> Drop for Exit<'_, F> {
    fn drop(&mut self) {
        if thread::panicking() {
            self.0.poisoned.store(true, Ordering::Release);
            log::error!(
                "the compaction thread panicked; writes are off until the store is reopened"
            );
            self.0.wake_stalled();
        }
    }
}

impl<F: Fs> Inner<F> {
    /// The compaction thread: compacts each time a group of writes makes it
    /// due, until the store is dropped.
    pub(super) fn compact_in_background(&self) {
        let background = self.background.as_ref().expect("a thread of its own");
        let _exit = Exit(self);
        loop {
            {
                let mut signal = hold(&background.signal);
                while !signal.due && !signal.stop {
                    signal = background
                        .wake
                        .wait(signal)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                if signal.stop {
                    return;
                }
                signal.due = false;
            }
            let compacting = self.compacting();
            let cleaned = self.compact_if_due(&compacting);
            drop(compacting);
            if let Ok(true) = cleaned {
                // Writes went on meanwhile, and may have made it due again.
                hold(&background.signal).due = true;
            }
            self.wake_stalled();
        }
    }

    /// Stops the compaction thread, after the compaction it is running if
    /// any, and waits for it.
    pub(super) fn stop_compactor(&self) {
        let Some(background) = &self.background else {
            return;
        };
        hold(&background.signal).stop = true;
        background.wake.notify_one();
        if let Some(thread) = hold(&background.thread).take() {
            // A panic there has poisoned the store and been reported.
            let _ = thread.join();
        }
    }

    /// After a group that wrote something: wakes the compaction thread if
    /// compaction is due, or, without one, compacts on this thread unless a
    /// compaction is running. Not while automatic compaction is off after a
    /// failure, or the store is poisoned: it would do nothing.
    pub(super) fn after_group(&self) {
        let Some(auto) = self.auto_compact else {
            return;
        };
        if self.auto_failed.load(Ordering::Relaxed) || self.writable().is_err() {
            return;
        }
        let Some(background) = &self.background else {
            if let Some(compacting) = self.try_compacting() {
                let _ = self.compact_if_due(&compacting);
            }
            return;
        };
        let (dead, len) = self.dead_and_len();
        if auto.is_due(dead, len) {
            hold(&background.signal).due = true;
            background.wake.notify_one();
        }
    }

    /// Before a leader writes its group: sleeps once if the log is past
    /// `slow_percent`, then waits while it is past `stop_percent`, until
    /// compaction brings it under. Not while automatic compaction is off
    /// after a failure, or the store is poisoned: nothing would bring it
    /// under.
    pub(super) fn throttle(&self) {
        let (Some(background), Some(auto)) = (&self.background, self.auto_compact) else {
            return;
        };
        let can_catch_up =
            || !self.auto_failed.load(Ordering::Relaxed) && !self.poisoned.load(Ordering::Acquire);
        let (dead, len) = self.dead_and_len();
        if can_catch_up() && auto.slows(dead, len) {
            thread::sleep(SLOWDOWN);
        }
        let stopped = || {
            let (dead, len) = self.dead_and_len();
            can_catch_up() && auto.stops(dead, len)
        };
        let mut signal = hold(&background.signal);
        if !stopped() {
            return;
        }
        #[cfg(test)]
        self.stalls.fetch_add(1, Ordering::Relaxed);
        while stopped() {
            signal.due = true;
            background.wake.notify_one();
            signal = background
                .caught_up
                .wait(signal)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Wakes writers waiting for compaction to catch up, to look again.
    pub(super) fn wake_stalled(&self) {
        if let Some(background) = &self.background {
            let _signal = hold(&background.signal);
            background.caught_up.notify_all();
        }
    }

    #[cfg(test)]
    pub(super) fn between(&self, phase: Phase) {
        if let Some(between) = self.between.get() {
            between(self, phase);
        }
    }

    /// Calls `hook` at each phase of every compaction from now on.
    #[cfg(test)]
    pub(crate) fn on_phase(&self, hook: impl Fn(&Inner<F>, Phase) + Send + Sync + 'static) {
        assert!(
            self.between.set(Box::new(hook)).is_ok(),
            "one hook per store"
        );
    }

    #[cfg(test)]
    pub(crate) fn fs(&self) -> &F {
        &self.fs
    }
}
