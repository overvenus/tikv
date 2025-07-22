use std::{
    ops::{Deref, DerefMut},
    panic::Location,
    sync::{Mutex, PoisonError},
    time::Duration,
};

mod metrics;

trait InstantExt {
    fn saturating_elapsed(&self) -> Duration;
}

impl InstantExt for std::time::Instant {
    #[inline]
    fn saturating_elapsed(&self) -> Duration {
        std::time::Instant::now().saturating_duration_since(*self)
    }
}

// #[derive(Eq, Hash, PartialEq, Debug)]
pub struct InstrumentedMutex<T> {
    inner: Mutex<T>,

    acquire_histogram: Histogram,
    hold_histogram: Histogram,
}

impl<T> InstrumentedMutex<T> {
    pub fn new(inner: T, name: &str) -> Self {
        InstrumentedMutex {
            inner: Mutex::new(inner),
            acquire_histogram: metrics::MUTEX_ACQUIRE_HISTOGRAM.with_label_values(&[name]),
            hold_histogram: metrics::MUTEX_HOLD_HISTOGRAM.with_label_values(&[name]),
        }
    }
}

mod trace {
    use super::*;

    impl<T> InstrumentedMutex<T> {
        #[track_caller]
        pub fn lock(&self) -> Result<MutexGuard<'_, T>, PoisonError<std::sync::MutexGuard<'_, T>>> {
            let caller = Location::caller();
            let now = std::time::Instant::now();
            let guard = self.inner.lock()?;
            let elapsed = now.saturating_elapsed();
            if elapsed.as_millis() > 2 {
                slog_global::warn!(
                    "dbg mutex lock took too long";
                    "elapsed" => ?elapsed,
                    "location" => %caller,
                );
            }
            self.acquire_histogram.observe(elapsed.as_nanos() as f64);
            Ok(MutexGuard {
                guard,
                location: caller,
                start: std::time::Instant::now(),
                hold_histogram: &self.hold_histogram,
            })
        }
    }

    pub struct MutexGuard<'a, T: ?Sized + 'a> {
        pub(super) guard: std::sync::MutexGuard<'a, T>,
        location: &'a Location<'a>,
        start: std::time::Instant,
        hold_histogram: &'a Histogram,
    }

    impl<T: ?Sized> Drop for MutexGuard<'_, T> {
        #[inline]
        fn drop(&mut self) {
            let elapsed = self.start.saturating_elapsed();
            self.hold_histogram.observe(elapsed.as_nanos() as f64);
            if elapsed.as_millis() > 2 {
                slog_global::warn!(
                    "dbg mutex lock hold too long";
                    "elapsed" => ?elapsed,
                    "location" => %self.location,
                );
            }
        }
    }
}
use prometheus::Histogram;
pub use trace::MutexGuard;

#[cfg(skip)]
mod no_trace {
    use super::*;

    impl<T> InstrumentedMutex<T> {
        #[track_caller]
        pub fn lock(&self) -> Result<MutexGuard<'_, T>, PoisonError<std::sync::MutexGuard<'_, T>>> {
            let guard = self.inner.lock()?;
            Ok(MutexGuard { guard })
        }
    }
    pub struct MutexGuard<'a, T: ?Sized + 'a> {
        pub(super) guard: std::sync::MutexGuard<'a, T>,
    }
}
#[cfg(skip)]
pub use no_trace::MutexGuard;

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &*self.guard
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut *self.guard
    }
}

#[cfg(skip)]
mod trace_stopwatch {
    pub struct StopWatch {
        caller: &'static str,
        location: &'static Location<'static>,
        timer: std::cell::Cell<std::time::Instant>,
        tag: &'static str,
    }

    impl StopWatch {
        #[track_caller]
        pub fn new(caller: &'static str) -> Self {
            StopWatch {
                location: Location::caller(),
                timer: std::cell::Cell::new(std::time::Instant::now()),
                caller,
                tag: "msg",
            }
        }

        pub fn ready(caller: &'static str) -> Self {
            StopWatch {
                location: Location::caller(),
                timer: std::cell::Cell::new(std::time::Instant::now()),
                caller,
                tag: "ready",
            }
        }

        pub fn end(caller: &'static str) -> Self {
            StopWatch {
                location: Location::caller(),
                timer: std::cell::Cell::new(std::time::Instant::now()),
                caller,
                tag: "end",
            }
        }

        #[track_caller]
        pub fn lap(&self) {
            let now = std::time::Instant::now();
            let elapsed = now.saturating_duration_since(self.timer.get());
            if elapsed.as_millis() > 2 {
                let location = Location::caller();
                slog_global::warn!(
                    "dbg stopwatch lap too long";
                    "elapsed" => ?elapsed,
                    "location" => %location,
                    "caller" => %self.caller,
                    "tag" => %self.tag,
                );
            }
            self.timer.set(now);
        }
    }

    impl Drop for StopWatch {
        #[inline]
        fn drop(&mut self) {
            let elapsed = self.timer.get().saturating_elapsed();
            if elapsed.as_millis() > 2 {
                slog_global::warn!(
                    "dbg stopwatch lap (drop) too long";
                    "elapsed" => ?elapsed,
                    "location" => %self.location,
                    "caller" => %self.caller,
                    "tag" => %self.tag,
                );
            }
        }
    }
}
#[cfg(skip)]
pub use trace_stopwatch::StopWatch;

mod no_trace_stopwatch {
    pub struct StopWatch {}

    impl StopWatch {
        pub fn new(_caller: &'static str) -> Self {
            StopWatch {}
        }

        pub fn ready(_caller: &'static str) -> Self {
            StopWatch {}
        }

        pub fn end(_caller: &'static str) -> Self {
            StopWatch {}
        }

        pub fn lap(&self) {}
    }
}
pub use no_trace_stopwatch::StopWatch;
