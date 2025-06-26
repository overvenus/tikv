use std::{
    ops::{Deref, DerefMut},
    panic::Location,
    sync::{Mutex, PoisonError},
    time::Duration,
};

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
}

impl<T> InstrumentedMutex<T> {
    pub fn new(inner: T) -> Self {
        InstrumentedMutex {
            inner: Mutex::new(inner),
        }
    }

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
        Ok(MutexGuard {
            guard,
            location: caller,
            start: std::time::Instant::now(),
        })
    }
}

pub struct MutexGuard<'a, T: ?Sized + 'a> {
    guard: std::sync::MutexGuard<'a, T>,
    location: &'a Location<'a>,
    start: std::time::Instant,
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        let elapsed = self.start.saturating_elapsed();
        if elapsed.as_millis() > 2 {
            slog_global::warn!(
                "dbg mutex lock hold too long";
                "elapsed" => ?elapsed,
                "location" => %self.location,
            );
        }
    }
}

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

pub struct StopWatch {
    caller: &'static str,
    location: &'static Location<'static>,
    timer: std::cell::Cell<std::time::Instant>,
}

impl StopWatch {
    #[track_caller]
    pub fn new(caller: &'static str) -> Self {
        StopWatch {
            location: Location::caller(),
            timer: std::cell::Cell::new(std::time::Instant::now()),
            caller,
        }
    }

    #[track_caller]
    pub fn lap(&self) {
        let now = std::time::Instant::now();
        let elapsed = now.saturating_duration_since(self.timer.get());
        if elapsed.as_millis() > 2 {
            let location = Location::caller();
            slog_global::warn!(
                "dbg stopwatch lap hold too long";
                "elapsed" => ?elapsed,
                "location" => %location,
                "caller" => %self.caller,
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
                "dbg span took too long";
                "elapsed" => ?elapsed,
                "location" => %self.location,
                "caller" => %self.caller,
            );
        }
    }
}
