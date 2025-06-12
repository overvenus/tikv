use std::{
    panic::Location,
    sync::{LockResult, Mutex},
};

use tikv_util::warn;

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
    pub fn lock(&self) -> LockResult<std::sync::MutexGuard<'_, T>> {
        let now = std::time::Instant::now();
        let guard = self.inner.lock();
        let elapsed = now.elapsed();
        if elapsed.as_millis() > 2 {
            let caller = Location::caller();
            warn!(
                "dbg mutex lock took too long";
                "elapsed" => ?elapsed,
                "location" => %caller,
            );
        }
        guard
    }
}
