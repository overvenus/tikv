use core::hash::{BuildHasher, Hash};
use std::{
    borrow::Borrow, collections::hash_map::RandomState, fmt::Debug, panic::Location, time::Duration,
};

use dashmap::{
    iter::Iter,
    mapref::one::{Ref, RefMut},
    DashMap,
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

pub struct InstrumentedDashMap<K, V, S = RandomState> {
    inner: DashMap<K, V, S>,
}

impl<'a, K: 'a + Eq + Hash, V: 'a> InstrumentedDashMap<K, V, RandomState> {
    pub fn new() -> Self {
        InstrumentedDashMap {
            inner: DashMap::new(),
        }
    }
}

#[track_caller]
fn instrument<F, Out, K>(f: F, key: Option<K>, method: &str) -> Out
where
    F: FnOnce() -> Out,
    K: Debug,
{
    let caller = Location::caller();
    let now = std::time::Instant::now();

    let result = f();
    let elapsed = now.saturating_elapsed();
    if elapsed.as_millis() > 2 {
        slog_global::warn!(
            "dbg dash map access too long";
            "elapsed" => ?elapsed,
            "location" => %caller,
            "method" => method,
            "key" => ?key
        );
    }
    result
}

impl<'a, K: 'a + Eq + Hash + Debug + Copy, V: 'a, S: BuildHasher + Clone>
    InstrumentedDashMap<K, V, S>
{
    #[track_caller]
    pub fn remove<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Debug,
    {
        instrument(|| self.inner.remove(key), Some(key), "remove")
    }

    #[track_caller]
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        instrument(|| self.inner.insert(key, value), Some(key), "insert")
    }

    #[track_caller]
    pub fn get<Q>(&'a self, key: &Q) -> Option<Ref<'a, K, V, S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Debug,
    {
        instrument(|| self.inner.get(key), Some(key), "get")
    }

    #[track_caller]
    pub fn get_mut<Q>(&'a self, key: &Q) -> Option<RefMut<'a, K, V, S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Debug,
    {
        instrument(|| self.inner.get_mut(key), Some(key), "get_mut")
    }

    #[track_caller]
    pub fn iter(&'a self) -> Iter<'a, K, V, S, DashMap<K, V, S>> {
        instrument(|| self.inner.iter(), None::<&K>, "iter")
    }

    #[track_caller]
    pub fn clear(&self) {
        instrument(
            || {
                self.inner.clear();
            },
            None::<&K>,
            "clear",
        )
    }

    #[track_caller]
    pub fn capacity(&self) -> usize {
        instrument(|| self.inner.capacity(), None::<&K>, "capacity")
    }

    #[track_caller]
    pub fn len(&self) -> usize {
        instrument(|| self.inner.len(), None::<&K>, "len")
    }

    #[track_caller]
    pub fn shrink_to_fit(&self) {
        instrument(
            || {
                self.inner.shrink_to_fit();
            },
            None::<&K>,
            "shrink_to_fit",
        )
    }
}
