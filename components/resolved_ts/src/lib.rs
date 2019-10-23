use std::cmp;
use std::collections::BTreeMap;
use tikv::storage::Key;
use tikv_util::collections::HashSet;

pub struct Resolver {
    // start_ts -> locked keys.
    locks: BTreeMap<u64, HashSet<Key>>,
    resolved_ts: Option<u64>,
    min_ts: Option<u64>,
}

impl Resolver {
    pub fn new() -> Resolver {
        Resolver {
            locks: BTreeMap::new(),
            resolved_ts: None,
            min_ts: None,
        }
    }

    pub fn init(&mut self) {
        self.resolved_ts = Some(0);
    }

    pub fn resolved_ts(&self) -> Option<u64> {
        self.resolved_ts
    }

    pub fn locks(&self) -> &BTreeMap<u64, HashSet<Key>> {
        &self.locks
    }

    pub fn track_lock(&mut self, start_ts: u64, key: Key) {
        self.locks.entry(start_ts).or_default().insert(key);
    }

    pub fn untrack_lock(&mut self, start_ts: u64, commit_ts: Option<u64>, key: Key) {
        // This key is from the write cf which is alway appended a ts,
        // The ts must be removed before trying to remove tracked lock key.
        let truncated_key = key.truncate_ts().unwrap();
        if let Some(commit_ts) = commit_ts {
            assert!(
                self.resolved_ts.map_or(true, |rts| commit_ts > rts),
                "{}@{}, commit@{} > {:?}",
                truncated_key,
                start_ts,
                commit_ts,
                self.resolved_ts
            );
            assert!(
                self.min_ts.map_or(true, |mts| commit_ts > mts),
                "{}@{}, commit@{} > {:?}",
                truncated_key,
                start_ts,
                commit_ts,
                self.min_ts
            );
        }

        let entry = self.locks.get_mut(&start_ts);
        assert!(
            entry.is_some(),
            "{}@{} is not tracked",
            truncated_key,
            start_ts
        );
        let locked_keys = entry.unwrap();
        assert!(
            locked_keys.remove(&truncated_key),
            "{}@{} is not tracked, {:?}",
            truncated_key,
            start_ts,
            locked_keys,
        );
        if locked_keys.is_empty() {
            self.locks.remove(&start_ts);
        }
    }

    /// Try to advance resolved ts.
    /// Requirement of min_ts:
    ///   1. later commit_ts must be great than the min_ts.
    pub fn resolve(&mut self, min_ts: u64) -> Option<u64> {
        self.resolved_ts?;
        let min_start_ts = *self.locks.keys().next().unwrap_or(&min_ts);
        let new_resolved_ts = cmp::min(min_start_ts, min_ts);
        if let Some(old_resolved_ts) = self.resolved_ts {
            self.resolved_ts = Some(cmp::max(old_resolved_ts, new_resolved_ts));
        } else {
            self.resolved_ts = Some(new_resolved_ts);
        }
        if let Some(old_min_ts) = self.min_ts {
            self.min_ts = Some(cmp::max(old_min_ts, min_ts));
        } else {
            self.min_ts = Some(min_ts);
        }
        self.resolved_ts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    enum Event {
        Lock(u64, Key),
        Unlock(u64, Option<u64>, Key),
        // min_ts, expect
        Resolve(u64, u64),
    }

    #[test]
    fn test_resolve() {
        let cases = vec![
            vec![Event::Lock(1, Key::from_raw(b"a")), Event::Resolve(2, 1)],
            vec![
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Unlock(1, Some(2), Key::from_raw(b"a").append_ts(2)),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Lock(3, Key::from_raw(b"a")),
                Event::Unlock(3, Some(4), Key::from_raw(b"a").append_ts(4)),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Unlock(1, Some(2), Key::from_raw(b"a").append_ts(2)),
                Event::Lock(1, Key::from_raw(b"b")),
                Event::Resolve(2, 1),
            ],
            vec![
                Event::Lock(2, Key::from_raw(b"a")),
                Event::Unlock(2, Some(3), Key::from_raw(b"a").append_ts(3)),
                Event::Resolve(2, 2),
                // Pessimistic txn may write a smaller start_ts.
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Resolve(2, 2),
                Event::Unlock(1, Some(4), Key::from_raw(b"a").append_ts(4)),
                Event::Resolve(3, 3),
            ],
        ];

        for (i, case) in cases.into_iter().enumerate() {
            let mut resolver = Resolver::new();
            resolver.init();
            for e in case.clone() {
                match e {
                    Event::Lock(start_ts, key) => resolver.track_lock(start_ts, key),
                    Event::Unlock(start_ts, commit_ts, key) => {
                        resolver.untrack_lock(start_ts, commit_ts, key)
                    }
                    Event::Resolve(min_ts, expect) => {
                        assert_eq!(resolver.resolve(min_ts).unwrap(), expect, "case {}", i)
                    }
                }
            }

            let mut resolver = Resolver::new();
            for e in case {
                match e {
                    Event::Lock(start_ts, key) => resolver.track_lock(start_ts, key),
                    Event::Unlock(start_ts, commit_ts, key) => {
                        resolver.untrack_lock(start_ts, commit_ts, key)
                    }
                    Event::Resolve(min_ts, _) => {
                        assert_eq!(resolver.resolve(min_ts), None, "case {}", i)
                    }
                }
            }
        }
    }
}
