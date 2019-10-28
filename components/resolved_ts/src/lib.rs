use std::cmp;
use std::collections::BTreeMap;
use tikv_util::collections::HashSet;

pub struct Resolver {
    // start_ts -> locked keys.
    locks: BTreeMap<u64, HashSet<Vec<u8>>>,
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

    pub fn locks(&self) -> &BTreeMap<u64, HashSet<Vec<u8>>> {
        &self.locks
    }

    pub fn track_lock(&mut self, start_ts: u64, key: Vec<u8>) {
        self.locks.entry(start_ts).or_default().insert(key);
    }

    pub fn untrack_lock(&mut self, start_ts: u64, commit_ts: Option<u64>, key: Vec<u8>) {
        if let Some(commit_ts) = commit_ts {
            assert!(
                self.resolved_ts.map_or(true, |rts| commit_ts > rts),
                "{}@{}, commit@{} > {:?}",
                hex::encode_upper(key),
                start_ts,
                commit_ts,
                self.resolved_ts
            );
            assert!(
                self.min_ts.map_or(true, |mts| commit_ts > mts),
                "{}@{}, commit@{} > {:?}",
                hex::encode_upper(key),
                start_ts,
                commit_ts,
                self.min_ts
            );
        }

        let entry = self.locks.get_mut(&start_ts);
        assert!(
            entry.is_some(),
            "{}@{} is not tracked",
            hex::encode_upper(key),
            start_ts
        );
        let locked_keys = entry.unwrap();
        assert!(
            locked_keys.remove(&key),
            "{}@{} is not tracked, {:?}",
            hex::encode_upper(key),
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
    use tikv::storage::Key;

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
                Event::Unlock(1, Some(2), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Lock(3, Key::from_raw(b"a")),
                Event::Unlock(3, Some(4), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
            ],
            vec![
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Unlock(1, Some(2), Key::from_raw(b"a")),
                Event::Lock(1, Key::from_raw(b"b")),
                Event::Resolve(2, 1),
            ],
            vec![
                Event::Lock(2, Key::from_raw(b"a")),
                Event::Unlock(2, Some(3), Key::from_raw(b"a")),
                Event::Resolve(2, 2),
                // Pessimistic txn may write a smaller start_ts.
                Event::Lock(1, Key::from_raw(b"a")),
                Event::Resolve(2, 2),
                Event::Unlock(1, Some(4), Key::from_raw(b"a")),
                Event::Resolve(3, 3),
            ],
        ];

        for (i, case) in cases.into_iter().enumerate() {
            let mut resolver = Resolver::new();
            resolver.init();
            for e in case.clone() {
                match e {
                    Event::Lock(start_ts, key) => {
                        resolver.track_lock(start_ts, key.into_raw().unwrap())
                    }
                    Event::Unlock(start_ts, commit_ts, key) => {
                        resolver.untrack_lock(start_ts, commit_ts, key.into_raw().unwrap())
                    }
                    Event::Resolve(min_ts, expect) => {
                        assert_eq!(resolver.resolve(min_ts).unwrap(), expect, "case {}", i)
                    }
                }
            }

            let mut resolver = Resolver::new();
            for e in case {
                match e {
                    Event::Lock(start_ts, key) => {
                        resolver.track_lock(start_ts, key.into_raw().unwrap())
                    }
                    Event::Unlock(start_ts, commit_ts, key) => {
                        resolver.untrack_lock(start_ts, commit_ts, key.into_raw().unwrap())
                    }
                    Event::Resolve(min_ts, _) => {
                        assert_eq!(resolver.resolve(min_ts), None, "case {}", i)
                    }
                }
            }
        }
    }
}
