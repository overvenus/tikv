use engine::CF_LOCK;
use resolved_ts::Resolver;
use tikv::raftstore::store::{RegionIterator, RegionSnapshot};
use tikv::storage::mvcc::Lock;
use tikv::storage::Key;
use tikv::storage::{CFStatistics, Cursor, CursorBuilder};

use crate::Result;

pub struct LockScanner {
    region_snapshot: RegionSnapshot,
    statistics: CFStatistics,
    lock_cursor: Cursor<RegionIterator>,
}

impl LockScanner {
    pub fn new(region_snapshot: RegionSnapshot) -> Result<LockScanner> {
        let lock_cursor = CursorBuilder::new(&region_snapshot, CF_LOCK)
            .range(None, None)
            .fill_cache(false)
            .build()?;

        Ok(LockScanner {
            region_snapshot,
            lock_cursor,
            statistics: CFStatistics::default(),
        })
    }

    pub fn build_resolver(&mut self) -> Result<Resolver> {
        let mut resolver = Resolver::new();
        if !self.lock_cursor.seek_to_first(&mut self.statistics) {
            info!("no lock found";
                "region_id" => self.region_snapshot.get_region().get_id());
            resolver.init();
            return Ok(resolver);
        }

        loop {
            if !self.lock_cursor.valid().unwrap() {
                resolver.init();
                let rts = resolver.resolve(0);
                info!("resolver initialized";
                    "region_id" => self.region_snapshot.get_region().get_id(),
                    "resolved_ts" => rts,
                    "lock_count" => resolver.locks().len());
                return Ok(resolver);
            }

            let locked_key = self.lock_cursor.key(&mut self.statistics);
            let lock = {
                let lock_value = self.lock_cursor.value(&mut self.statistics);
                Lock::parse(lock_value)?
            };
            resolver.track_lock(
                lock.ts,
                Key::from_encoded_slice(locked_key).to_raw().unwrap(),
            );

            self.lock_cursor.next(&mut self.statistics);
        }
    }

    #[allow(dead_code)]
    pub fn statistics(&self) -> &CFStatistics {
        &self.statistics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvproto::metapb::Region;
    use tikv::storage::mvcc::tests::*;
    use tikv::storage::RocksEngine;

    #[test]
    fn test_build_resolver() {
        let tmp = tempfile::TempDir::new().unwrap();

        let mut engine = RocksEngine::new(
            tmp.path().to_str().unwrap(),
            engine::ALL_CFS,
            None,
            false, /* shared_block_cache */
        )
        .unwrap();
        engine.use_data_key(true);

        let region_id = 1;
        let mut region = Region::new();
        region.set_id(region_id);
        region.mut_peers().push(Default::default());

        let locked_key = vec![
            // key, start_ts
            (b"key1", 4),
            (b"key2", 5),
            (b"key3", 6),
        ];
        for (key, start_ts) in locked_key {
            // lock key
            must_prewrite_put(&engine, key, key, key, start_ts);
        }

        let cases = vec![
            (0, 0, 3), // "" -> "", 3 locks
            (1, 3, 2), // "key1" -> "key3", 2 locks
            (1, 0, 3),
            (0, 3, 2),
            (2, 0, 2),
            (0, 2, 1),
            (4, 0, 0),
        ];
        let build_key = |i| {
            if i == 0 {
                vec![]
            } else {
                Key::from_raw(format!("key{}", i).as_bytes()).into_encoded()
            }
        };
        for (start, end, lock_count) in cases {
            let start_key = build_key(start);
            let end_key = build_key(end);
            let mut r = region.clone();
            r.set_start_key(start_key);
            r.set_end_key(end_key);
            let snap = RegionSnapshot::from_raw(engine.get_rocksdb(), r);
            let mut lock_scanner = LockScanner::new(snap).unwrap();
            let resolver = lock_scanner.build_resolver().unwrap();
            let locks = resolver.locks();
            assert_eq!(locks.len(), lock_count, "{:?}", (start, end, locks));
            resolver.resolved_ts().unwrap();
        }
    }
}
