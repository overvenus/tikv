// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::*;

use kvproto::kvrpcpb::{Context, IsolationLevel};
use kvproto::metapb::*;
use tikv::storage::kv::Engine;
use tikv::storage::txn::{EntryBatch, SnapshotStore, TxnEntryScanner, TxnEntryStore};
use tikv::storage::{Key, Statistics};

use crate::metrics::*;
use crate::*;

#[derive(Debug)]
pub struct BackupRange {
    pub(crate) start_key: Option<Key>,
    pub(crate) end_key: Option<Key>,
    pub(crate) region: Region,
    pub(crate) leader: Peer,
}

impl BackupRange {
    /// Get entries from the scanner and save them to storage
    pub fn backup<E: Engine>(
        &self,
        writer: &mut BackupWriter,
        engine: &E,
        backup_ts: u64,
    ) -> Result<Statistics> {
        let mut ctx = Context::new();
        ctx.set_region_id(self.region.get_id());
        ctx.set_region_epoch(self.region.get_region_epoch().to_owned());
        ctx.set_peer(self.leader.clone());
        let snapshot = match engine.snapshot(&ctx) {
            Ok(s) => s,
            Err(e) => {
                error!("backup snapshot failed"; "error" => ?e);
                return Err(e.into());
            }
        };
        let snap_store = SnapshotStore::new(
            snapshot,
            backup_ts,
            IsolationLevel::Si,
            false, /* fill_cache */
        );
        let start_key = self.start_key.clone();
        let end_key = self.end_key.clone();
        let mut scanner = snap_store.entry_scanner(start_key, end_key).unwrap();

        let start = Instant::now();
        let mut batch = EntryBatch::with_capacity(1024);
        loop {
            if let Err(e) = scanner.scan_entries(&mut batch) {
                error!("backup scan entries failed"; "error" => ?e);
                return Err(e.into());
            };
            if batch.is_empty() {
                break;
            }
            debug!("backup scan entries"; "len" => batch.len());
            // Build sst files.
            if let Err(e) = writer.write(batch.drain(), true) {
                error!("backup build sst failed"; "error" => ?e);
                return Err(e);
            }
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["scan"])
            .observe(start.elapsed().as_secs_f64());
        let stat = scanner.take_statistics();
        Ok(stat)
    }
}
