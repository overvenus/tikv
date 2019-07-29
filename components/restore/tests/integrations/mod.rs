#![recursion_limit = "200"]
#![allow(unused_imports)]

#[macro_use(
    kv,
    slog_kv,
    slog_trace,
    slog_debug,
    slog_info,
    slog_warn,
    slog_error,
    slog_record,
    slog_b,
    slog_log,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;

mod test_backup_restore;
mod test_restore;

use std::path::*;
use std::sync::*;
use std::{thread, time};

use engine::{Engines, CF_DEFAULT};
use kvproto::metapb::{Peer, Region};
use kvproto::raft_serverpb::*;
use tikv::raftstore::store::fsm::*;
use tikv::raftstore::store::msg::*;
use tikv::raftstore::store::SnapManager;
use tikv::raftstore::store::{util, Config};

use restore::RestoreSystem;

pub fn fixture_region() -> (Region, u64) {
    let mut region = Region::new();
    region.mut_region_epoch().set_version(1);
    region.mut_region_epoch().set_conf_ver(1);
    region.mut_peers().push(util::new_peer(1, 2));
    region.mut_peers().push(util::new_peer(2, 3));
    region.mut_peers().push(util::new_learner_peer(3, 4));
    (region, 3)
}

pub fn create_engines_and_snap_mgr(
    router: RaftRouter,
    dir: &Path,
) -> (engine::Engines, SnapManager) {
    let kv_path = dir.join("kv");
    let cfg = tikv::config::TiKvConfig::default();
    let cache = cfg.storage.block_cache.build_shared_cache();
    let kv_db_opt = cfg.rocksdb.build_opt();
    let kv_cfs_opt = cfg.rocksdb.build_cf_opts(&cache);
    let engine = Arc::new(
        engine::rocks::util::new_engine_opt(kv_path.to_str().unwrap(), kv_db_opt, kv_cfs_opt)
            .unwrap(),
    );
    let raft_path = dir.join(Path::new("raft"));
    let raft_engine = Arc::new(
        engine::rocks::util::new_engine(raft_path.to_str().unwrap(), None, &[CF_DEFAULT], None)
            .unwrap(),
    );
    let snap_path = dir.join(Path::new("snap"));
    let s = snap_path.as_path().to_str().unwrap().to_owned();
    let snap_mgr = SnapManager::new(s, Some(router), None);
    (Engines::new(engine, raft_engine, cache.is_some()), snap_mgr)
}

pub fn init() {
    static INIT: Once = ONCE_INIT;
    INIT.call_once(|| {
        test_util::init_log_for_test();
    })
}
