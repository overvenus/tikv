mod test_service;
mod test_simple;

use test_raftstore::*;
use tikv_util::config::{ReadableDuration, ReadableSize};

pub fn configure_for_backup<T: Simulator>(cluster: &mut Cluster<T>) {
    // Avoid log compaction which flush log files unexpectedly.
    cluster.cfg.raft_store.raft_log_gc_threshold = 1000;
    cluster.cfg.raft_store.raft_log_gc_count_limit = 1000;
    cluster.cfg.raft_store.raft_log_gc_size_limit = ReadableSize::mb(20);
    cluster.cfg.raft_store.snap_mgr_gc_tick_interval = ReadableDuration::hours(50);
}
