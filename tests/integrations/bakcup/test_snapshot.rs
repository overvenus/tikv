// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures::Future;

use kvproto::metapb;
use kvproto::raft_cmdpb::{RaftCmdResponse, RaftResponseHeader};
use kvproto::raft_serverpb::*;
use raft::eraftpb::{ConfChangeType, MessageType};

use engine::*;
use test_raftstore::*;
use tikv::pd::PdClient;
use tikv::raftstore::store::*;
use tikv::raftstore::Result;
use tikv_util::config::ReadableDuration;
use tikv_util::HandyRwLock;

fn run_cluster_with_backup<T: Simulator>(cluster: &mut Cluster<T>, backup_nodes: &[u64]) -> u64 {
    let reigon_id = cluster.run_conf_change();
    for node_id in backup_nodes {
        cluster.stop_node(*node_id);
        let mut cfg = cluster.cfg.clone();
        cfg.server.backup_mode = true;
        cluster.run_node_with_config(*node_id, cfg).unwrap();
    }
    reigon_id
}

fn check_snapshot(bm: &BackupManager, region_id: u64, cf_count: usize) {
    let region_list = bm
        .storage
        .list_dir(&bm.current_dir().join(format!("{}", region_id)))
        .unwrap();
    // snapshot dir;
    assert_eq!(region_list.len(), 1);
    let snap_list = bm.storage.list_dir(&region_list[0]).unwrap();
    // cf files and a meta file.
    assert_eq!(snap_list.len(), cf_count + 1);
}

fn test_simple_snapshot(cluster: &mut Cluster<ServerCluster>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let r1 = run_cluster_with_backup(cluster, &[2, 3]);

    // Now region 1 only has peer (1, 1);
    let (key, value) = (b"k1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, b"k1");
    // add learner (2, 2) to region 1.
    pd_client.must_add_peer(r1, new_learner_peer(2, 2));
    pd_client.must_none_pending_peer(new_learner_peer(2, 2));

    // check only default is not emptry.
    let backup_mgr2 = cluster.sim.rl().get_backup_mgr(2).unwrap();
    check_snapshot(&backup_mgr2, r1, 1);

    let (key, value) = (b"k2", b"v2");
    cluster.must_put_cf(CF_LOCK, key, value);
    assert_eq!(cluster.get_cf(CF_LOCK, key), Some(value.to_vec()));
    // CF_WRITE requires key length >= 8.
    let (key, value) = (b"kkkk_kkkk3", b"v3");
    cluster.must_put_cf(CF_WRITE, key, value);
    assert_eq!(cluster.get_cf(CF_WRITE, key), Some(value.to_vec()));

    // add learner (3, 3) to region 1.
    pd_client.must_add_peer(r1, new_learner_peer(3, 3));
    pd_client.must_none_pending_peer(new_learner_peer(3, 3));

    // check all cfs are not emptry.
    let backup_mgr3 = cluster.sim.rl().get_backup_mgr(3).unwrap();
    check_snapshot(&backup_mgr3, r1, 3);
}

#[test]
fn test_server_simple_backup_snapshot() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_simple_snapshot(&mut cluster);
}
