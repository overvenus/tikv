// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use engine::*;
use test_raftstore::*;
use tikv::raftstore::store::*;
use tikv_util::HandyRwLock;

use super::configure_for_backup;

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

#[test]
fn test_server_simple_snapshot() {
    let mut cluster = new_server_cluster(0, 4);
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    // Now region 1 only has peer (1, 1);
    let r1 = cluster.run_with_backup(&[2, 3, 4]);

    // Add learner (2, 2) to region 1.
    pd_client.must_add_peer(r1, new_learner_peer(2, 2));
    pd_client.must_none_pending_peer(new_learner_peer(2, 2));
    // Check all cfs are emptry.
    let backup_mgr2 = cluster.sim.rl().get_backup_mgr(2).unwrap();
    check_snapshot(&backup_mgr2, r1, 0);

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // Add learner (3, 3) to region 1.
    pd_client.must_add_peer(r1, new_learner_peer(3, 3));
    pd_client.must_none_pending_peer(new_learner_peer(3, 3));
    // Check only default cf is not emptry.
    let backup_mgr3 = cluster.sim.rl().get_backup_mgr(3).unwrap();
    check_snapshot(&backup_mgr3, r1, 1);

    // Check only default is not emptry.
    let (key, value) = (b"k2", b"v2");
    cluster.must_put_cf(CF_LOCK, key, value);
    assert_eq!(cluster.get_cf(CF_LOCK, key), Some(value.to_vec()));
    // CF_WRITE requires key length >= 8.
    let (key, value) = (b"kkkk_kkkk3", b"v3");
    cluster.must_put_cf(CF_WRITE, key, value);
    assert_eq!(cluster.get_cf(CF_WRITE, key), Some(value.to_vec()));

    // Add learner (4, 4) to region 1.
    pd_client.must_add_peer(r1, new_learner_peer(4, 4));
    pd_client.must_none_pending_peer(new_learner_peer(4, 4));
    // Check all cfs are not emptry.
    let backup_mgr4 = cluster.sim.rl().get_backup_mgr(4).unwrap();
    check_snapshot(&backup_mgr4, r1, 3);
}

#[test]
fn test_server_simple_replication() {
    let mut cluster = new_server_cluster(0, 2);
    configure_for_backup(&mut cluster);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    // Now region 1 only has peer (1, 1);
    let r1 = cluster.run_with_backup(&[2]);

    // Add learner (2, 2) to region 1.
    pd_client.must_add_peer(r1, new_learner_peer(2, 2));
    pd_client.must_none_pending_peer(new_learner_peer(2, 2));
    // Check all cfs are emptry.
    let backup_mgr2 = cluster.sim.rl().get_backup_mgr(2).unwrap();
    check_snapshot(&backup_mgr2, r1, 0);
    let check_list_dir = |region_id, count| {
        let list = backup_mgr2
            .storage
            .list_dir(&backup_mgr2.region_path(region_id))
            .unwrap();
        assert_eq!(list.len(), count, "{:?}", list,);
    };
    check_list_dir(r1, 1);

    // Write cmds does not flush log files.
    for _ in 0..20 {
        let (key, value) = (b"k1", b"v1");
        cluster.must_put(key, value);
    }
    // Only snapshot dir.
    check_list_dir(r1, 1);

    // Split cmds do flush log files.
    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k2");
    // Split is right derived by default.
    let region2 = cluster.get_region(b"k1");
    // TODO(backup): use readindex to make sure learner has applied latest committed logs.
    sleep_ms(500);
    // Snapshot dir and a log file.
    check_list_dir(r1, 2);
    // Raft peers always propose a raft log after became leader.
    check_list_dir(region2.get_id(), 1);
}

#[test]
fn test_server_simple_request_snasphot() {
    let mut cluster = new_server_cluster(0, 2);
    configure_for_backup(&mut cluster);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    // Now region 1 only has peer (1, 1);
    let r1 = cluster.run_with_backup(&[2]);

    // Add learner (2, 2) to region 1.
    pd_client.must_add_peer(r1, new_learner_peer(2, 2));
    pd_client.must_none_pending_peer(new_learner_peer(2, 2));
    // Check all cfs are emptry.
    let backup_mgr2 = cluster.sim.rl().get_backup_mgr(2).unwrap();
    check_snapshot(&backup_mgr2, r1, 0);

    // Write something then request a snapshot.
    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);

    let region_epoch = cluster.get_region_epoch(r1);
    let router2 = cluster.sim.read().unwrap().get_router(2).unwrap();
    let (tx, rx) = mpsc::channel();
    let req_snap = PeerMsg::CasualMessage(CasualMessage::RequestSnapshot {
        region_epoch,
        callback: Callback::Write(Box::new(move |resp: WriteResponse| {
            let resp = resp.response;
            assert!(!resp.get_header().has_error(), "{:?}", resp,);
            tx.send(()).unwrap();
        })),
    });
    router2.send(r1, req_snap).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let start = Instant::now();
    loop {
        let list = backup_mgr2
            .storage
            .list_dir(&backup_mgr2.region_path(r1))
            .unwrap();
        if list.len() == 2 {
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("should be 2 snapshot dir, {:?}", list);
        }
    }
}
