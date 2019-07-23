// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use backup::*;
use engine::*;
use kvproto::backup::*;
use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb::RaftSnapshotData;
use protobuf::Message;
use raft::eraftpb::Snapshot;
use restore::*;
use test_raftstore::*;
use tikv::raftstore::store::*;
use tikv_util::HandyRwLock;

use super::*;

#[test]
fn test_backup_and_restore_snapshot() {
    super::init();
    let mut cluster = new_server_cluster(0, 2);
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    // Now region 1 only has peer (1, 1);
    let r1 = cluster.run_with_backup(&[2]);
    // Add learner (2, 2) to region 1.
    pd_client.must_add_peer(r1, new_learner_peer(2, 2));
    pd_client.must_none_pending_peer(new_learner_peer(2, 2));

    // Start backup regoin r1.
    let backup_mgr2 = cluster.sim.rl().get_backup_mgr(2).unwrap();
    backup_mgr2.start_backup_region(r1).unwrap();

    // Write some data.
    let (key, value) = (b"kkkk_kkkk1", b"v1");
    cluster.must_put_cf(CF_DEFAULT, key, value);
    cluster.must_put_cf(CF_WRITE, key, value);
    cluster.must_put_cf(CF_LOCK, key, value);

    let (tx, rx) = mpsc::channel();
    let region_epoch = cluster.get_region_epoch(r1);
    let request = PeerMsg::CasualMessage(CasualMessage::RequestSnapshot {
        region_epoch,
        start_cb: Callback::None,
        end_cb: Callback::Write(Box::new(move |resp| {
            tx.send(resp).unwrap();
        })),
    });
    let router = cluster.sim.rl().get_router(2).unwrap();
    router.send(r1, request).unwrap();
    let resp = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        !resp.response.get_header().has_error(),
        "{:?}",
        resp.response
    );

    // Write some raft log.
    let (key2, value2) = (b"kkkk_kkkk2", b"v2");
    cluster.must_put_cf(CF_DEFAULT, key2, value2);
    cluster.must_put_cf(CF_WRITE, key2, value2);
    cluster.must_put_cf(CF_LOCK, key2, value2);

    // Finish backup.
    // TODO: remove sleep, we should reguard backup progress
    //       before stopping node.
    sleep_ms(1000);
    let dep = backup_mgr2.step(BackupState::Stop).unwrap();

    sleep_ms(1000);
    cluster.shutdown();
    sleep_ms(1000);

    // Create a restore manager.
    let backup_path = format!("{}", dep);
    let storage = backup_mgr2.storage.clone();
    let restore_mgr = RestoreManager::new(backup_path.into(), storage).unwrap();

    // Create restore system.
    let mut cfg = Config::new();
    let (router, raft_system) = create_raft_batch_system(&cfg);
    let (engines, snap_mgr) = create_engines_and_snap_mgr(router.clone());
    cfg.raftdb_path = engines.raft.path().to_owned();
    let snap_path = snap_mgr.base_path();
    let mut system = RestoreSystem::new(
        1,
        2, /* learner_store */
        cfg,
        engines.clone(),
        router.clone(),
        raft_system,
        snap_mgr,
    );
    system.bootstrap().unwrap();
    let apply_rx = system.start().unwrap();

    let runner = Runner::new(router.clone(), apply_rx, 2, &snap_path);
    restore_mgr.executor().unwrap().execute(runner);

    for cf in &[CF_DEFAULT, CF_LOCK, CF_WRITE] {
        let handle = engines.kv.cf_handle(cf).unwrap();
        assert_eq!(engines.kv.get_cf(handle, &keys::data_key(key)).unwrap().unwrap(), value);
        assert_eq!(engines.kv.get_cf(handle,&keys::data_key(key2)).unwrap().unwrap(), value2);
    }

    system.stop().unwrap();
}
