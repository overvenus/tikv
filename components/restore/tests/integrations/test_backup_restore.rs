// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

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

    sleep_ms(1000);

    // Finish backup.
    let dep = backup_mgr2.step(BackupState::Stop).unwrap();

    sleep_ms(1000);
    cluster.shutdown();
    sleep_ms(1000);

    // Create a restore manager.
    let backup_path = format!("{}", dep);
    let storage = backup_mgr2.storage.clone();
    let restore_mgr = RestoreManager::new(backup_path.into(), storage).unwrap();
    let mut tasks: Vec<_> = restore_mgr.executor().unwrap().tasks().collect();
    assert_eq!(tasks.len(), 1); // snapshot
    let (mut tasks, _w) = tasks.remove(0);

    // Create restore system.
    let mut cfg = Config::new();
    let (router, raft_system) = create_raft_batch_system(&cfg);
    let (engines, snap_mgr) = create_engines_and_snap_mgr(router.clone());
    cfg.raftdb_path = engines.raft.path().to_owned();
    let cvrt = Converter::new(2 /* learner_store */, snap_mgr.clone());
    let mut system = RestoreSystem::new(
        1,
        2, /* learner_store */
        cfg,
        engines,
        router.clone(),
        raft_system,
        snap_mgr,
    );
    system.bootstrap().unwrap();
    system.start().unwrap();

    let msgs = cvrt.convert(Region::new(), tasks.remove(0).data);

    // Create region.
    router.send_raft_message(msgs[0].clone()).unwrap();
    // Wait for create region.
    thread::sleep(time::Duration::from_millis(200));

    let learner = msgs[1].get_to_peer().clone();
    router.send_raft_message(msgs[1].clone()).unwrap();
    // Wait for apply snapshot.
    thread::sleep(time::Duration::from_millis(200));
    // Must send successfully.
    let mut req = RaftCmdRequest::new();
    req.mut_header().set_region_id(r1);
    req.mut_header().set_peer(learner);
    req.set_status_request(new_region_detail_cmd());
    let (tx, rx) = mpsc::channel();
    let cmd = RaftCommand::new(
        req,
        Callback::Read(Box::new(move |resp| {
            tx.send(resp).unwrap();
        })),
    );
    router.send_raft_command(cmd).unwrap();
    let resp = rx.recv_timeout(time::Duration::from_secs(5)).unwrap();
    assert!(!format!("{:?}", resp).contains("region has not been initialized"));

    system.stop().unwrap();
}
