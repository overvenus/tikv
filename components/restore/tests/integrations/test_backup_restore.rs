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
    cluster.must_put_cf(CF_DEFAULT, key, value);
    cluster.must_put_cf(CF_DEFAULT, key, value);
    cluster.must_put_cf(CF_DEFAULT, key, value);

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
    let mut tasks: Vec<_> = restore_mgr.executor().unwrap().tasks().collect();
    assert!(tasks.len() > 1, "{:?}", tasks); // snapshot + raft logs

    // Create restore system.
    let mut cfg = Config::new();
    let (router, raft_system) = create_raft_batch_system(&cfg);
    let (engines, snap_mgr) = create_engines_and_snap_mgr(router.clone());
    cfg.raftdb_path = engines.raft.path().to_owned();
    let mut cvrt = Converter::new(2 /* learner_store */, snap_mgr.clone());
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
    let apply_rx = system.start().unwrap();

    let (mut snap_msgs, _) = tasks.remove(0);
    let msgs = cvrt.convert(0, snap_msgs.remove(0).data);
    // Create region.
    router.send_raft_message(msgs[0].clone()).unwrap();
    // Wait for create region.
    thread::sleep(time::Duration::from_millis(200));

    let (tx, rx) = mpsc::channel();
    router
        .send(
            r1,
            PeerMsg::RestoreMessage(RestoreMessage {
                msg: msgs[1].clone(), // The actuall snapshot message.
                callback: Callback::Read(Box::new(move |resp| {
                    tx.send(resp).unwrap();
                })),
            }),
        )
        .unwrap();
    let resp = rx.recv_timeout(time::Duration::from_secs(5)).unwrap();
    assert!(!resp.response.get_header().has_error(), "{:?}", resp);

    let must_apply = |index| {
        let start = Instant::now();
        loop {
            let (apply_index, _) = apply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            if apply_index >= index {
                break;
            }
            if start.elapsed() >= Duration::from_secs(5) {
                panic!("apply too long");
            }
        }
    };
    // Restore raft logs.
    for (task, _) in tasks {
        for t in task {
            assert_eq!(r1, t.region_id);
            let msgs = cvrt.convert(r1, t.data);
            for m in msgs {
                router.send(r1, PeerMsg::RaftMessage(m)).unwrap();
            }
            let pr = cvrt.progress(r1);
            must_apply(pr.last_index);
        }
    }

    system.stop().unwrap();
}
