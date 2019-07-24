// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use backup::*;
use engine::*;
use kvproto::backup::*;
use kvproto::metapb;
use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb::RaftSnapshotData;
use protobuf::Message;
use raft::eraftpb::Snapshot;
use restore::*;
use test_raftstore::*;
use tikv::raftstore::store::*;
use tikv_util::HandyRwLock;

use super::*;

struct BackupCluster {
    cluster: Cluster<ServerCluster>,
    backup_mgr: Arc<BackupManager>,
    learner_store: u64,
}

impl BackupCluster {
    fn new() -> (BackupCluster, u64) {
        let mut cluster = new_server_cluster(0, 2);
        let pd_client = Arc::clone(&cluster.pd_client);
        // Disable default max peer count check.
        pd_client.disable_default_operator();

        // Now region 1 only has peer (1, 1);
        let frist_region = cluster.run_with_backup(&[2]);
        // Add learner (2, 2) to region 1.
        pd_client.must_add_peer(frist_region, new_learner_peer(2, 2));
        pd_client.must_none_pending_peer(new_learner_peer(2, 2));

        let backup_mgr = cluster.sim.rl().get_backup_mgr(2).unwrap();
        (
            BackupCluster {
                cluster,
                backup_mgr,
                learner_store: 2,
            },
            frist_region,
        )
    }

    fn backup_region(&self, region_id: u64) {
        self.backup_mgr.start_backup_region(region_id).unwrap();
        let (tx, rx) = mpsc::channel();
        let region_epoch = self.cluster.get_region_epoch(region_id);
        let request = PeerMsg::CasualMessage(CasualMessage::RequestSnapshot {
            region_epoch,
            start_cb: Callback::None,
            end_cb: Callback::Write(Box::new(move |resp| {
                tx.send(resp).unwrap();
            })),
        });
        let router = self.cluster.sim.rl().get_router(2).unwrap();
        router.send(region_id, request).unwrap();
        let resp = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            !resp.response.get_header().has_error(),
            "{:?}",
            resp.response
        );
    }

    fn restore(&mut self, backup_path: &str) -> (Engines, tempdir::TempDir) {
        // Create a restore manager.
        let storage = self.backup_mgr.storage.clone();
        let restore_mgr = RestoreManager::new(backup_path.into(), storage).unwrap();

        // Create restore system.
        let mut cfg = Config::new();
        let (router, raft_system) = create_raft_batch_system(&cfg);
        let dir = self.backup_mgr.tmp_dir("restore").unwrap();
        let (engines, snap_mgr) = create_engines_and_snap_mgr(router.clone(), dir.path());
        cfg.raftdb_path = engines.raft.path().to_owned();
        let snap_path = snap_mgr.base_path();
        let mut system = RestoreSystem::new(
            1,
            self.learner_store,
            cfg,
            engines.clone(),
            router.clone(),
            raft_system,
            snap_mgr,
        );
        system.bootstrap().unwrap();
        let apply_rx = system.start().unwrap();

        let runner = Runner::new(router.clone(), apply_rx, self.learner_store, &snap_path);
        restore_mgr.executor().unwrap().execute(runner);
        system.stop().unwrap();
        (engines, dir)
    }

    fn wait_learner_apply(&self, region_id: u64) {
        let leader = self.cluster.get_all_engines(1);
        let learner = self.cluster.get_all_engines(2);
        let raft_state_key = keys::raft_state_key(region_id);
        let raft_state = leader
            .raft
            .get_msg::<RaftLocalState>(&raft_state_key)
            .unwrap()
            .unwrap();
        let commit_idx = raft_state.get_hard_state().get_commit();

        let apply_state_key = keys::apply_state_key(region_id);
        let start = Instant::now();
        loop {
            let apply_state = learner
                .raft
                .get_msg::<RaftApplyState>(&apply_state_key)
                .unwrap()
                .unwrap();
            let applied_idx = apply_state.get_applied_index();
            if applied_idx >= commit_idx {
                return;
            }
            if start.elapsed() > Duration::from_secs(5) {
                panic!("learner apply too slow {:?} {:?}", raft_state, apply_state);
            }
            sleep_ms(100);
        }
    }
}

impl std::ops::Deref for BackupCluster {
    type Target = Cluster<ServerCluster>;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

impl std::ops::DerefMut for BackupCluster {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cluster
    }
}

#[test]
fn test_restore_snapshot_and_entries() {
    super::init();
    let (mut cluster, r1) = BackupCluster::new();

    // Write some data.
    let (key, value) = (b"kkkk_kkkk1", b"v1");
    cluster.must_put_cf(CF_DEFAULT, key, value);
    cluster.must_put_cf(CF_WRITE, key, value);
    cluster.must_put_cf(CF_LOCK, key, value);
    cluster.wait_learner_apply(r1);

    // Start backup regoin r1.
    cluster.backup_region(r1);

    // Write some raft log.
    let (key2, value2) = (b"kkkk_kkkk2", b"v2");
    cluster.must_put_cf(CF_DEFAULT, key2, value2);
    cluster.must_put_cf(CF_WRITE, key2, value2);
    cluster.must_put_cf(CF_LOCK, key2, value2);
    cluster.wait_learner_apply(r1);

    // Finish backup.
    let dep = cluster.backup_mgr.step(BackupState::Stop).unwrap();

    // TODO: remove sleep, we should reguard backup progress
    //       before stopping node.
    sleep_ms(1000);
    cluster.shutdown();

    let backup_path = format!("{}", dep);
    let (engines, _dir) = cluster.restore(&backup_path);

    for cf in &[CF_DEFAULT, CF_LOCK, CF_WRITE] {
        let handle = engines.kv.cf_handle(cf).unwrap();
        assert_eq!(
            engines
                .kv
                .get_cf(handle, &keys::data_key(key))
                .unwrap()
                .unwrap(),
            value
        );
        assert_eq!(
            engines
                .kv
                .get_cf(handle, &keys::data_key(key2))
                .unwrap()
                .unwrap(),
            value2
        );
    }
}

#[test]
fn test_restore_empty_cf() {
    super::init();
    let (mut cluster, r1) = BackupCluster::new();

    // Write some data except the default cf.
    let (key, value) = (b"kkkk_kkkk1", b"v1");
    cluster.must_put_cf(CF_WRITE, key, value);
    cluster.must_put_cf(CF_LOCK, key, value);
    cluster.wait_learner_apply(r1);

    // Start backup regoin r1.
    cluster.backup_region(r1);

    // Finish backup.
    let dep = cluster.backup_mgr.step(BackupState::Stop).unwrap();

    // TODO: remove sleep, we should reguard backup progress
    //       before stopping node.
    sleep_ms(1000);
    cluster.shutdown();

    let backup_path = format!("{}", dep);
    let (engines, _dir) = cluster.restore(&backup_path);

    for cf in &[CF_LOCK, CF_WRITE] {
        let handle = engines.kv.cf_handle(cf).unwrap();
        assert_eq!(
            engines
                .kv
                .get_cf(handle, &keys::data_key(key))
                .unwrap()
                .unwrap(),
            value
        );
    }
}

#[test]
fn test_restore_multi_snapshots() {
    super::init();
    let (mut cluster, r1) = BackupCluster::new();

    // Write some data except the default cf.
    let (key, value) = (b"kkkk_kkkk1", b"v1");
    cluster.must_put_cf(CF_DEFAULT, key, value);
    cluster.must_put_cf(CF_WRITE, key, value);
    cluster.must_put_cf(CF_LOCK, key, value);

    let (key2, value2) = (b"kkkk_kkkk2", b"v1");
    cluster.must_put_cf(CF_DEFAULT, key2, value2);
    cluster.must_put_cf(CF_WRITE, key2, value2);
    cluster.must_put_cf(CF_LOCK, key2, value2);
    cluster.wait_learner_apply(r1);

    let r = cluster.get_region(b"");
    cluster.must_split(&r, key2);
    let region1 = cluster.get_region(key);
    let region2 = cluster.get_region(key2);

    cluster.wait_learner_apply(region2.get_id());
    cluster.wait_learner_apply(region1.get_id());

    // Start backup regoin 1.
    cluster.backup_region(region1.get_id());
    // Start backup regoin 2.
    cluster.backup_region(region2.get_id());

    // Finish backup.
    let dep = cluster.backup_mgr.step(BackupState::Stop).unwrap();

    // TODO: remove sleep, we should reguard backup progress
    //       before stopping node.
    sleep_ms(1000);
    cluster.shutdown();

    let backup_path = format!("{}", dep);
    let (engines, _dir) = cluster.restore(&backup_path);

    for cf in &[CF_DEFAULT, CF_LOCK, CF_WRITE] {
        let handle = engines.kv.cf_handle(cf).unwrap();
        assert_eq!(
            engines
                .kv
                .get_cf(handle, &keys::data_key(key))
                .unwrap()
                .unwrap(),
            value
        );
        assert_eq!(
            engines
                .kv
                .get_cf(handle, &keys::data_key(key2))
                .unwrap()
                .unwrap(),
            value2
        );
    }
}

// TODO test if snapshots are removed(GC) before creating a peer.
