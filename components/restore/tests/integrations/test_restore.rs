use std::path::*;
use std::sync::*;
use std::{thread, time};

use engine::{Engines, CF_DEFAULT};
use kvproto::metapb::*;
use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb::*;
use protobuf::Message;
use raft::eraftpb::*;
use restore::*;
use test_raftstore::*;
use tikv::raftstore::store::fsm::*;
use tikv::raftstore::store::msg::*;
use tikv::raftstore::store::SnapManager;
use tikv::raftstore::store::{util, Config};

use super::*;

#[test]
fn test_create_peer() {
    super::init();
    let mut cfg = Config::new();
    let (router, raft_system) = create_raft_batch_system(&cfg);
    let (engines, snap_mgr) = create_engines_and_snap_mgr(router.clone());
    cfg.raftdb_path = engines.raft.path().to_owned();

    let (region, learner_store) = fixture_region();
    let mut cvrt = Converter::new(learner_store, snap_mgr.clone());
    let mut system = RestoreSystem::new(
        1,
        learner_store,
        cfg,
        engines,
        router.clone(),
        raft_system,
        snap_mgr,
    );
    system.bootstrap().unwrap();
    system.start().unwrap();

    let mut snap_data = RaftSnapshotData::new();
    snap_data.set_region(region.clone());
    let mut snap = Snapshot::new();
    snap.set_data(snap_data.write_to_bytes().unwrap());
    snap.mut_metadata().set_index(6);
    snap.mut_metadata().set_term(6);
    cvrt.update_region(region.clone());
    let msgs = cvrt.snapshot_to_messages(&region, snap);
    let learner = msgs[0].get_to_peer().clone();
    router.send_raft_message(msgs[0].clone()).unwrap();
    thread::sleep(time::Duration::from_millis(200));

    // Must send successfully.
    let mut req = RaftCmdRequest::new();
    req.mut_header().set_region_id(region.get_id());
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
    let resp = rx.recv_timeout(time::Duration::from_millis(500)).unwrap();
    assert!(format!("{:?}", resp).contains("region has not been initialized"));

    system.stop().unwrap();
}

#[test]
fn test_bootstarp() {
    super::init();
    let mut cfg = Config::new();
    let (router, raft_system) = create_raft_batch_system(&cfg);
    let (engines, snap_mgr) = create_engines_and_snap_mgr(router.clone());
    cfg.raftdb_path = engines.raft.path().to_owned();

    let mut system = RestoreSystem::new(1, 2, cfg, engines, router.clone(), raft_system, snap_mgr);
    system.bootstrap().unwrap();
    system.bootstrap().unwrap_err();
}
