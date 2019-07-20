// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::fs;
use std::path::*;
use std::sync::Arc;

use backup::*;
use kvproto::metapb::*;
use kvproto::raft_serverpb::*;
use protobuf::Message as MessageTrait;
use raft::eraftpb::*;

use tikv::raftstore::store::{SnapEntry, SnapKey, SnapManager};

pub struct Converter {
    store_id: u64,
    snap_base: PathBuf,
}

impl Converter {
    pub fn new(store_id: u64, snap_mgr: SnapManager) -> Converter {
        Converter {
            store_id,
            snap_base: snap_mgr.base_path(),
        }
    }

    pub fn snapshot_to_messages(&self, mut snap: Snapshot) -> Vec<RaftMessage> {
        let mut snap_data = RaftSnapshotData::new();
        snap_data.merge_from_bytes(snap.get_data()).unwrap();
        let region = snap_data.get_region();
        let epoch = snap_data.get_region().get_region_epoch().to_owned();
        let (from, to) = find_leader_and_learner(self.store_id, region);

        // The first msg creates a peer.
        let mut first_msg = RaftMessage::new();
        first_msg.set_region_id(region.get_id());
        first_msg.set_to_peer(to);
        first_msg.set_from_peer(from);
        first_msg
            .mut_message()
            .set_msg_type(MessageType::MsgHeartbeat);
        first_msg.set_region_epoch(epoch);

        let mut snap_msg = first_msg.clone();
        fill_snapshot_conf(&mut snap, region);
        snap_msg
            .mut_message()
            .set_msg_type(MessageType::MsgSnapshot);
        snap_msg.mut_message().set_snapshot(snap);
        vec![first_msg, snap_msg]
    }

    pub fn entries_to_messages(&self, mut snap: Snapshot) -> Vec<RaftMessage> {
        let mut snap_data = RaftSnapshotData::new();
        snap_data.merge_from_bytes(snap.get_data()).unwrap();
        let region = snap_data.get_region();
        let epoch = snap_data.get_region().get_region_epoch().to_owned();
        let (from, to) = find_leader_and_learner(self.store_id, region);

        // The first msg creates a peer.
        let mut first_msg = RaftMessage::new();
        first_msg.set_region_id(region.get_id());
        first_msg.set_to_peer(to);
        first_msg.set_from_peer(from);
        first_msg
            .mut_message()
            .set_msg_type(MessageType::MsgHeartbeat);
        first_msg.set_region_epoch(epoch);

        let mut snap_msg = first_msg.clone();
        fill_snapshot_conf(&mut snap, region);
        snap_msg
            .mut_message()
            .set_msg_type(MessageType::MsgSnapshot);
        snap_msg.mut_message().set_snapshot(snap);
        vec![first_msg, snap_msg]
    }

    pub fn convert(&self, region: Region, data: Data) -> Vec<RaftMessage> {
        match data {
            Data::Snapshot(s) => {
                // Write cf files
                let cfs = [&s.default, &s.write, &s.lock];
                for f in &cfs {
                    let p = self.snap_base.join(&f.0);
                    debug!("convert snapshot"; "file" => ?p, "size" => &f.1.len());
                    fs::write(p, &f.1).unwrap();
                }
                // Write snapshot meta file.
                let p = self.snap_base.join(&s.meta.0);
                debug!("convert snapshot"; "file" => ?p, "meta" => ?s.meta.1);

                let mut snap_data = RaftSnapshotData::new();
                snap_data.merge_from_bytes(s.meta.1.get_data()).unwrap();
                fs::write(p, snap_data.get_meta().write_to_bytes().unwrap()).unwrap();
                self.snapshot_to_messages(s.meta.1.clone())
            }
            Data::Logs(es) => {
                // TODO: how to choose a correct peer as leader?
                //       peer can be remove.
                assert_ne!(0, region.get_id());
                vec![]
            }
        }
    }
}

fn fill_snapshot_conf(snap: &mut Snapshot, region: &Region) {
    let conf = snap.mut_metadata().mut_conf_state();
    for peer in region.get_peers() {
        if peer.get_is_learner() {
            conf.mut_learners().push(peer.get_id());
        } else {
            conf.mut_nodes().push(peer.get_id());
        }
    }
}

fn find_leader_and_learner(learner_store: u64, region: &Region) -> (Peer, Peer) {
    let mut pr = None;
    let mut lr = None;
    for peer in region.get_peers() {
        if pr.is_none() {
            pr = Some(peer);
        }
        let store_id = peer.get_store_id();
        if peer.get_is_learner() && peer.get_store_id() == learner_store {
            lr = Some(peer.to_owned());
        } else if pr.as_ref().unwrap().get_store_id() > store_id {
            pr = Some(peer);
        }
    }
    (pr.unwrap().to_owned(), lr.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tikv::raftstore::store::util;

    fn fixture_region() -> (Region, u64) {
        let mut region = Region::new();
        region.mut_region_epoch().set_version(1);
        region.mut_region_epoch().set_conf_ver(1);
        region.mut_peers().push(util::new_peer(1, 2));
        region.mut_peers().push(util::new_peer(2, 3));
        region.mut_peers().push(util::new_learner_peer(3, 4));
        (region, 3)
    }

    #[test]
    fn test_find_leader_and_learner() {
        let (region, learner_store) = fixture_region();
        let (leader, learner) = find_leader_and_learner(learner_store, &region);
        assert_ne!(learner, leader);
        assert_eq!(learner.get_store_id(), learner_store);
    }

    #[test]
    fn test_fill_snapshot_conf() {
        let (region, _) = fixture_region();
        let mut snap_data = RaftSnapshotData::new();
        snap_data.set_region(region.clone());
        let mut snap = Snapshot::new();
        snap.set_data(snap_data.write_to_bytes().unwrap());

        fill_snapshot_conf(&mut snap, &region);
        assert_eq!(snap.get_metadata().get_conf_state().get_learners(), &[4]);
        assert!(snap
            .get_metadata()
            .get_conf_state()
            .get_nodes()
            .contains(&2));
        assert!(snap
            .get_metadata()
            .get_conf_state()
            .get_nodes()
            .contains(&3));
    }
}
