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

#[derive(Default)]
pub struct RegionPrgress {
    pub region: Region,
    pub last_index: u64,
    pub last_term: u64,
}

pub struct Converter {
    store_id: u64,
    snap_base: PathBuf,
    progress: HashMap<u64, RegionPrgress>,
}

impl Converter {
    pub fn new(store_id: u64, snap_mgr: SnapManager) -> Converter {
        Converter {
            store_id,
            snap_base: snap_mgr.base_path(),
            progress: HashMap::new(),
        }
    }

    pub fn update_region(&mut self, region: Region) {
        let region_id = region.get_id();
        self.progress.entry(region_id).or_default().region = region;
    }

    pub fn update_last_entry(&mut self, region_id: u64, index: u64, term: u64) {
        let mut progress = self.progress.get_mut(&region_id).unwrap();
        progress.last_index = index;
        progress.last_term = term;
    }

    pub fn progress(&self, region_id: u64) -> &RegionPrgress {
        &self.progress[&region_id]
    }

    fn new_raft_message(&self, region_id: u64) -> RaftMessage {
        let progress = &self.progress[&region_id];
        let region = &progress.region;
        let epoch = region.get_region_epoch().to_owned();
        let (leader, learner) = find_leader_and_learner(self.store_id, region);
        let mut msg = RaftMessage::new();
        msg.set_region_id(region_id);
        msg.set_to_peer(learner);
        msg.set_from_peer(leader);
        msg.set_region_epoch(epoch);
        msg
    }

    pub fn snapshot_to_messages(&self, region: &Region, mut snap: Snapshot) -> Vec<RaftMessage> {
        // The first msg creates a peer.
        let region_id = region.get_id();
        let mut first_msg = self.new_raft_message(region_id);
        first_msg
            .mut_message()
            .set_msg_type(MessageType::MsgHeartbeat);

        let mut snap_msg = first_msg.clone();
        fill_snapshot_conf(&mut snap, region);
        snap_msg
            .mut_message()
            .set_msg_type(MessageType::MsgSnapshot);
        snap_msg.mut_message().set_snapshot(snap);
        vec![first_msg, snap_msg]
    }

    pub fn entries_to_messages(&self, region_id: u64, es: Vec<Entry>) -> Vec<RaftMessage> {
        let progress = &self.progress[&region_id];
        let commit_idx = es.last().unwrap().get_index();
        let mut commit_msg = self.new_raft_message(region_id);
        commit_msg
            .mut_message()
            .set_msg_type(MessageType::MsgAppend);
        commit_msg.mut_message().set_commit(commit_idx);
        commit_msg.mut_message().set_entries(es.into());
        commit_msg.mut_message().set_log_term(progress.last_term);
        commit_msg.mut_message().set_index(progress.last_index);
        vec![commit_msg]
    }

    pub fn convert(&mut self, region_id: u64, data: Data) -> Vec<RaftMessage> {
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
                let region = snap_data.get_region();
                self.update_region(region.clone());
                let index = s.meta.1.get_metadata().get_index();
                let term = s.meta.1.get_metadata().get_term();
                let msgs = self.snapshot_to_messages(region, s.meta.1.clone());
                self.update_last_entry(region.get_id(), index, term);
                msgs
            }
            Data::Logs(es) => {
                assert_ne!(0, region_id);
                let index = es.last().unwrap().get_index();
                let term = es.last().unwrap().get_term();
                let msgs = self.entries_to_messages(region_id, es);
                self.update_last_entry(region_id, index, term);
                msgs
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

    use crate::mock::fixture_region;

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
