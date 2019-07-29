// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::*;
use std::sync::mpsc;
use std::thread;
use std::time::*;

use backup::*;
use engine::rocks::Writable;
use engine::Iterable;
use kvproto::metapb::*;
use kvproto::raft_serverpb::*;
use protobuf::Message as MessageTrait;
use raft::eraftpb::*;

use tikv::raftstore::store::fsm::*;
use tikv::raftstore::store::SnapManager;
use tikv::raftstore::store::*;

use super::Converter;

pub struct Runner {
    pub router: RaftRouter,
    pub apply_notify: mpsc::Receiver<(u64, Region)>,
    cvrt: Converter,
    engines: engine::Engines,
    store_id: u64,
}

impl Runner {
    pub fn new(
        router: RaftRouter,
        apply_notify: mpsc::Receiver<(u64, Region)>,
        store_id: u64,
        snap_path: &Path,
        engines: engine::Engines,
    ) -> Runner {
        let cvrt = Converter::new(store_id, snap_path);
        Runner {
            router,
            apply_notify,
            cvrt,
            engines,
            store_id,
        }
    }

    fn restore_snapshot<I: Iterator<Item = RaftMessage>>(&mut self, region_id: u64, msgs: I) {
        info!("restore snapshot"; "region_id" => region_id);

        let start = Instant::now();
        let mut is_frist_message = true;
        for m in msgs {
            if is_frist_message {
                // Create region.
                self.router.send_raft_message(m.clone()).unwrap();

                let start = Instant::now();
                loop {
                    // Wait for create region.
                    thread::sleep(Duration::from_millis(100));
                    let dur = start.elapsed();
                    if dur > Duration::from_secs(5) {
                        warn!("restore apply snapshot slow";
                            "take" => ?dur,
                            "region_id" => region_id);
                    }
                    if self
                        .router
                        .send(region_id, PeerMsg::RaftMessage(m.clone()))
                        .is_err()
                    {
                        continue;
                    } else {
                        // Send Ok mean the peer has been created.
                        break;
                    }
                }
                is_frist_message = false
            } else {
                let (tx, rx) = mpsc::channel();
                let peer_msg = PeerMsg::RestoreMessage(RestoreMessage {
                    msg: m, // The actuall snapshot message.
                    callback: Callback::Read(Box::new(move |resp| {
                        tx.send(resp).unwrap();
                    })),
                });

                // Send snapshot message.
                self.router.send(region_id, peer_msg).unwrap();

                loop {
                    // Wait for create region.
                    thread::sleep(Duration::from_millis(100));

                    match rx.recv_timeout(Duration::from_secs(1)) {
                        Ok(resp) => {
                            assert!(!resp.response.get_header().has_error(), "{:?}", resp);
                            return;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => (),
                        e @ Err(mpsc::RecvTimeoutError::Disconnected) => {
                            panic!("restore snapshot region {} {:?}", region_id, e);
                        }
                    }
                    let dur = start.elapsed();
                    if dur > Duration::from_secs(5) {
                        warn!("restore apply snapshot slow";
                            "take" => ?dur,
                            "region_id" => region_id);
                    }
                }
            }
        }
    }

    fn restore_entries<I: Iterator<Item = RaftMessage>>(&mut self, region_id: u64, msgs: I) {
        info!("restore entries"; "region_id" => region_id);

        let mut dur = Duration::new(0, 0);
        for msg in msgs {
            self.router
                .send(region_id, PeerMsg::RaftMessage(msg))
                .unwrap();
        }
        let last_index = self.cvrt.progress(region_id).last_index;
        loop {
            match self.apply_notify.recv_timeout(Duration::from_secs(1)) {
                Ok((index, region)) => {
                    if last_index == index {
                        self.cvrt.update_region(region);
                        break;
                    } else if last_index < index {
                        panic!("apply unknown entry at index {}", index);
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    dur += Duration::from_secs(1);
                    if dur > Duration::from_secs(5) {
                        warn!("restore apply entries slow";
                            "take" => ?dur,
                            "region_id" => region_id);
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => panic!("restore apply disconnected"),
            }
        }
    }
}

impl Restorable for Runner {
    fn restore(&mut self, task: RestoreTask) {
        let region_id = task.region_id;
        let is_snapshot = task.is_snapshot();
        let msgs = self.cvrt.convert(region_id, task.data);
        if is_snapshot {
            self.restore_snapshot(region_id, msgs);
        } else {
            self.restore_entries(region_id, msgs);
        }
    }

    fn restore_meta(&mut self) {
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let mut total_count = 0;
        let mut tombstone_count = 0;
        let mut merging_count = 0;
        let t = Instant::now();
        let wb = engine::WriteBatch::new();
        let raft_engine = self.engines.raft.clone();
        raft_engine
            .scan(start_key, end_key, false, |key, value| {
                let (region_id, suffix) = keys::decode_region_meta_key(key).unwrap();
                if suffix != keys::REGION_STATE_SUFFIX {
                    return Ok(true);
                }

                total_count += 1;

                let mut local_state =
                    protobuf::parse_from_bytes::<RegionLocalState>(value).unwrap();
                let region = local_state.get_region();
                if local_state.get_state() == PeerState::Tombstone {
                    tombstone_count += 1;
                    debug!("region is tombstone"; "region" => ?region, "store_id" => self.store_id);
                    return Ok(true);
                }

                // After restore there must be none peer in the Applying state.
                assert_ne!(local_state.get_state(), PeerState::Applying);

                // TODO: handle merging.
                if local_state.get_state() == PeerState::Merging {
                    info!("region is merging"; "region" => ?region, "store_id" => self.store_id);
                    merging_count += 1;
                }

                let peers = local_state.mut_region().take_peers();
                for mut pr in peers.into_vec() {
                    if pr.get_store_id() == self.store_id {
                        debug!("preserve region peer";
                        "region_id" => region_id, "peer" => ?pr);
                        pr.set_is_learner(false);
                        local_state.mut_region().mut_peers().push(pr);
                    } else {
                        debug!("remove region peer";
                        "region_id" => region_id, "peer" => ?pr);
                    }
                }
                wb.put(key, &local_state.write_to_bytes().unwrap()).unwrap();
                Ok(true)
            })
            .unwrap();
        let mut opt = engine::WriteOptions::new();
        opt.set_sync(true);
        raft_engine.write_opt(&wb, &opt).unwrap();

        info!(
            "restore meta";
            "store_id" => self.store_id,
            "region_count" => total_count,
            "tombstone_count" => tombstone_count,
            "merge_count" => merging_count,
            "take" => ?t.elapsed(),
        );
    }
}
