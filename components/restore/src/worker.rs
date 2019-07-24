// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::*;
use std::sync::mpsc;
use std::thread;
use std::time::*;

use backup::*;
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
}

impl Runner {
    pub fn new(
        router: RaftRouter,
        apply_notify: mpsc::Receiver<(u64, Region)>,
        store_id: u64,
        snap_path: &Path,
    ) -> Runner {
        let cvrt = Converter::new(store_id, snap_path);
        Runner {
            router,
            apply_notify,
            cvrt,
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

impl RestoreRunable for Runner {
    fn run(&mut self, task: RestoreTask) {
        let region_id = task.region_id;
        let is_snapshot = task.is_snapshot();
        let msgs = self.cvrt.convert(region_id, task.data);
        if is_snapshot {
            self.restore_snapshot(region_id, msgs);
        } else {
            self.restore_entries(region_id, msgs);
        }
    }
}
