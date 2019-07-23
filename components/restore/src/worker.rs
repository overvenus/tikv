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

    fn restore_snapshot(&mut self, region_id: u64, mut msgs: Vec<RaftMessage>) {
        assert_eq!(msgs.len(), 2 /* create peer and snapshot */);
        let frist_message = msgs.remove(0);
        let snap_message = msgs.remove(0);

        let (tx, rx) = mpsc::channel();
        let mut peer_msg = PeerMsg::RestoreMessage(RestoreMessage {
            msg: snap_message, // The actuall snapshot message.
            callback: Callback::Read(Box::new(move |resp| {
                tx.send(resp).unwrap();
            })),
        });

        // Create region.
        self.router.send_raft_message(frist_message).unwrap();

        let start = Instant::now();
        loop {
            // Wait for create region.
            thread::sleep(Duration::from_millis(100));

            // Try send snapshot message.
            if let Err(e) = self.router.send(region_id, peer_msg) {
                peer_msg = e.into_inner();
            } else {
                let resp = rx.recv_timeout(Duration::from_secs(1)).unwrap();
                assert!(!resp.response.get_header().has_error(), "{:?}", resp);
                return;
            }
            let dur = start.elapsed();
            if dur < Duration::from_secs(5) {
                warn!("restore apply slow"; "take" => ?dur);
            }
        }
    }

    fn restore_entries(&mut self, region_id: u64, msgs: Vec<RaftMessage>) {
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
                    warn!("restore apply slow"; "take" => ?dur);
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
