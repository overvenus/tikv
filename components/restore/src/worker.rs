// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::mpsc;
use std::time::*;

use backup::*;
use kvproto::metapb::*;
use kvproto::raft_serverpb::*;
use protobuf::Message as MessageTrait;
use raft::eraftpb::*;

use tikv::raftstore::store::fsm::{ApplyRouter, RaftRouter};
use tikv::raftstore::store::SnapManager;

pub struct Runner {
    pub router: RaftRouter,
    pub apply_notify: mpsc::Receiver<(u64, Region)>,
}

impl RestoreRunable for Runner {
    fn run(&mut self, _task: RestoreTask) {
        let mut dur = Duration::new(0, 0);
        loop {
            match self.apply_notify.recv_timeout(Duration::from_secs(1)) {
                Ok((_index, _region)) => {
                    // TODO: make sure the index reaches the last task's index
                    //       before handle next task.
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
