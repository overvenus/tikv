// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use backup::*;
use kvproto::metapb::*;
use kvproto::raft_serverpb::*;
use protobuf::Message as MessageTrait;
use raft::eraftpb::*;

use tikv::raftstore::store::fsm::{ApplyRouter, RaftRouter};
use tikv::raftstore::store::SnapManager;

pub struct Runner {
    raft_router: RaftRouter,
    apply_router: ApplyRouter,
}

impl RestoreRunable for Runner {
    fn run(&mut self, task: RestoreTask) {
        unimplemented!()
    }
}
