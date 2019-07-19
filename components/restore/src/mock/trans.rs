// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_serverpb::RaftMessage;
use tikv::raftstore::store::*;
use tikv::raftstore::Result;

#[derive(Clone)]
pub struct Trans {}

impl Trans {
    pub fn new() -> Trans {
        Trans {}
    }
}

impl Transport for Trans {
    fn send(&mut self, msg: RaftMessage) -> Result<()> {
        info!("trans send"; "msg" => ?msg);
        Ok(())
    }
    fn flush(&mut self) {}
}
