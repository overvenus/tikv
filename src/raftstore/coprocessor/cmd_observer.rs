// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::vec::IntoIter;

use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};

use super::*;

#[derive(Clone, Debug)]
pub struct Cmd {
    pub index: u64,
    pub request: RaftCmdRequest,
    pub response: RaftCmdResponse,
}

impl Cmd {
    pub fn new(index: u64, request: RaftCmdRequest, response: RaftCmdResponse) -> Cmd {
        Cmd {
            index,
            request,
            response,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CmdBatch {
    pub region_id: u64,
    cmds: Vec<Cmd>,
}

impl CmdBatch {
    pub fn new(region_id: u64) -> CmdBatch {
        CmdBatch {
            region_id,
            cmds: Vec::new(),
        }
    }

    pub fn push(&mut self, region_id: u64, cmd: Cmd) {
        assert_eq!(region_id, self.region_id);
        self.cmds.push(cmd)
    }

    pub fn into_iter(self, region_id: u64) -> IntoIter<Cmd> {
        assert_eq!(self.region_id, region_id);
        self.cmds.into_iter()
    }

    pub fn len(&self) -> usize {
        self.cmds.len()
    }
}

pub trait CmdObserver: Coprocessor {
    /// Hook to call after region is registerd to observe cmd.
    fn on_registered(&self, ctx: &mut ObserverContext<'_>);
    /// Hook to call after applying write request.
    fn on_batch_executed(&self, batch: &[CmdBatch]);
}
