use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, RaftResponseHeader, Request, Response};
use tikv::raftstore::coprocessor::*;
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;
use crate::RawEvent;

#[derive(Clone)]
pub struct CdcObserver {
    sink: Scheduler<Task>,
}

impl CdcObserver {
    pub fn new(sink: Scheduler<Task>) -> CdcObserver {
        CdcObserver { sink }
    }
}

impl Coprocessor for CdcObserver {
    fn start(&self) {}
    fn stop(&self) {}
}

impl AdminObserver for CdcObserver {
    fn pre_apply_admin(&self, ctx: &mut ObserverContext<'_>, index: u64, req: &AdminRequest) {
        let event = RawEvent::AdminRequest {
            region_id: ctx.region().get_id(),
            index,
            request: req.clone(),
        };
        self.sink.schedule(Task::RawEvent(event)).unwrap();
    }

    fn post_apply_admin(
        &self,
        ctx: &mut ObserverContext<'_>,
        index: u64,
        header: &RaftResponseHeader,
        resp: &mut AdminResponse,
    ) {
        let event = RawEvent::AdminResponse {
            region_id: ctx.region().get_id(),
            index,
            header: header.clone(),
            response: resp.clone(),
        };
        self.sink.schedule(Task::RawEvent(event)).unwrap();
    }
}

impl QueryObserver for CdcObserver {
    fn pre_apply_query(&self, ctx: &mut ObserverContext<'_>, index: u64, reqs: &[Request]) {
        let event = RawEvent::DataRequest {
            region_id: ctx.region().get_id(),
            index,
            requests: reqs.to_vec(),
        };
        self.sink.schedule(Task::RawEvent(event)).unwrap();
    }

    fn post_apply_query(
        &self,
        ctx: &mut ObserverContext<'_>,
        index: u64,
        header: &RaftResponseHeader,
        _: &mut Vec<Response>,
    ) {
        let event = RawEvent::DataResponse {
            region_id: ctx.region().get_id(),
            index,
            header: header.clone(),
        };
        self.sink.schedule(Task::RawEvent(event)).unwrap();
    }
}
