use std::sync::{Arc, RwLock};

use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, RaftResponseHeader, Request, Response};
use tikv::raftstore::coprocessor::*;
use tikv_util::collections::HashSet;
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;
use crate::RawEvent;

#[derive(Clone)]
pub struct CdcObserver {
    // A shared set of region ID.
    observe_region: Arc<RwLock<HashSet<u64>>>,
    sink: Scheduler<Task>,
}

impl CdcObserver {
    pub fn new(sink: Scheduler<Task>) -> CdcObserver {
        CdcObserver {
            observe_region: Arc::default(),
            sink,
        }
    }

    pub fn register_region(&self, region_id: u64) {
        self.observe_region.write().unwrap().insert(region_id);
    }

    pub fn deregister_region(&self, region_id: u64) {
        self.observe_region.write().unwrap().remove(&region_id);
    }

    pub fn is_registered(&self, region_id: u64) -> bool {
        self.observe_region.read().unwrap().contains(&region_id)
    }
}

impl Coprocessor for CdcObserver {
    fn start(&self) {}
    fn stop(&self) {}
}

impl AdminObserver for CdcObserver {
    fn pre_apply_admin(&self, ctx: &mut ObserverContext<'_>, index: u64, req: &AdminRequest) {
        if !self.is_registered(ctx.region().id) {
            return;
        }

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
        if !self.is_registered(ctx.region().id) {
            return;
        }

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
        if !self.is_registered(ctx.region().id) {
            return;
        }

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
        if !self.is_registered(ctx.region().id) {
            return;
        }

        let event = RawEvent::DataResponse {
            region_id: ctx.region().get_id(),
            index,
            header: header.clone(),
        };
        self.sink.schedule(Task::RawEvent(event)).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvproto::metapb::Region;
    use std::time::Duration;

    #[test]
    fn test_register_and_deregister() {
        let (scheduler, rx) = tikv_util::worker::dummy_scheduler();
        let observer = CdcObserver::new(scheduler);

        // obsever should ommit requests if regions are not registered.
        let mut region1 = Region::new();
        region1.set_id(1);
        let mut ctx = ObserverContext::new(&region1);
        observer.pre_apply_query(&mut ctx, 1, &[]);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // Must receive changes after registered.
        observer.register_region(1);
        observer.pre_apply_query(&mut ctx, 1, &[]);
        rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap();

        // Must not receive changes after deregistered.
        observer.deregister_region(1);
        observer.pre_apply_query(&mut ctx, 1, &[]);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();
    }
}
