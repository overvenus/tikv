use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use engine::Engines;
use kvproto::metapb::Region;
use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, RaftResponseHeader, Request, Response};
use tikv::raftstore::coprocessor::*;
use tikv::raftstore::store::RegionSnapshot;
use tikv_util::collections::HashMap;
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;
use crate::RawEvent;

#[derive(Clone)]
pub struct CdcObserver {
    // A shared set of region ID.
    observe_region: Arc<RwLock<HashMap<u64, AtomicBool>>>,
    sink: Scheduler<Task>,
    engines: Engines,
}

impl CdcObserver {
    pub fn new(sink: Scheduler<Task>, engines: Engines) -> CdcObserver {
        CdcObserver {
            observe_region: Arc::default(),
            sink,
            engines,
        }
    }

    pub fn register_region(&self, region_id: u64) {
        self.observe_region
            .write()
            .unwrap()
            .insert(region_id, AtomicBool::new(false));
    }

    pub fn deregister_region(&self, region_id: u64) {
        self.observe_region.write().unwrap().remove(&region_id);
    }

    fn maybe_registered(&self, region: &Region) -> bool {
        let region_id = region.get_id();
        if let Some(initial) = self.observe_region.read().unwrap().get(&region_id) {
            let initial = initial.swap(true, Ordering::Relaxed);
            if !initial {
                // load all locks
                let region_snapshot =
                    RegionSnapshot::from_raw(self.engines.kv.clone(), region.clone());
                let load_locks = Task::LoadLocks { region_snapshot };
                info!("schedule load locks"; "region_id" => region_id);
                self.sink.schedule(load_locks).unwrap();
            }
            true
        } else {
            false
        }
    }
}

impl Coprocessor for CdcObserver {
    fn start(&self) {}
    fn stop(&self) {}
}

impl AdminObserver for CdcObserver {
    fn pre_apply_admin(&self, ctx: &mut ObserverContext<'_>, index: u64, req: &AdminRequest) {
        if !self.maybe_registered(ctx.region()) {
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
        if !self.maybe_registered(ctx.region()) {
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
        if !self.maybe_registered(ctx.region()) {
            return;
        }
        if index == 0 {
            // TODO(cdc): hack, index == 0 means this is triggered by
            // register_cmd_observer. What it needs is a call to maybe_registered
            // which in turn sends load lock and init resolver.
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
        if !self.maybe_registered(ctx.region()) {
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
    use tempfile::Builder;

    #[test]
    fn test_register_and_deregister() {
        let path = Builder::new()
            .prefix("test_cdc_observer")
            .tempdir()
            .unwrap();
        let db = Arc::new(
            engine::rocks::util::new_engine(
                path.path().join("db").to_str().unwrap(),
                None,
                engine::ALL_CFS,
                None,
            )
            .unwrap(),
        );
        let engines = Engines::new(db.clone(), db, false /* shared_block_cache */);
        let (scheduler, rx) = tikv_util::worker::dummy_scheduler();
        let observer = CdcObserver::new(scheduler, engines);

        // obsever should ommit requests if regions are not registered.
        let mut region1 = Region::new();
        region1.set_id(1);
        let mut ctx = ObserverContext::new(&region1);
        observer.pre_apply_query(&mut ctx, 1, &[]);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // Must receive changes after registered.
        observer.register_region(1);
        observer.pre_apply_query(&mut ctx, 1, &[]);
        // It first send a load lock event.
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::LoadLocks { .. } => (),
            _ => panic!("unexpected task"),
        };
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::RawEvent { .. } => (),
            _ => panic!("unexpected task"),
        };

        // Must not receive changes after deregistered.
        observer.deregister_region(1);
        observer.pre_apply_query(&mut ctx, 1, &[]);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();
    }
}
