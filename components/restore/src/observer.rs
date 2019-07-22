// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::*;

use kvproto::metapb::Region;
use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, Request, Response};
use protobuf::RepeatedField;
use tikv::raftstore::coprocessor::{
    AdminObserver, Coprocessor, ObserverContext, QueryObserver, Result as CopResult,
};

/// `RestoreObserver` observes apply progress.
#[derive(Clone)]
pub struct RestoreObserver {
    // TODO: maybe we should replace it with a thread safe channel.
    tx: Arc<Mutex<mpsc::Sender<(u64, Region)>>>,
}

impl RestoreObserver {
    pub fn new() -> (RestoreObserver, mpsc::Receiver<(u64, Region)>) {
        let (tx, rx) = mpsc::channel();
        let ro = RestoreObserver {
            tx: Arc::new(Mutex::new(tx)),
        };
        (ro, rx)
    }

    fn notify(&self, region: Region, index: u64) {
        debug!("restore observer notify";
            "region_id" => region.get_id(),
            "index" => index);
        if let Err(e) = self.tx.lock().unwrap().send((index, region)) {
            error!("restore observer notify fail"; "error" => ?e);
        }
    }
}

impl Coprocessor for RestoreObserver {}

impl AdminObserver for RestoreObserver {
    fn post_apply_admin(&self, ctx: &mut ObserverContext<'_>, _: &mut AdminResponse, index: u64) {
        self.notify(ctx.region().clone(), index)
    }
}

impl QueryObserver for RestoreObserver {
    fn post_apply_query(
        &self,
        ctx: &mut ObserverContext<'_>,
        _: &mut RepeatedField<Response>,
        index: u64,
    ) {
        self.notify(ctx.region().clone(), index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::fixture_region;

    #[test]
    fn test_observe() {
        let (region, _) = fixture_region();
        let mut ctx = ObserverContext::new(&region);

        let (observer, rx) = RestoreObserver::new();

        let mut admin_req = AdminRequest::new();
        let mut admin_resp = AdminResponse::new();
        observer
            .pre_propose_admin(&mut ctx, &mut admin_req)
            .unwrap();
        rx.try_recv().unwrap_err();
        observer.pre_apply_admin(&mut ctx, &admin_req);
        rx.try_recv().unwrap_err();
        observer.post_apply_admin(&mut ctx, &mut admin_resp, 1);
        assert_eq!((1, region.clone()), rx.try_recv().unwrap());

        let query_req = Request::new();
        let query_resp = Response::new();
        observer
            .pre_propose_query(&mut ctx, &mut vec![query_req.clone()].into())
            .unwrap();
        rx.try_recv().unwrap_err();
        observer.pre_apply_query(&mut ctx, &[query_req]);
        rx.try_recv().unwrap_err();
        observer.post_apply_query(&mut ctx, &mut vec![query_resp.clone()].into(), 1);
        assert_eq!((1, region.clone()), rx.try_recv().unwrap());
    }
}
