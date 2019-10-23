use std::sync::{Arc, RwLock};

use raft::StateRole;
use tikv::raftstore::coprocessor::*;
use tikv::raftstore::Error as RaftStoreError;
use tikv_util::collections::HashSet;
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;
use crate::Error as CdcError;

#[derive(Clone)]
pub struct CdcObserver {
    sink: Scheduler<Task>,
    observe_regions: Arc<RwLock<HashSet<u64>>>,
}

impl CdcObserver {
    pub fn new(sink: Scheduler<Task>) -> CdcObserver {
        CdcObserver {
            sink,
            observe_regions: Arc::default(),
        }
    }

    pub fn subscribe_region(&self, region_id: u64) {
        self.observe_regions.write().unwrap().insert(region_id);
    }

    pub fn unsubscribe_region(&self, region_id: u64) {
        self.observe_regions.write().unwrap().remove(&region_id);
    }

    pub fn is_subscribed(&self, region_id: u64) -> bool {
        self.observe_regions.read().unwrap().contains(&region_id)
    }
}

impl Coprocessor for CdcObserver {
    fn start(&self) {}
    fn stop(&self) {}
}

impl CmdObserver for CdcObserver {
    fn on_batch_executed(&self, batch: &[CmdBatch]) {
        if let Err(e) = self.sink.schedule(Task::MultiBatch {
            multi: batch.to_vec(),
        }) {
            warn!("schedule cdc task failed"; "error" => ?e);
        }
    }
}

impl RoleObserver for CdcObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        if role != StateRole::Leader {
            let region_id = ctx.region().get_id();
            if self.is_subscribed(region_id) {
                let store_err = RaftStoreError::NotLeader(region_id, None);
                if let Err(e) = self.sink.schedule(Task::Deregister {
                    region_id,
                    id: Err(CdcError::Request(store_err.into())),
                }) {
                    warn!("schedule cdc task failed"; "error" => ?e);
                }
            }
        }
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

        observer.on_batch_executed(&[CmdBatch::new(1)]);
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::MultiBatch { .. } => (),
            _ => panic!("unexpected task"),
        };

        // Does not send unsubscribed region events.
        let mut region = Region::default();
        region.set_id(1);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        observer.subscribe_region(1);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::Deregister { region_id, id } => {
                assert_eq!(region_id, 1);
                assert!(id.is_err());
            }
            _ => panic!("unexpected task"),
        };

        // No event if it changes to leader.
        observer.on_role_change(&mut ctx, StateRole::Leader);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // No event if it is unsubscribed.
        observer.unsubscribe_region(1);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // No event if it is unsubscribed.
        region.set_id(999);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();
    }
}
