use tikv::raftstore::coprocessor::*;
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;

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

impl CmdObserver for CdcObserver {
    fn on_batch_executed(&self, batch: &[CmdBatch]) {
        self.sink
            .schedule(Task::MultiBatch {
                multi: batch.to_vec(),
            })
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    }
}
