use std::collections::VecDeque;
use std::fmt;

use futures::sync::mpsc::UnboundedSender;
use kvproto::cdcpb::*;
use tikv_util::collections::HashMap;
use tikv_util::worker::Runnable;
use tokio_threadpool::ThreadPool;

use crate::delegate::Delegate;
use crate::RawEvent;

pub enum Task {
    Register {
        request: ChangeDataRequest,
        sink: UnboundedSender<ChangeDataEvent>,
    },
    Deregister {
        region_id: u64,
    },
    RawEvent(RawEvent),
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("CdcTask");
        match self {
            Task::Register { ref request, .. } => de.field("request", request).finish(),
            Task::Deregister { ref region_id, .. } => de.field("region_id", region_id).finish(),
            Task::RawEvent(_) => de.field("raw_event", &"...").finish(),
        }
    }
}

pub struct Endpoint {
    capture_regions: HashMap<u64, Delegate>,
}

impl Endpoint {
    pub fn new() -> Endpoint {
        Endpoint {
            // TODO: config threadpool.
            capture_regions: HashMap::default(),
        }
    }

    fn on_deregister(&mut self, region_id: u64) {
        info!("cdc deregister region"; "region_id" => region_id);
        self.capture_regions.remove(&region_id);
    }

    pub fn on_register(
        &mut self,
        request: ChangeDataRequest,
        sink: UnboundedSender<ChangeDataEvent>,
    ) {
        let region_id = request.region_id;
        info!("cdc register region"; "region_id" => region_id);
        let delegate = Delegate {
            region_id,
            pending: VecDeque::new(),
            sink,
        };
        if self.capture_regions.insert(region_id, delegate).is_some() {
            // TODO: should we close the sink?
            warn!("replace region change data sink"; "region_id"=> region_id);
        }
    }

    pub fn on_raw_event(&mut self, event: RawEvent) {
        match event {
            RawEvent::DataRequest {
                region_id,
                index,
                requests,
            } => {
                if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                    delegate.on_data_requsts(index, requests)
                }
            }
            RawEvent::DataResponse {
                region_id,
                index,
                header,
            } => {
                if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                    delegate.on_data_responses(index, header)
                }
            }
            RawEvent::AdminRequest {
                region_id,
                index,
                request,
            } => {
                if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                    delegate.on_admin_requst(index, request)
                }
            }
            RawEvent::AdminResponse {
                region_id,
                index,
                header,
                response,
            } => {
                if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                    delegate.on_admin_response(index, header, response)
                }
            }
        }
    }
}

impl Runnable<Task> for Endpoint {
    fn run(&mut self, task: Task) {
        debug!("run cdc task"; "task" => %task);
        match task {
            Task::Register { request, sink } => self.on_register(request, sink),
            Task::Deregister { region_id } => self.on_deregister(region_id),
            Task::RawEvent(event) => self.on_raw_event(event),
        }
    }
}
