#[macro_use]
extern crate slog_global;

use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, RaftResponseHeader, Request, Response};

mod delegate;
mod endpoint;
mod observer;
mod service;

pub use endpoint::Endpoint;
pub use observer::CdcObserver;
pub use service::Service;

pub enum RawEvent {
    DataRequest {
        region_id: u64,
        index: u64,
        requests: Vec<Request>,
    },
    DataResponse {
        region_id: u64,
        index: u64,
        header: RaftResponseHeader,
    },
    AdminRequest {
        region_id: u64,
        index: u64,
        request: AdminRequest,
    },
    AdminResponse {
        region_id: u64,
        index: u64,
        header: RaftResponseHeader,
        response: AdminResponse,
    },
}
