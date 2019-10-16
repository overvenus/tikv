#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate failure;

use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, RaftResponseHeader, Request};

mod delegate;
mod endpoint;
mod errors;
mod lock_scanner;
mod observer;
mod service;

pub use endpoint::Endpoint;
pub use errors::{Error, Result};
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
