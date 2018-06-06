// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! The coprocessor framework to support distributed computing.

mod checksum;
pub mod codec;
mod dag;
mod endpoint;
mod error;
pub mod local_metrics;
mod metrics;
mod readpool_context;
mod statistics;
mod util;

pub use self::endpoint::err_resp;
pub use self::error::{Error, Result};
pub use self::readpool_context::Context as ReadPoolContext;

use std::time::Duration;

use kvproto::{coprocessor as coppb, kvrpcpb};

use util::time::Instant;

/// The type number of DAG requests.
pub const REQ_TYPE_DAG: i64 = 103;
/// The type number of ANALYZE requests.
pub const REQ_TYPE_ANALYZE: i64 = 104;
/// The type number of CHECKSUM requests.
pub const REQ_TYPE_CHECKSUM: i64 = 105;

const SINGLE_GROUP: &[u8] = b"SingleGroup";

/// A convenience wrapper for `Result<(Option<coppb::Response>, bool), error::Error>`.
///
/// `HandlerStreamStepResult` the return type of `handle_streaming_request`,
///  and works like a `futures::Poll`.
///
/// * `Ok((resp, true))` means the streaming request is finished.
/// * `Ok((resp, false))` means the streaming request has further response,
///    we should keep calling `RequestHandler::handle_streaming_request`.
/// * `Err(e)` means that an error was encountered when attempting to complete
///    the streaming request, we should stop it and return the error.
type HandlerStreamStepResult = Result<(Option<coppb::Response>, bool)>;

/// RequestHandler is abastraction over a request with its context to compute
/// the result.
///
/// It is required to be `Send` so we can spawn it to a worker thread.
trait RequestHandler: Send {
    /// Try to handle the request and return the `Response`.
    fn handle_request(&mut self) -> Result<coppb::Response> {
        panic!("unary request is not supported for this handler");
    }

    /// Try to process the streaming request.
    fn handle_streaming_request(&mut self) -> HandlerStreamStepResult {
        panic!("streaming request is not supported for this handler");
    }

    /// Collect runtime metrics to the handler.
    fn collect_metrics_into(&mut self, _metrics: &mut self::dag::executor::ExecutorMetrics) {
        // Do nothing by default
    }

    /// Convenience function for turning self into a trait object.
    fn into_boxed(self) -> Box<RequestHandler>
    where
        Self: 'static + Sized,
    {
        box self
    }
}

/// A ReqContext carries some properties for a requst.
#[derive(Debug)]
pub struct ReqContext {
    /// A context comes from a request.
    pub context: kvrpcpb::Context,
    /// Whether is a table scan request.
    pub table_scan: bool,
    /// A transaction start timestamp.
    pub txn_start_ts: u64,
    /// A start time of a request.
    pub start_time: Instant,
    /// A deadline of a requset.
    pub deadline: Instant,
}

impl ReqContext {
    /// Crate a new `ReqContext`.
    pub fn new(ctx: &kvrpcpb::Context, txn_start_ts: u64, table_scan: bool) -> ReqContext {
        let start_time = Instant::now_coarse();
        ReqContext {
            context: ctx.clone(),
            table_scan,
            txn_start_ts,
            start_time,
            deadline: start_time,
        }
    }

    /// Set the max duration a request can take.
    pub fn set_max_handle_duration(&mut self, request_max_handle_duration: Duration) {
        self.deadline = self.start_time + request_max_handle_duration;
    }

    /// Return a request's tag.
    #[inline]
    pub fn get_scan_tag(&self) -> &'static str {
        if self.table_scan {
            "select"
        } else {
            "index"
        }
    }

    /// Check if a request exceed its deadline.
    pub fn check_if_outdated(&self) -> Result<()> {
        let now = Instant::now_coarse();
        if self.deadline <= now {
            let elapsed = now.duration_since(self.start_time);
            return Err(Error::Outdated(elapsed, self.get_scan_tag()));
        }
        Ok(())
    }
}

pub use self::dag::{ScanOn, Scanner};
pub use self::endpoint::{Host as EndPointHost, RequestTask, Task as EndPointTask,
                         DEFAULT_REQUEST_MAX_HANDLE_SECS};
