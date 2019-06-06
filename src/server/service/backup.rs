// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::raftstore::store::{BackupManager, Callback, CasualMessage};
use crate::server::metrics::*;
use crate::server::transport::RaftStoreRouter;
use crate::server::Error;
use futures::Future;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use kvproto::backup::{BackupRegionRequest, BackupRegionResponse, BackupRequest, BackupResponse};
use kvproto::backup_grpc;
use tikv_util::future::paired_future_callback;

/// Service handles the RPC messages for the `Tikv` service.
#[derive(Clone)]
pub struct Service<T: RaftStoreRouter + 'static> {
    // For handling raft messages.
    ch: T,
    // For handling backup.
    backup_mgr: Arc<BackupManager>,
}

impl<T: RaftStoreRouter + 'static> Service<T> {
    /// Constructs a new `Service` which provides the `Tikv` service.
    pub fn new(ch: T, backup_mgr: Arc<BackupManager>) -> Self {
        Service { ch, backup_mgr }
    }

    fn send_fail_status<M>(
        &self,
        ctx: RpcContext<'_>,
        sink: UnarySink<M>,
        err: Error,
        code: RpcStatusCode,
    ) {
        let status = RpcStatus::new(code, Some(format!("{}", err)));
        ctx.spawn(sink.fail(status).map_err(|_| ()));
    }
}

impl<T: RaftStoreRouter + 'static> backup_grpc::Backup for Service<T> {
    fn backup(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: BackupRequest,
        _sink: UnarySink<BackupResponse>,
    ) {
        unimplemented!()
    }

    fn backup_region(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: BackupRegionRequest,
        sink: UnarySink<BackupRegionResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.backup.start_coarse_timer();

        let region_id = req.get_context().get_region_id();
        let (cb, future) = paired_future_callback();
        let req = CasualMessage::RequestSnapshot {
            region_epoch: req.mut_context().take_region_epoch(),
            callback: Callback::Write(cb),
        };

        if let Err(e) = self.ch.casual_send(region_id, req) {
            self.send_fail_status(ctx, sink, Error::from(e), RpcStatusCode::ResourceExhausted);
            return;
        }

        let future = future
            .map_err(Error::from)
            .map(move |mut v| {
                let mut resp = BackupRegionResponse::new();
                if v.response.get_header().has_error() {
                    resp.mut_error()
                        .set_region_error(v.response.mut_header().take_error());
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                warn!("backup rpc failed";
                    "request" => "backup",
                    "err" => ?e
                );
                GRPC_MSG_FAIL_COUNTER.backup.inc();
            });

        ctx.spawn(future);
    }
}
