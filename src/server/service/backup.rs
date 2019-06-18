// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::raftstore::store::{Callback, CasualMessage};
use crate::server::metrics::*;
use crate::server::transport::RaftStoreRouter;
use crate::server::Error as ServerError;
use backup::BackupManager;
use futures::Future;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use kvproto::backup::{BackupRegionRequest, BackupRegionResponse, BackupRequest, BackupResponse};
use kvproto::backup_grpc;
use tikv_util::future::paired_future_callback;

/// Service handles the RPC messages for the `Backup` service.
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
        err: ServerError,
        code: RpcStatusCode,
    ) {
        let status = RpcStatus::new(code, Some(format!("{}", err)));
        ctx.spawn(sink.fail(status).map_err(|_| ()));
    }
}

impl<T: RaftStoreRouter + 'static> backup_grpc::Backup for Service<T> {
    fn backup(&mut self, ctx: RpcContext<'_>, req: BackupRequest, sink: UnarySink<BackupResponse>) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.backup.start_coarse_timer();

        // TODO(backup): move it to another thread, since it may block grpc threads.
        let mut resp = BackupResponse::new();
        let cluster_id = req.get_cluster_id();
        let request = req.get_state();
        match self
            .backup_mgr
            .check_cluster_id(cluster_id)
            .and_then(|_| self.backup_mgr.step(request))
        {
            Err(e) => {
                warn!("backup rpc failed";
                    "request" => "backup",
                    "err" => ?e,
                    "request" => ?req,
                );
                resp.set_error(e.into());
            }
            Ok(dep) => {
                resp.set_current_dependency(dep);
            }
        }

        let future = sink
            .success(resp)
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                warn!("backup rpc failed";
                    "request" => "backup",
                    "err" => ?e,
                    "request" => ?req,
                );
                GRPC_MSG_FAIL_COUNTER.backup.inc();
            });

        ctx.spawn(future);
    }

    fn backup_region(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: BackupRegionRequest,
        sink: UnarySink<BackupRegionResponse>,
    ) {
        let timer = GRPC_MSG_HISTOGRAM_VEC.backup_region.start_coarse_timer();

        let region_id = req.get_context().get_region_id();
        if let Err(e) = self.backup_mgr.start_backup_region(region_id) {
            warn!("backup region rpc failed";
                "request" => "backup_region",
                "err" => ?e,
                "request" => ?req,
            );
            let mut resp = BackupRegionResponse::new();
            resp.set_error(e.into());
            ctx.spawn(sink.success(resp).then(|_| Ok(())));
            return;
        }

        let (cb, future) = paired_future_callback();
        let request = CasualMessage::RequestSnapshot {
            region_epoch: req.mut_context().take_region_epoch(),
            callback: Callback::Write(cb),
        };

        if let Err(e) = self.ch.casual_send(region_id, request) {
            warn!("backup region rpc failed";
                "request" => "backup_region",
                "err" => ?e,
                "request" => ?req,
            );
            self.send_fail_status(
                ctx,
                sink,
                ServerError::from(e),
                RpcStatusCode::ResourceExhausted,
            );
            return;
        }

        let future = future
            .map_err(ServerError::from)
            .map(move |mut v| {
                let mut resp = BackupRegionResponse::new();
                if v.response.get_header().has_error() {
                    resp.mut_error()
                        .set_region_error(v.response.mut_header().take_error());
                }
                resp
            })
            .and_then(|res| sink.success(res).map_err(ServerError::from))
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                warn!("backup region rpc failed";
                    "request" => "backup_region",
                    "err" => ?e,
                    "request" => ?req,
                );
                GRPC_MSG_FAIL_COUNTER.backup_region.inc();
            });

        ctx.spawn(future);
    }
}
