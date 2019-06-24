// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::raftstore::store::{Callback, CasualMessage, WriteResponse};
use crate::server::metrics::*;
use crate::server::transport::RaftStoreRouter;
use backup::BackupManager;
use futures::sync::mpsc;
use futures::{Future, Stream};
use grpcio::{RpcContext, UnarySink};
use kvproto::backup::{BackupRegionRequest, BackupRegionResponse, BackupRequest, BackupResponse};
use kvproto::backup_grpc;

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
        let backup_mgr = self.backup_mgr.clone();
        let (tx, future) = mpsc::unbounded::<BackupRegionResponse>();
        let tx_ = tx.clone();
        let start_cb = Box::new(move |mut result: WriteResponse| {
            if result.response.get_header().has_error() {
                let mut resp = BackupRegionResponse::new();
                resp.mut_error()
                    .set_region_error(result.response.mut_header().take_error());
                warn!("start backup region failed";
                    "request" => "backup_region",
                );
                tx_.unbounded_send(resp).unwrap();
            } else {
                backup_mgr.start_backup_region(region_id).unwrap();
            }
        });

        let end_cb = Box::new(move |mut result: WriteResponse| {
            let mut resp = BackupRegionResponse::new();
            if result.response.get_header().has_error() {
                resp.mut_error()
                    .set_region_error(result.response.mut_header().take_error());
            }
            tx.unbounded_send(resp).unwrap();
        });

        let request = CasualMessage::RequestSnapshot {
            region_epoch: req.mut_context().take_region_epoch(),
            start_cb: Callback::Write(start_cb),
            end_cb: Callback::Write(end_cb),
        };

        if let Err(e) = self.ch.casual_send(region_id, request) {
            warn!("backup region rpc failed";
                "request" => "backup_region",
                "err" => ?e,
                "request" => ?req,
            );
            let mut resp = BackupRegionResponse::new();
            resp.mut_error().set_region_error(e.into());
            ctx.spawn(sink.success(resp).then(|_| Ok(())));
            return;
        }

        let future = future
            .into_future()
            .map(|(resp, _)| resp.unwrap())
            .map_err(|(e, _)| e)
            .and_then(|res| {
                sink.success(res).map_err(move |e| {
                    warn!("backup region rpc failed";
                        "request" => "backup_region",
                        "err" => ?e,
                    );
                })
            })
            .map(|_| timer.observe_duration())
            .map_err(move |e| {
                warn!("backup region rpc failed";
                    "request" => "backup_region",
                    "request" => ?req,
                    "err" => ?e,
                );
                GRPC_MSG_FAIL_COUNTER.backup_region.inc();
            });

        ctx.spawn(future);
    }
}
