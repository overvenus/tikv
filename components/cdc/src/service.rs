use futures::sync::mpsc;
use futures::{Future, Sink, Stream};
use grpcio::*;
use kvproto::cdcpb::*;
use tikv_util::worker::*;

use crate::delegate::Downstream;
use crate::endpoint::Task;

#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Task>,
}

impl Service {
    pub fn new(scheduler: Scheduler<Task>) -> Service {
        Service { scheduler }
    }
}

impl ChangeData for Service {
    fn event_feed(
        &mut self,
        ctx: RpcContext,
        request: ChangeDataRequest,
        sink: ServerStreamingSink<ChangeDataEvent>,
    ) {
        let region_id = request.region_id;
        let peer = ctx.peer();
        // TODO: make it a bounded channel.
        let (tx, rx) = mpsc::unbounded();
        let downstream = Downstream::new(peer.clone(), tx);
        if let Err(status) = self
            .scheduler
            .schedule(Task::Register {
                request,
                downstream,
            })
            .map_err(|e| RpcStatus::new(RpcStatusCode::INVALID_ARGUMENT, Some(format!("{:?}", e))))
        {
            error!("cdc task initiate failed"; "error" => ?status);
            ctx.spawn(sink.fail(status).map_err(|e| {
                error!("cdc failed to send error"; "error" => ?e);
            }));
            return;
        }

        let send_resp = sink.send_all(rx.then(|resp| match resp {
            Ok(resp) => Ok((resp, WriteFlags::default())),
            Err(e) => {
                error!("cdc send failed"; "error" => ?e);
                Err(Error::RpcFailure(RpcStatus::new(
                    RpcStatusCode::UNKNOWN,
                    Some(format!("{:?}", e)),
                )))
            }
        }));
        let scheduler = self.scheduler.clone();
        ctx.spawn(send_resp.then(move |res| {
            scheduler
                .schedule(Task::Deregister {
                    region_id,
                    peer: Ok(peer),
                })
                .unwrap();
            match res {
                Ok(_s) => {
                    info!("cdc send half closed");
                }
                Err(e) => {
                    error!("cdc send failed"; "error" => ?e);
                }
            }
            Ok(())
        }));
    }
}
