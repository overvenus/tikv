use std::collections::VecDeque;

use futures::sync::mpsc::*;
use kvproto::cdcpb::*;
use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, CmdType, RaftResponseHeader, Request};
use resolved_ts::Resolver;
use tikv::storage::mvcc::{Lock, LockType, Write, WriteType};
use tikv::storage::Key;
use tikv_util::collections::HashMap;
use tikv_util::Either;

pub struct Downstream {
    // The IP address of downstream.
    peer: String,
    sink: UnboundedSender<ChangeDataEvent>,
}

impl Downstream {
    pub fn new(peer: String, sink: UnboundedSender<ChangeDataEvent>) -> Downstream {
        Downstream { peer, sink }
    }
}

pub struct Delegate {
    pub region_id: u64,
    pub pending: VecDeque<(u64, Either<Vec<Request>, AdminRequest>)>,
    pub downstreams: Vec<Downstream>,
    pub resolver: Option<Resolver>,
    initial_buffer: Option<
        Vec<
            Either<
                (u64, Either<Vec<Request>, AdminRequest>),
                (
                    u64,
                    Either<RaftResponseHeader, (RaftResponseHeader, AdminResponse)>,
                ),
            >,
        >,
    >,
}

impl Delegate {
    pub fn new(region_id: u64) -> Delegate {
        Delegate {
            region_id,
            pending: VecDeque::new(),
            downstreams: Vec::new(),
            resolver: None,
            initial_buffer: Some(Vec::new()),
        }
    }

    pub fn subscribe(&mut self, downstream: Downstream) {
        self.downstreams.push(downstream);
    }

    pub fn unsubscribe(&mut self, peer: String) {
        self.downstreams.retain(|d| d.peer != peer)
    }

    fn broadcast(&self, change_data: ChangeDataEvent) {
        for d in &self.downstreams {
            if d.sink.unbounded_send(change_data.clone()).is_err() {
                info!("send event failed";
                        "downstream" => %d.peer,
                        "change_data" => ?change_data);
            }
        }
    }

    pub fn on_region_ready(&mut self, resolver: Resolver) {
        assert!(
            self.resolver.is_none(),
            "region resolver should not be ready"
        );
        self.resolver = Some(resolver);
        for buffer in self.initial_buffer.take() {
            for e in buffer {
                match e {
                    Either::Left((index, Either::Left(reqs))) => {
                        self.on_data_requsts(index, reqs);
                    }
                    Either::Left((index, Either::Right(req))) => {
                        self.on_admin_requst(index, req);
                    }
                    Either::Right((index, Either::Left(header))) => {
                        self.on_data_responses(index, header);
                    }
                    Either::Right((index, Either::Right((header, resp)))) => {
                        self.on_admin_response(index, header, resp);
                    }
                }
            }
        }
    }

    pub fn on_min_ts(&mut self, min_ts: u64) {
        if self.resolver.is_none() {
            info!("region resolver not ready";
                "region_id" => self.region_id, "min_ts" => min_ts);
            return;
        }
        info!("try to advance ts"; "region_id" => self.region_id);
        let resolver = self.resolver.as_mut().unwrap();
        let resolved_ts = match resolver.resolve(min_ts) {
            Some(rts) => rts,
            None => return,
        };
        info!("resolved ts updated";
            "region_id" => self.region_id, "resolved_ts" => resolved_ts);
        let mut change_data_event = Event::new();
        change_data_event.region_id = self.region_id;
        change_data_event.event = Some(Event_oneof_event::ResolvedTs(resolved_ts));
        let mut change_data = ChangeDataEvent::new();
        change_data.mut_events().push(change_data_event);
        self.broadcast(change_data);
    }

    pub fn on_data_requsts(&mut self, index: u64, reqs: Vec<Request>) {
        if let Some(buf) = self.initial_buffer.as_mut() {
            buf.push(Either::Left((index, Either::Left(reqs))));
            return;
        }
        self.pending.push_back((index, Either::Left(reqs)))
    }
    pub fn on_data_responses(&mut self, index: u64, header: RaftResponseHeader) {
        if let Some(buf) = self.initial_buffer.as_mut() {
            buf.push(Either::Right((index, Either::Left(header))));
            return;
        }
        while let Some((idx, req)) = self.pending.pop_front() {
            if idx < index {
                warn!("requests gap";
                    "region_id" => self.region_id,
                    "pervious_index" => idx,
                    "current_index" => index);
            // TODO: handle gap
            } else if idx == index {
                if !header.has_error() {
                    match req {
                        Either::Left(requests) => self.sink_data(index, requests),
                        Either::Right(_) => unreachable!(),
                    }
                } else {
                    self.sink_noop(index);
                }
                break;
            } else {
                self.pending.push_front((idx, req));
                break;
            }
        }
    }

    pub fn on_admin_requst(&mut self, index: u64, req: AdminRequest) {
        if let Some(buf) = self.initial_buffer.as_mut() {
            buf.push(Either::Left((index, Either::Right(req))));
            return;
        }
        self.pending.push_back((index, Either::Right(req)))
    }
    pub fn on_admin_response(
        &mut self,
        index: u64,
        header: RaftResponseHeader,
        resp: AdminResponse,
    ) {
        if let Some(buf) = self.initial_buffer.as_mut() {
            buf.push(Either::Right((index, Either::Right((header, resp)))));
            return;
        }
    }

    pub fn sink_noop(&self, index: u64) {
        self.broadcast(ChangeDataEvent::default());
    }
    pub fn sink_data(&mut self, index: u64, requests: Vec<Request>) {
        let mut kv: HashMap<Vec<u8>, EventRow> = HashMap::default();
        for mut req in requests {
            if req.cmd_type == CmdType::Put {
                let mut put = req.take_put();
                match put.cf.as_str() {
                    "write" => {
                        let write = Write::parse(put.get_value()).unwrap();
                        let (op_type, r_type) = match write.write_type {
                            WriteType::Put => (EventRowOpType::Put, EventLogType::Commit),
                            WriteType::Delete => (EventRowOpType::Delete, EventLogType::Commit),
                            WriteType::Rollback => {
                                (EventRowOpType::Unknown, EventLogType::Rollback)
                            }
                            other => {
                                debug!("skip write record";
                                    "write" => ?other);
                                continue;
                            }
                        };
                        let key = Key::from_encoded(put.take_key());
                        let commit_ts = key.decode_ts().unwrap();
                        let start_ts = write.start_ts;

                        let mut row = kv.entry(key.to_raw().unwrap()).or_default();
                        row.start_ts = start_ts;
                        row.commit_ts = commit_ts;
                        row.key = key.to_raw().unwrap();
                        row.op_type = op_type;
                        row.r_type = r_type;

                        // In order to advance resolved ts,
                        // we must untrack inflight txns if they are committed.
                        assert!(self.resolver.is_some(), "region resolver should be ready");
                        let resolver = self.resolver.as_mut().unwrap();
                        resolver.untrack_lock(start_ts, Some(commit_ts), key);
                    }
                    "lock" => {
                        let lock = Lock::parse(put.get_value()).unwrap();
                        let op_type = match lock.lock_type {
                            LockType::Put => EventRowOpType::Put,
                            LockType::Delete => EventRowOpType::Delete,
                            other => {
                                debug!("skip lock record";
                                    "lock" => ?other);
                                continue;
                            }
                        };
                        let key = Key::from_encoded(put.take_key());
                        let start_ts = lock.ts;

                        let mut row = kv.entry(key.to_raw().unwrap()).or_default();
                        row.start_ts = start_ts;
                        row.key = key.to_raw().unwrap();
                        row.op_type = op_type;
                        row.r_type = EventLogType::Prewrite;
                        if let Some(value) = lock.short_value {
                            row.value = value;
                        }

                        // In order to compute resolved ts,
                        // we must track inflight txns.
                        assert!(self.resolver.is_some(), "region resolver should be ready");
                        let resolver = self.resolver.as_mut().unwrap();
                        resolver.track_lock(start_ts, key);
                    }
                    "" | "default" => {
                        let key = Key::from_encoded(put.take_key());

                        let mut row = kv.entry(key.to_raw().unwrap()).or_default();
                        let value = put.get_value();
                        if !value.is_empty() {
                            row.value = value.to_vec();
                        }
                    }
                    other => {
                        panic!("invalid cf {}", other);
                    }
                }
            } else if req.cmd_type != CmdType::Delete {
                info!(
                    "skip other command";
                    "region_id" => self.region_id,
                    "command" => ?req,
                );
            }
        }
        let mut entires = Vec::with_capacity(kv.len());
        for (_, v) in kv {
            entires.push(v);
        }
        let mut event_entries = EventEntries::new();
        event_entries.entries = entires.into();
        let mut change_data_event = Event::new();
        change_data_event.region_id = self.region_id;
        change_data_event.index = index;
        change_data_event.event = Some(Event_oneof_event::Entries(event_entries));
        let mut change_data = ChangeDataEvent::new();
        change_data.mut_events().push(change_data_event);
        self.broadcast(change_data);
    }
    pub fn sink_admin(&self, index: u64, request: AdminRequest, response: AdminResponse) {
        self.broadcast(ChangeDataEvent::default());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::rocks::*;
    use futures::{Future, Stream};
    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, Response};
    use kvproto::raft_serverpb::RaftMessage;
    use std::cell::Cell;
    use std::sync::Arc;
    use tikv::raftstore::store::{
        keys, Callback, CasualMessage, ReadResponse, RegionSnapshot, SignificantMsg,
    };
    use tikv::raftstore::Result as RaftStoreResult;
    use tikv::server::transport::RaftStoreRouter;
    use tikv::server::RaftKv;
    use tikv::storage::mvcc::tests::*;
    use tikv_util::mpsc::{bounded, Sender as UtilSender};

    #[derive(Clone)]
    struct MockRouter {
        region: Region,
        engine: Arc<DB>,
        sender: UtilSender<RaftCmdRequest>,
    }
    impl RaftStoreRouter for MockRouter {
        fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
            Ok(())
        }
        fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
            let wb = WriteBatch::new();
            let mut snap = None;
            let mut responses = Vec::with_capacity(req.get_requests().len());
            for req in req.get_requests() {
                let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
                let key = keys::data_key(key);
                let cmd_type = req.get_cmd_type();
                match cmd_type {
                    CmdType::Put => {
                        if !req.get_put().get_cf().is_empty() {
                            let cf = req.get_put().get_cf();
                            let handle = util::get_cf_handle(&self.engine, cf).unwrap();
                            wb.put_cf(handle, &key, value).unwrap();
                        } else {
                            wb.put(&key, value).unwrap();
                        }
                    }
                    CmdType::Snap => {
                        snap = Some(self.engine.snapshot());
                    }
                    CmdType::Delete => {
                        if !req.get_put().get_cf().is_empty() {
                            let cf = req.get_put().get_cf();
                            let handle = util::get_cf_handle(&self.engine, cf).unwrap();
                            wb.delete_cf(handle, &key).unwrap();
                        } else {
                            wb.delete(&key).unwrap();
                        }
                    }
                    other => {
                        panic!("invalid cmd type {:?}", other);
                    }
                }
                let mut resp = Response::new();
                resp.set_cmd_type(cmd_type);

                responses.push(resp);
            }
            self.engine.write(&wb).unwrap();
            let mut response = RaftCmdResponse::new();
            response.set_responses(responses.into());
            if let Some(snap) = snap {
                cb.invoke_read(ReadResponse {
                    response,
                    snapshot: Some(RegionSnapshot::from_raw(
                        self.engine.clone(),
                        self.region.clone(),
                    )),
                })
            } else {
                cb.invoke_with_response(response);
                // Send write request only.
                self.sender.send(req).unwrap();
            }
            Ok(())
        }
        fn significant_send(&self, _: u64, _: SignificantMsg) -> RaftStoreResult<()> {
            Ok(())
        }
        fn broadcast_unreachable(&self, _: u64) {}
        fn casual_send(&self, _: u64, _: CasualMessage) -> RaftStoreResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_delegate() {
        let tmp = tempfile::TempDir::new().unwrap();
        let region_id = 1;
        let (sink, events) = unbounded();
        let mut delegate = Delegate::new(region_id);
        delegate.subscribe(Downstream::new(String::new(), sink));
        let mut resolver = Resolver::new();
        resolver.init();
        delegate.on_region_ready(resolver);

        let mut region = Region::new();
        region.set_id(region_id);
        region.mut_peers().push(Default::default());
        let engine = Arc::new(
            util::new_engine(tmp.path().to_str().unwrap(), None, engine::ALL_CFS, None).unwrap(),
        );
        let (sender, cmds) = bounded(10);
        let engine = RaftKv::new(MockRouter {
            region,
            engine,
            sender,
        });

        let events_wrap = Cell::new(Some(events));
        let mut check_event = |event_row: EventRow| {
            let mut cmd = cmds.try_recv().unwrap();
            delegate.on_data_requsts(1, cmd.take_requests().into());
            delegate.on_data_responses(1, RaftResponseHeader::new());
            let (change_data, events) = events_wrap
                .replace(None)
                .unwrap()
                .into_future()
                .wait()
                .unwrap();
            events_wrap.set(Some(events));
            let mut change_data = change_data.unwrap();
            assert_eq!(change_data.events.len(), 1);
            let change_data_event = &mut change_data.events[0];
            assert_eq!(change_data_event.region_id, region_id);
            assert_eq!(change_data_event.index, 1);
            let event = change_data_event.event.take().unwrap();
            match event {
                Event_oneof_event::Entries(entries) => {
                    assert_eq!(entries.entries.len(), 1);
                    let row = &entries.entries[0];
                    assert_eq!(*row, event_row);
                }
                _ => panic!("unknown event"),
            }
        };

        let mut ts = 0;
        let mut alloc_ts = || {
            ts += 1;
            ts
        };
        let (key, value) = (b"keya", b"valuea");
        let start_ts = alloc_ts();
        let commit_ts = alloc_ts();

        // Test prewrite.
        must_prewrite_put(&engine, key, value, key, start_ts);
        let mut row = EventRow::new();
        row.start_ts = start_ts;
        row.commit_ts = 0;
        row.key = key.to_vec();
        row.op_type = EventRowOpType::Put;
        row.r_type = EventLogType::Prewrite;
        row.value = value.to_vec();
        check_event(row);

        // Test commit.
        must_commit(&engine, key, start_ts, commit_ts);
        let mut row = EventRow::new();
        row.start_ts = start_ts;
        row.commit_ts = commit_ts;
        row.key = key.to_vec();
        row.op_type = EventRowOpType::Put;
        row.r_type = EventLogType::Commit;
        check_event(row);
    }
}
