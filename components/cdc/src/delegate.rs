use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::sync::mpsc::*;
use kvproto::cdcpb::*;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CmdType, Request};
use resolved_ts::Resolver;
use tikv::raftstore::coprocessor::{Cmd, CmdBatch};
use tikv::raftstore::Error as RaftStoreError;
use tikv::storage::mvcc::{Lock, LockType, Write, WriteType};
use tikv::storage::Key;
use tikv_util::collections::HashMap;

use crate::Error;

pub struct Downstream {
    pub id: usize,
    // The IP address of downstream.
    peer: String,
    sink: UnboundedSender<ChangeDataEvent>,
}

impl Downstream {
    pub fn new(id: usize, peer: String, sink: UnboundedSender<ChangeDataEvent>) -> Downstream {
        Downstream { id, peer, sink }
    }
}

pub struct Delegate {
    pub region_id: u64,
    pub downstreams: Vec<Downstream>,
    pub resolver: Option<Resolver>,
    initial_buffer: Option<Vec<CmdBatch>>,
    enabled: Arc<AtomicBool>,
}

impl Delegate {
    pub fn new(region_id: u64) -> Delegate {
        Delegate {
            region_id,
            downstreams: Vec::new(),
            resolver: None,
            initial_buffer: Some(Vec::new()),
            enabled: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn enabled(&self) -> Arc<AtomicBool> {
        self.enabled.clone()
    }

    pub fn subscribe(&mut self, downstream: Downstream) {
        self.downstreams.push(downstream);
    }

    pub fn unsubscribe(&mut self, id: usize) -> bool {
        self.downstreams.retain(|d| d.id != id);
        let is_last = self.downstreams.is_empty();
        if is_last {
            self.enabled.store(false, Ordering::Relaxed);
        }
        is_last
    }

    pub fn fail(&mut self, err: Error) {
        // Stop observe further events.
        self.enabled.store(false, Ordering::Relaxed);

        let mut change_data_event = Event::new();
        let mut cdc_err = EventError::default();
        let mut err = err.extract_error_header();
        if err.has_region_not_found() {
            let region_not_found = err.take_region_not_found();
            cdc_err.set_region_not_found(region_not_found);
        } else if err.has_not_leader() {
            let not_leader = err.take_not_leader();
            cdc_err.set_not_leader(not_leader);
        } else if err.has_epoch_not_match() {
            let epoch_not_match = err.take_epoch_not_match();
            cdc_err.set_epoch_not_match(epoch_not_match);
        } else {
            panic!(
                "region met unknown error region_id: {}, error: {:?}",
                self.region_id, err
            );
        }
        info!("region met error";
            "region_id" => self.region_id, "error" => ?cdc_err);
        change_data_event.event = Some(Event_oneof_event::Error(cdc_err));
        change_data_event.region_id = self.region_id;
        let mut change_data = ChangeDataEvent::new();
        change_data.mut_events().push(change_data_event);
        self.broadcast(change_data);
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
        if let Some(multi) = self.initial_buffer.take() {
            for batch in multi {
                self.on_batch(batch);
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

    pub fn on_batch(&mut self, batch: CmdBatch) {
        for cmd in batch.into_iter(self.region_id) {
            let Cmd {
                index,
                mut request,
                mut response,
            } = cmd;
            if !response.get_header().has_error() {
                if !request.has_admin_request() {
                    self.sink_data(index, request.requests.into());
                } else {
                    self.sink_admin(request.take_admin_request(), response.take_admin_response());
                }
            } else {
                let err_header = response.mut_header().take_error();
                let err = Error::Request(err_header);
                self.fail(err);
            }
        }
    }

    fn sink_data(&mut self, index: u64, requests: Vec<Request>) {
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

    fn sink_admin(&mut self, request: AdminRequest, mut response: AdminResponse) {
        let store_err = match request.get_cmd_type() {
            AdminCmdType::Split => RaftStoreError::EpochNotMatch(
                "split".to_owned(),
                vec![
                    response.mut_split().take_left(),
                    response.mut_split().take_right(),
                ],
            ),
            AdminCmdType::BatchSplit => RaftStoreError::EpochNotMatch(
                "batchsplit".to_owned(),
                response.mut_splits().take_regions().into(),
            ),
            AdminCmdType::PrepareMerge
            | AdminCmdType::CommitMerge
            | AdminCmdType::RollbackMerge => {
                RaftStoreError::EpochNotMatch("merge".to_owned(), vec![])
            }
            _ => return,
        };
        let err = Error::Request(store_err.into());
        self.fail(err);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::rocks::*;
    use futures::{Future, Stream};
    use kvproto::errorpb::Error as ErrorHeader;
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
        sender: UtilSender<Cmd>,
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
                        snap = Some(Snapshot::new(self.engine.clone()));
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
                    snapshot: Some(RegionSnapshot::from_snapshot(
                        snap.into_sync(),
                        self.region.clone(),
                    )),
                })
            } else {
                cb.invoke_with_response(response.clone());
                // Send write request only.
                self.sender
                    .send(Cmd {
                        index: 1,
                        request: req,
                        response,
                    })
                    .unwrap();
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
    fn test_txn() {
        let tmp = tempfile::TempDir::new().unwrap();
        let region_id = 1;
        let (sink, events) = unbounded();
        let mut delegate = Delegate::new(region_id);
        delegate.subscribe(Downstream::new(1, String::new(), sink));
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
            let cmd = cmds.try_recv().unwrap();
            let mut batch = CmdBatch::new(1);
            batch.push(1, cmd);
            delegate.on_batch(batch);
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

    #[test]
    fn test_error() {
        let region_id = 1;
        let (sink, events) = unbounded();
        let mut delegate = Delegate::new(region_id);
        delegate.subscribe(Downstream::new(1, String::new(), sink));
        let enabled = delegate.enabled();
        assert!(enabled.load(Ordering::Relaxed));
        let mut resolver = Resolver::new();
        resolver.init();
        delegate.on_region_ready(resolver);

        let events_wrap = Cell::new(Some(events));
        let receive_error = || {
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
            let event = change_data_event.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => err,
                _ => panic!("unknown event"),
            }
        };

        let mut err_header = ErrorHeader::default();
        err_header.set_not_leader(Default::default());
        delegate.fail(Error::Request(err_header));
        let err = receive_error();
        assert!(err.has_not_leader());
        // Enable is disabled by any error.
        assert!(!enabled.load(Ordering::Relaxed));

        let mut err_header = ErrorHeader::default();
        err_header.set_region_not_found(Default::default());
        delegate.fail(Error::Request(err_header));
        let err = receive_error();
        assert!(err.has_region_not_found());

        let mut err_header = ErrorHeader::default();
        err_header.set_epoch_not_match(Default::default());
        delegate.fail(Error::Request(err_header));
        let err = receive_error();
        assert!(err.has_epoch_not_match());

        // Split
        let mut region = Region::default();
        region.set_id(1);
        let mut request = AdminRequest::default();
        request.cmd_type = AdminCmdType::Split;
        let mut response = AdminResponse::default();
        response.mut_split().set_left(region.clone());
        delegate.sink_admin(request, response);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        err.take_epoch_not_match()
            .current_regions
            .into_iter()
            .find(|r| r.get_id() == 1)
            .unwrap();

        let mut request = AdminRequest::default();
        request.cmd_type = AdminCmdType::BatchSplit;
        let mut response = AdminResponse::default();
        response
            .mut_splits()
            .set_regions(vec![region.clone()].into());
        delegate.sink_admin(request, response);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        err.take_epoch_not_match()
            .current_regions
            .into_iter()
            .find(|r| r.get_id() == 1)
            .unwrap();

        // Merge
        let mut request = AdminRequest::default();
        request.cmd_type = AdminCmdType::PrepareMerge;
        let response = AdminResponse::default();
        delegate.sink_admin(request, response);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_regions.is_empty());

        let mut request = AdminRequest::default();
        request.cmd_type = AdminCmdType::CommitMerge;
        let response = AdminResponse::default();
        delegate.sink_admin(request, response);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_regions.is_empty());

        let mut request = AdminRequest::default();
        request.cmd_type = AdminCmdType::RollbackMerge;
        let response = AdminResponse::default();
        delegate.sink_admin(request, response);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_regions.is_empty());
    }
}
