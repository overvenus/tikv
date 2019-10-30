use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::sync::mpsc::*;
use kvproto::cdcpb::*;
use kvproto::metapb::{Region, RegionEpoch};
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, CmdType, Request};
use resolved_ts::Resolver;
use tikv::raftstore::coprocessor::{Cmd, CmdBatch};
use tikv::raftstore::store::util::compare_region_epoch;
use tikv::raftstore::Error as RaftStoreError;
use tikv::storage::mvcc::{Lock, LockType, Write, WriteType};
use tikv::storage::txn::TxnEntry;
use tikv::storage::Key;
use tikv_util::collections::HashMap;

use crate::Error;

#[derive(Clone)]
pub struct Downstream {
    // TODO: include cdc request.
    // TODO: make ID a concrete type.
    pub id: usize,
    // The IP address of downstream.
    peer: String,
    region_epoch: RegionEpoch,
    sink: UnboundedSender<ChangeDataEvent>,
}

impl Downstream {
    pub fn new(
        id: usize,
        peer: String,
        region_epoch: RegionEpoch,
        sink: UnboundedSender<ChangeDataEvent>,
    ) -> Downstream {
        Downstream {
            id,
            peer,
            sink,
            region_epoch,
        }
    }

    fn sink(&self, change_data: ChangeDataEvent) {
        if self.sink.unbounded_send(change_data).is_err() {
            info!("send event failed"; "downstream" => %self.peer);
        }
    }
}

#[derive(Default)]
struct Pending {
    multi_batch: Vec<CmdBatch>,
    downstreams: Vec<Downstream>,
    scan: Vec<(usize, Vec<Option<TxnEntry>>)>,
}

pub struct Delegate {
    pub region_id: u64,
    region: Option<Region>,
    pub downstreams: Vec<Downstream>,
    pub resolver: Option<Resolver>,
    pending: Option<Pending>,
    enabled: Arc<AtomicBool>,
    failed: bool,
}

impl Delegate {
    pub fn new(region_id: u64) -> Delegate {
        Delegate {
            region_id,
            downstreams: Vec::new(),
            resolver: None,
            region: None,
            pending: Some(Pending::default()),
            enabled: Arc::new(AtomicBool::new(true)),
            failed: false,
        }
    }

    pub fn enabled(&self) -> Arc<AtomicBool> {
        self.enabled.clone()
    }

    pub fn subscribe(&mut self, downstream: Downstream) {
        if let Some(region) = self.region.as_ref() {
            if let Err(e) = compare_region_epoch(
                &downstream.region_epoch,
                region,
                false, /* check_conf_ver */
                true,  /* check_ver */
                true,  /* include_region */
            ) {
                let err = Error::Request(e.into());
                let change_data_error = self.error_event(err);
                downstream.sink(change_data_error);
                return;
            }
            self.downstreams.push(downstream);
        } else {
            self.pending.as_mut().unwrap().downstreams.push(downstream);
        }
    }

    pub fn unsubscribe(&mut self, id: usize, err: Option<Error>) -> bool {
        let change_data_error = err.map(|err| self.error_event(err));
        let downstreams = if self.pending.is_some() {
            &mut self.pending.as_mut().unwrap().downstreams
        } else {
            &mut self.downstreams
        };
        downstreams.retain(|d| {
            if d.id == id {
                if let Some(change_data_error) = change_data_error.clone() {
                    d.sink(change_data_error);
                }
            }
            d.id != id
        });
        let is_last = self.downstreams.is_empty();
        if is_last {
            self.enabled.store(false, Ordering::Relaxed);
        }
        is_last
    }

    fn error_event(&self, err: Error) -> ChangeDataEvent {
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
        change_data_event.event = Some(Event_oneof_event::Error(cdc_err));
        change_data_event.region_id = self.region_id;
        let mut change_data = ChangeDataEvent::new();
        change_data.mut_events().push(change_data_event);
        change_data
    }

    pub fn fail(&mut self, err: Error) {
        // Stop observe further events.
        self.enabled.store(false, Ordering::Relaxed);

        info!("region met error";
            "region_id" => self.region_id, "error" => ?err);
        let change_data = self.error_event(err);
        self.broadcast(change_data);

        // Mark this delegate has failed.
        self.failed = true;
    }

    pub fn has_failed(&self) -> bool {
        self.failed
    }

    fn broadcast(&self, change_data: ChangeDataEvent) {
        let downstreams = if self.pending.is_some() {
            &self.pending.as_ref().unwrap().downstreams
        } else {
            &self.downstreams
        };
        for d in downstreams {
            d.sink(change_data.clone());
        }
    }

    pub fn on_region_ready(&mut self, resolver: Resolver, region: Region) {
        assert!(
            self.resolver.is_none(),
            "region resolver should not be ready"
        );
        self.resolver = Some(resolver);
        self.region = Some(region);
        if let Some(pending) = self.pending.take() {
            // Re-subscribe pending downstreams.
            for downstream in pending.downstreams {
                self.subscribe(downstream);
            }
            for (downstream_id, entires) in pending.scan {
                self.on_scan(downstream_id, entires);
            }
            for batch in pending.multi_batch {
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
        if let Some(pending) = self.pending.as_mut() {
            pending.multi_batch.push(batch);
            return;
        }
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

    pub fn on_scan(&mut self, downstream_id: usize, entries: Vec<Option<TxnEntry>>) {
        if let Some(pending) = self.pending.as_mut() {
            pending.scan.push((downstream_id, entries));
            return;
        }
        let d = if let Some(d) = self.downstreams.iter_mut().find(|d| d.id == downstream_id) {
            d
        } else {
            warn!("downstream not found"; "downstream_id" => downstream_id);
            return;
        };

        let mut rows = Vec::with_capacity(entries.len());
        for entry in entries {
            match entry {
                Some(TxnEntry::Prewrite { default, lock }) => {
                    let mut row = EventRow::default();
                    let skip = decode_lock(lock.0, &lock.1, &mut row);
                    if skip {
                        continue;
                    }
                    decode_default(default.1, &mut row);
                    rows.push(row);
                }
                Some(TxnEntry::Commit { default, write }) => {
                    let mut row = EventRow::default();
                    let skip = decode_write(write.0, &write.1, &mut row);
                    if skip {
                        continue;
                    }
                    decode_default(default.1, &mut row);

                    // This type means the row is self-contained, it has,
                    //   1. start_ts
                    //   2. commit_ts
                    //   3. key
                    //   4. value
                    row.r_type = EventLogType::Committed;
                    rows.push(row);
                }
                None => {
                    let mut row = EventRow::default();

                    // This type means scan has finised.
                    row.r_type = EventLogType::Initialized;
                    rows.push(row);
                }
            }
        }

        let mut event_entries = EventEntries::new();
        event_entries.entries = rows.into();
        let mut change_data_event = Event::new();
        change_data_event.region_id = self.region_id;
        change_data_event.event = Some(Event_oneof_event::Entries(event_entries));
        let mut change_data = ChangeDataEvent::new();
        change_data.mut_events().push(change_data_event);
        d.sink(change_data);
    }

    fn sink_data(&mut self, index: u64, requests: Vec<Request>) {
        let mut rows = HashMap::default();
        for mut req in requests {
            if req.cmd_type == CmdType::Put {
                let mut put = req.take_put();
                match put.cf.as_str() {
                    "write" => {
                        let mut row = EventRow::default();
                        let skip = decode_write(put.take_key(), put.get_value(), &mut row);
                        if skip {
                            continue;
                        }

                        // In order to advance resolved ts,
                        // we must untrack inflight txns if they are committed.
                        assert!(self.resolver.is_some(), "region resolver should be ready");
                        let resolver = self.resolver.as_mut().unwrap();
                        resolver.untrack_lock(row.start_ts, Some(row.commit_ts), row.key.clone());

                        let r = rows.insert(row.key.clone(), row);
                        assert!(r.is_none());
                    }
                    "lock" => {
                        let mut row = EventRow::default();
                        let skip = decode_lock(put.take_key(), put.get_value(), &mut row);
                        if skip {
                            continue;
                        }

                        let occupied = rows.entry(row.key.clone()).or_default();
                        if !occupied.value.is_empty() {
                            assert!(row.value.is_empty());
                            let mut value = vec![];
                            mem::swap(&mut occupied.value, &mut value);
                            row.value = value;
                        }

                        // In order to compute resolved ts,
                        // we must track inflight txns.
                        assert!(self.resolver.is_some(), "region resolver should be ready");
                        let resolver = self.resolver.as_mut().unwrap();
                        resolver.track_lock(row.start_ts, row.key.clone());

                        *occupied = row;
                    }
                    "" | "default" => {
                        let key = Key::from_encoded(put.take_key()).truncate_ts().unwrap();
                        let row = rows.entry(key.to_raw().unwrap()).or_default();
                        decode_default(put.take_value(), row);
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
        let mut entires = Vec::with_capacity(rows.len());
        for (_, v) in rows {
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

fn decode_write(key: Vec<u8>, value: &[u8], row: &mut EventRow) -> bool {
    let write = Write::parse(value).unwrap();
    let (op_type, r_type) = match write.write_type {
        WriteType::Put => (EventRowOpType::Put, EventLogType::Commit),
        WriteType::Delete => (EventRowOpType::Delete, EventLogType::Commit),
        WriteType::Rollback => (EventRowOpType::Unknown, EventLogType::Rollback),
        other => {
            debug!("skip write record"; "write" => ?other);
            return true;
        }
    };
    let key = Key::from_encoded(key);
    let commit_ts = key.decode_ts().unwrap();
    row.start_ts = write.start_ts;
    row.commit_ts = commit_ts;
    row.key = key.truncate_ts().unwrap().to_raw().unwrap();
    row.op_type = op_type;
    row.r_type = r_type;
    if let Some(value) = write.short_value {
        row.value = value;
    }

    false
}

fn decode_lock(key: Vec<u8>, value: &[u8], row: &mut EventRow) -> bool {
    let lock = Lock::parse(value).unwrap();
    let op_type = match lock.lock_type {
        LockType::Put => EventRowOpType::Put,
        LockType::Delete => EventRowOpType::Delete,
        other => {
            info!("skip lock record";
                "type" => ?other,
                "start_ts" => ?lock.ts,
                "for_update_ts" => ?lock.for_update_ts);
            return true;
        }
    };
    let key = Key::from_encoded(key);
    row.start_ts = lock.ts;
    row.key = key.to_raw().unwrap();
    row.op_type = op_type;
    row.r_type = EventLogType::Prewrite;
    if let Some(value) = lock.short_value {
        row.value = value;
    }

    false
}

fn decode_default(value: Vec<u8>, row: &mut EventRow) {
    if !value.is_empty() {
        row.value = value.to_vec();
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
    use tikv::storage::mvcc::reader::tests::*;
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
        let mut region = Region::default();
        region.set_id(region_id);
        region.mut_peers().push(Default::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(2);
        let region_epoch = region.get_region_epoch().clone();

        let (sink, events) = unbounded();
        let mut delegate = Delegate::new(region_id);
        delegate.subscribe(Downstream::new(
            1,
            String::new(),
            region_epoch.clone(),
            sink,
        ));
        let mut resolver = Resolver::new();
        resolver.init();
        delegate.on_region_ready(resolver, region.clone());

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
        row.value = value.to_vec(); // short value.
        check_event(row);
    }

    #[test]
    fn test_error() {
        let region_id = 1;
        let mut region = Region::default();
        region.set_id(region_id);
        region.mut_peers().push(Default::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(2);
        let region_epoch = region.get_region_epoch().clone();

        let (sink, events) = unbounded();
        let mut delegate = Delegate::new(region_id);
        delegate.subscribe(Downstream::new(
            1,
            String::new(),
            region_epoch.clone(),
            sink,
        ));
        let enabled = delegate.enabled();
        assert!(enabled.load(Ordering::Relaxed));
        let mut resolver = Resolver::new();
        resolver.init();
        delegate.on_region_ready(resolver, region);

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

    #[test]
    fn test_scan() {
        let region_id = 1;
        let mut region = Region::default();
        region.set_id(region_id);
        region.mut_peers().push(Default::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(2);
        let region_epoch = region.get_region_epoch().clone();

        let (sink, events) = unbounded();
        let mut delegate = Delegate::new(region_id);
        delegate.subscribe(Downstream::new(
            1,
            String::new(),
            region_epoch.clone(),
            sink,
        ));
        let enabled = delegate.enabled();
        assert!(enabled.load(Ordering::Relaxed));

        let events_wrap = Cell::new(Some(events));
        let check_event = |event_rows: Vec<EventRow>| {
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
            assert_eq!(change_data_event.index, 0);
            let event = change_data_event.event.take().unwrap();
            match event {
                Event_oneof_event::Entries(entries) => {
                    assert_eq!(entries.entries, event_rows.into());
                }
                _ => panic!("unknown event"),
            }
        };
        let into_entry = |e: Option<EntryBuilder>| {
            e.map(|e| {
                if e.commit_ts == 0 {
                    e.build_prewrite(LockType::Put, false)
                } else {
                    e.build_commit(WriteType::Put, false)
                }
            })
        };

        // Stashed in pending before region ready.
        let entries = vec![
            Some(EntryBuilder {
                key: b"a".to_vec(),
                value: b"b".to_vec(),
                start_ts: 1,
                commit_ts: 0,
            }),
            Some(EntryBuilder {
                key: b"a".to_vec(),
                value: b"b".to_vec(),
                start_ts: 1,
                commit_ts: 2,
            }),
            None,
        ];
        delegate.on_scan(1, entries.into_iter().map(into_entry).collect());
        assert_eq!(delegate.pending.as_ref().unwrap().scan.len(), 1);

        let mut resolver = Resolver::new();
        resolver.init();
        delegate.on_region_ready(resolver, region);

        // Flush all pending entries.
        let mut row1 = EventRow::new();
        row1.start_ts = 1;
        row1.commit_ts = 0;
        row1.key = b"a".to_vec();
        row1.op_type = EventRowOpType::Put;
        row1.r_type = EventLogType::Prewrite;
        row1.value = b"b".to_vec();
        let mut row2 = EventRow::new();
        row2.start_ts = 1;
        row2.commit_ts = 2;
        row2.key = b"a".to_vec();
        row2.op_type = EventRowOpType::Put;
        row2.r_type = EventLogType::Committed;
        row2.value = b"b".to_vec();
        let mut row3 = EventRow::new();
        row3.r_type = EventLogType::Initialized;
        check_event(vec![row1, row2, row3]);
    }
}
