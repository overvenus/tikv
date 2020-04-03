// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use crate::{new_event_feed, TestSuite};
use futures::sink::Sink;
use futures::Future;
use grpcio::WriteFlags;
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::*;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::{
    event::{Event as Event_oneof_event, LogType as EventLogType},
    ChangeDataRequest,
};
use kvproto::kvrpcpb::*;
use pd_client::PdClient;
use test_raftstore::sleep_ms;

#[test]
fn test_stale_observe_cmd() {
    let mut suite = TestSuite::new(3);

    let region = suite.cluster.get_region(&[]);
    let mut req = ChangeDataRequest::default();
    req.region_id = region.get_id();
    req.set_region_epoch(region.get_region_epoch().clone());
    let (req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(region.get_id()));
    let _req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        _ => panic!("unknown event"),
    }

    let (k, v) = ("key1".to_owned(), "value".to_owned());
    // Prewrite
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone().into_bytes();
    mutation.value = v.into_bytes();
    suite.must_kv_prewrite(
        region.get_id(),
        vec![mutation],
        k.clone().into_bytes(),
        start_ts,
    );
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Prewrite);
        }
        _ => panic!("unknown event"),
    }
    let fp = "before_cdc_flush_apply";
    fail::cfg(fp, "pause").unwrap();

    // Async commit
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let commit_resp =
        suite.async_kv_commit(region.get_id(), vec![k.into_bytes()], start_ts, commit_ts);
    sleep_ms(200);
    // Close previous connection and open a new one twice time
    let (req_tx, resp_rx) = suite
        .get_region_cdc_client(region.get_id())
        .event_feed()
        .unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let _req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    let (req_tx, resp_rx) = suite
        .get_region_cdc_client(region.get_id())
        .event_feed()
        .unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    fail::remove(fp);
    // Receive Commit response
    commit_resp.wait().unwrap();
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", es);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        _ => panic!("unknown event"),
    }

    // Make sure resolved ts can be advanced normally even with few tso failures.
    let mut counter = 0;
    loop {
        // Even if there is no write,
        // resolved ts should be advanced regularly.
        for e in receive_event(true) {
            match e.event.unwrap() {
                Event_oneof_event::ResolvedTs(ts) => {
                    assert_ne!(0, ts);
                    counter += 1;
                }
                _ => panic!("unknown event"),
            }
        }
        if counter > 5 {
            break;
        }
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_merge() {
    let mut suite = TestSuite::new(1);
    // Split region
    let region = suite.cluster.get_region(&[]);
    suite.cluster.must_split(&region, b"k1");
    // Subscribe source region
    let source = suite.cluster.get_region(b"k0");
    let mut req = ChangeDataRequest::default();
    req.region_id = source.get_id();
    req.set_region_epoch(source.get_region_epoch().clone());
    let (source_tx, source_wrap, source_event) =
        new_event_feed(suite.get_region_cdc_client(source.get_id()));
    let source_tx = source_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Subscribe target region
    let target = suite.cluster.get_region(b"k2");
    req.region_id = target.get_id();
    req.set_region_epoch(target.get_region_epoch().clone());
    let (target_tx, target_wrap, target_event) =
        new_event_feed(suite.get_region_cdc_client(target.get_id()));
    let _target_tx = target_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    sleep_ms(200);
    // Pause before completing commit merge
    let fp = "before_handle_catch_up_logs_for_merge";
    fail::cfg(fp, "pause").unwrap();
    // The call is finished when prepare_merge is applied.
    suite.cluster.try_merge(source.get_id(), target.get_id());
    // Epoch not match after prepare_merge
    let mut events = source_event(false);
    if events.len() == 1 {
        events.extend(source_event(false).into_iter());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }
    let mut events = target_event(false);
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }
    sleep_ms(200);
    // Retry to subscribe source region
    let source = suite.cluster.get_region(b"k0");
    req.region_id = source.get_id();
    req.set_region_epoch(source.get_region_epoch().clone());
    let _source_tx = source_tx.send((req, WriteFlags::default())).wait().unwrap();
    // Continue to commit merge
    fail::remove(fp);
    let mut events = source_event(false);
    if events.len() == 1 {
        events.extend(source_event(false).into_iter());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_region_not_found(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }
    let mut events = target_event(false);
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }

    source_wrap.as_ref().replace(None);
    target_wrap.as_ref().replace(None);
    suite.stop();
}
