// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::Cell;
use std::sync::*;
use std::time::Duration;

use futures::{Future, Stream};
use grpcio::{ChannelBuilder, Environment};
use kvproto::cdcpb::*;
use kvproto::kvrpcpb::*;
use kvproto::tikvpb::TikvClient;
use pd_client::PdClient;
use test_raftstore::*;
use tikv::raftstore::coprocessor::CoprocessorHost;
use tikv_util::collections::HashMap;
use tikv_util::worker::Worker;
use tikv_util::HandyRwLock;

use cdc::{CdcObserver, Task};

struct TestSuite {
    cluster: Cluster<ServerCluster>,
    endpoints: HashMap<u64, Worker<Task>>,
    obs: HashMap<u64, CdcObserver>,
    tikv_cli: TikvClient,
    cdc_cli: ChangeDataClient,

    _env: Arc<Environment>,
}

impl TestSuite {
    fn new(count: usize) -> TestSuite {
        super::init();
        let mut cluster = new_server_cluster(1, count);

        let pd_cli = cluster.pd_client.clone();
        let mut endpoints = HashMap::default();
        let mut obs = HashMap::default();
        // Hack! node id are generated from 1..count+1.
        for id in 1..=count as u64 {
            // Create and run cdc endpoints.
            let worker = Worker::new(format!("cdc-{}", id));
            let mut sim = cluster.sim.wl();

            // Register cdc service to gRPC server.
            let scheduler = worker.scheduler();
            sim.pending_services
                .entry(id)
                .or_default()
                .push(Box::new(move || {
                    create_change_data(cdc::Service::new(scheduler.clone()))
                }));
            let scheduler = worker.scheduler();
            let cdc_ob = cdc::CdcObserver::new(scheduler.clone());
            obs.insert(id, cdc_ob.clone());
            sim.coprocessor_hooks.entry(id).or_default().push(Box::new(
                move |host: &mut CoprocessorHost| {
                    host.registry
                        .register_cmd_observer(100, Box::new(cdc_ob.clone()) as _);
                    host.registry
                        .register_role_observer(100, Box::new(cdc_ob.clone()) as _);
                },
            ));
            endpoints.insert(id, worker);
        }

        cluster.run();
        for (id, worker) in &mut endpoints {
            let sim = cluster.sim.rl();
            let apply_router = (*sim).get_apply_router(*id);
            let cdc_ob = obs.get(&id).unwrap().clone();
            let mut cdc_endpoint =
                cdc::Endpoint::new(pd_cli.clone(), worker.scheduler(), apply_router, cdc_ob);
            cdc_endpoint.set_min_ts_interval(Duration::from_millis(100));
            worker.start(cdc_endpoint).unwrap();
        }

        let region = cluster.get_region(&[]);
        let leader = cluster.leader_of_region(region.get_id()).unwrap();
        let leader_addr = cluster.sim.rl().get_addr(leader.get_store_id()).to_owned();
        let env = Arc::new(Environment::new(1));
        let channel = ChannelBuilder::new(env.clone()).connect(&leader_addr);
        let tikv_cli = TikvClient::new(channel.clone());
        let cdc_cli = ChangeDataClient::new(channel);

        TestSuite {
            cluster,
            endpoints,
            obs,
            tikv_cli,
            cdc_cli,
            _env: env,
        }
    }

    fn stop(mut self) {
        for (_, mut worker) in self.endpoints {
            worker.stop().unwrap();
        }
        self.cluster.shutdown();
    }

    fn must_kv_prewrite(&mut self, muts: Vec<Mutation>, pk: Vec<u8>, ts: u64) {
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(self.get_context(1));
        prewrite_req.set_mutations(muts.into_iter().collect());
        prewrite_req.primary_lock = pk;
        prewrite_req.start_version = ts;
        prewrite_req.lock_ttl = prewrite_req.start_version + 1;
        let prewrite_resp = self.tikv_cli.kv_prewrite(&prewrite_req).unwrap();
        assert!(
            !prewrite_resp.has_region_error(),
            "{:?}",
            prewrite_resp.get_region_error()
        );
        assert!(
            prewrite_resp.errors.is_empty(),
            "{:?}",
            prewrite_resp.get_errors()
        );
    }

    fn must_kv_commit(&mut self, keys: Vec<Vec<u8>>, start_ts: u64, commit_ts: u64) {
        let mut commit_req = CommitRequest::default();
        commit_req.set_context(self.get_context(1));
        commit_req.start_version = start_ts;
        commit_req.set_keys(keys.into_iter().collect());
        commit_req.commit_version = commit_ts;
        let commit_resp = self.tikv_cli.kv_commit(&commit_req).unwrap();
        assert!(
            !commit_resp.has_region_error(),
            "{:?}",
            commit_resp.get_region_error()
        );
        assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    }

    fn get_context(&mut self, region_id: u64) -> Context {
        let epoch = self.cluster.get_region_epoch(region_id);
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let mut context = Context::default();
        context.set_region_id(region_id);
        context.set_peer(leader);
        context.set_region_epoch(epoch);
        context
    }
}

#[test]
fn test_cdc_basic() {
    let mut suite = TestSuite::new(1);

    let (k, v) = ("key1".to_owned(), "value".to_owned());
    // Prewrite
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.op = Op::Put;
    mutation.key = k.clone().into_bytes();
    mutation.value = v.clone().into_bytes();
    suite.must_kv_prewrite(vec![mutation], k.clone().into_bytes(), start_ts);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let event_feed = suite.cdc_cli.event_feed(&req).unwrap();

    let event_feed_wrap = Cell::new(Some(event_feed));
    let receive_event = |keep_resolved_ts: bool| loop {
        let (change_data, events) =
            match event_feed_wrap.replace(None).unwrap().into_future().wait() {
                Ok(res) => res,
                Err(e) => panic!("receive failed {:?}", e.0),
            };
        event_feed_wrap.set(Some(events));
        let mut change_data = change_data.unwrap();
        assert_eq!(change_data.events.len(), 1);
        let change_data_event = &mut change_data.events[0];
        let event = change_data_event.event.take().unwrap();
        match event {
            Event_oneof_event::ResolvedTs(_) if !keep_resolved_ts => continue,
            other => return other,
        }
    };
    // Even if there is no write, resolved ts should be advanced regularly.
    let event = receive_event(true);
    match event {
        Event_oneof_event::ResolvedTs(ts) => assert_ne!(0, ts),
        _ => panic!("unknown event"),
    }

    // There must be a delegate.
    let scheduler = suite.endpoints.values().next().unwrap().scheduler();
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(|delegate| {
                let d = delegate.unwrap();
                assert_eq!(d.downstreams.len(), 1);
            }),
        ))
        .unwrap();

    // Commit
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(vec![k.clone().into_bytes()], start_ts, commit_ts);
    let event = receive_event(false);
    match event {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].r_type, EventLogType::Commit);
        }
        _ => panic!("unknown event"),
    }

    // Split region 1
    let region1 = suite.cluster.get_region(&[]);
    suite.cluster.must_split(&region1, b"key2");
    let event = receive_event(false);
    match event {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }

    // The second stream.
    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let event_feed2 = suite.cdc_cli.event_feed(&req).unwrap();
    let event_feed1 = event_feed_wrap.replace(Some(event_feed2));
    let event = receive_event(true);
    match event {
        Event_oneof_event::ResolvedTs(ts) => assert_ne!(0, ts),
        _ => panic!("unknown event"),
    }
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(|delegate| {
                let d = delegate.unwrap();
                assert_eq!(d.downstreams.len(), 2);
            }),
        ))
        .unwrap();

    // Drop event_feed2 and cancel its server streaming.
    event_feed_wrap.replace(None);
    // Sleep a while to make sure the stream is deregistered.
    sleep_ms(500);
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(|delegate| {
                let d = delegate.unwrap();
                assert_eq!(d.downstreams.len(), 1);
            }),
        ))
        .unwrap();

    // Drop all event_feed.
    drop(event_feed1);
    // Sleep a while to make sure the stream is deregistered.
    sleep_ms(500);
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(|delegate| {
                assert!(delegate.is_none());
            }),
        ))
        .unwrap();

    // Stale region epoch.
    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(Default::default()); // Zero region epoch.
    let event_feed3 = suite.cdc_cli.event_feed(&req).unwrap();
    event_feed_wrap.replace(Some(event_feed3));
    let event = receive_event(false);
    match event {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }

    suite.stop();
}

#[test]
fn test_cdc_not_leader() {
    let mut suite = TestSuite::new(3);

    // Make sure region 1 is ready.
    let (k, v) = ("key1".to_owned(), "value".to_owned());
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.op = Op::Put;
    mutation.key = k.clone().into_bytes();
    mutation.value = v.clone().into_bytes();
    suite.must_kv_prewrite(vec![mutation], k.clone().into_bytes(), start_ts);

    let leader = suite.cluster.leader_of_region(1).unwrap();
    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let event_feed = suite.cdc_cli.event_feed(&req).unwrap();

    let event_feed_wrap = Cell::new(Some(event_feed));
    let receive_event = |keep_resolved_ts: bool| loop {
        let (change_data, events) =
            match event_feed_wrap.replace(None).unwrap().into_future().wait() {
                Ok(res) => res,
                Err(e) => panic!("receive failed {:?}", e.0),
            };
        event_feed_wrap.set(Some(events));
        let mut change_data = change_data.unwrap();
        assert_eq!(change_data.events.len(), 1);
        let change_data_event = &mut change_data.events[0];
        let event = change_data_event.event.take().unwrap();
        match event {
            Event_oneof_event::ResolvedTs(_) if !keep_resolved_ts => continue,
            other => return other,
        }
    };

    // Make sure region 1 is registered.
    let event = receive_event(true);
    match event {
        Event_oneof_event::ResolvedTs(ts) => assert_ne!(0, ts),
        _ => panic!("unknown event"),
    }

    // There must be a delegate.
    let scheduler = suite
        .endpoints
        .get(&leader.get_store_id())
        .unwrap()
        .scheduler();
    let (tx, rx) = mpsc::channel();
    let tx_ = tx.clone();
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(move |delegate| {
                let d = delegate.unwrap();
                assert_eq!(d.downstreams.len(), 1);
                tx_.send(()).unwrap();
            }),
        ))
        .unwrap();
    rx.recv_timeout(Duration::from_millis(200)).unwrap();
    assert!(suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .is_subscribed(1));

    // Transfer leader.
    let peer = suite
        .cluster
        .get_region(&[])
        .take_peers()
        .into_iter()
        .find(|p| *p != leader)
        .unwrap();
    suite.cluster.must_transfer_leader(1, peer);
    let event = receive_event(false);
    match event {
        Event_oneof_event::Error(err) => {
            assert!(err.has_not_leader(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }
    assert!(!suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .is_subscribed(1));

    // Sleep a while to make sure the stream is deregistered.
    sleep_ms(200);
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(move |delegate| {
                assert!(delegate.is_none());
                tx.send(()).unwrap();
            }),
        ))
        .unwrap();
    rx.recv_timeout(Duration::from_millis(200)).unwrap();

    event_feed_wrap.replace(None);
    suite.stop();
}
