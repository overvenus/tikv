// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use grpcio::{ChannelBuilder, Environment};

use futures::Future;
use kvproto::backup::*;
use kvproto::backup_grpc::*;
use kvproto::kvrpcpb::*;

use backup::BackupManager;
use test_raftstore::*;
use tikv_util::HandyRwLock;

use super::configure_for_backup;

fn must_new_backup_cluster_and_client() -> (
    Cluster<ServerCluster>,
    Arc<BackupManager>,
    BackupClient,
    u64,
) {
    let count = 2;
    let mut cluster = new_server_cluster(1, count);
    configure_for_backup(&mut cluster);

    // Disable default max peer count check.
    cluster.pd_client.disable_default_operator();
    let backup_store = 2;
    let region_id = cluster.run_with_backup(&[backup_store]);
    let bm = cluster.sim.rl().get_backup_mgr(2).unwrap();

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(cluster.sim.rl().get_addr(backup_store));
    let client = BackupClient::new(channel);

    (cluster, bm, client, region_id)
}

#[test]
fn test_service_backup_region() {
    let (cluster, bm, client, region_id) = must_new_backup_cluster_and_client();

    // Add learner (2, 2) to region 1.
    cluster
        .pd_client
        .must_add_peer(region_id, new_learner_peer(2, 2));
    cluster
        .pd_client
        .must_none_pending_peer(new_learner_peer(2, 2));

    let reigon_epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::new();
    ctx.set_region_id(region_id);
    ctx.set_region_epoch(reigon_epoch);

    let mut req = BackupRegionRequest::new();
    req.set_context(ctx);
    let resp = client.backup_region(&req).unwrap();
    assert!(!resp.get_error().has_region_error(), "{:?}", resp);
    bm.storage
        .list_dir(&bm.current_dir().join(format!("{}", region_id)))
        .unwrap();
    assert!(bm.is_region_started(region_id));

    req.mut_context().mut_region_epoch().set_version(1000);
    let resp = client.backup_region(&req).unwrap();
    assert!(resp.get_error().has_region_error(), "{:?}", resp);
}

#[test]
fn test_service_backup_region_abort() {
    let (mut cluster, bm, client, region_id) = must_new_backup_cluster_and_client();

    // Add learner (2, 2) to region 1.
    cluster
        .pd_client
        .must_add_peer(region_id, new_learner_peer(2, 2));
    cluster
        .pd_client
        .must_none_pending_peer(new_learner_peer(2, 2));

    let reigon_epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::new();
    ctx.set_region_id(region_id);
    ctx.set_region_epoch(reigon_epoch);

    // Isolate learner (2, 2) from rest of the cluster.
    cluster.add_send_filter(IsolationFilterFactory::new(2));

    let mut req = BackupRegionRequest::new();
    req.set_context(ctx);
    let resp = client.backup_region_async(&req).unwrap();

    // Split region 1
    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k1");

    // Remove store 2 filter.
    cluster.clear_send_filters();

    let resp = resp.wait().unwrap();
    assert!(resp.get_error().has_region_error(), "{:?}", resp);
    assert!(!bm.is_region_started(region_id));
}

#[test]
fn test_service_backup() {
    let (cluster, _, client, _) = must_new_backup_cluster_and_client();

    let mut req = BackupRequest::new();
    let resp = client.backup(&req).unwrap();
    assert!(resp.get_error().has_cluster_id_error(), "{:?}", resp);

    req.set_cluster_id(cluster.id());
    let resp = client.backup(&req).unwrap();
    assert!(!resp.get_error().has_cluster_id_error(), "{:?}", resp);
    assert!(resp.get_error().has_state_step_error(), "{:?}", resp);

    req.set_state(BackupState::Stop);
    let resp = client.backup(&req).unwrap();
    assert!(!resp.has_error(), "{:?}", resp);
}
