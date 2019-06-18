// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use test_raftstore::*;
use tikv_util::HandyRwLock;

use super::configure_for_backup;

#[test]
fn test_server_split_backup_region() {
    let mut cluster = new_server_cluster(0, 2);
    configure_for_backup(&mut cluster);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    // Now region 1 only has peer (1, 1);
    let r1 = cluster.run_with_backup(&[2]);

    let backup_mgr2 = cluster.sim.rl().get_backup_mgr(2).unwrap();
    let list_region = |region_id: u64| {
        backup_mgr2
            .storage
            .list_dir(&backup_mgr2.current_dir().join(format!("{}", region_id)))
    };

    // Add learner (2, 2) to region 1.
    pd_client.must_add_peer(r1, new_learner_peer(2, 2));
    pd_client.must_none_pending_peer(new_learner_peer(2, 2));

    // Split cmds do flush log files.
    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k2");
    // Split is right derived by default.
    let region = cluster.get_region(b"k2");
    let region2 = cluster.get_region(b"");
    list_region(region.get_id()).unwrap_err();
    list_region(region2.get_id()).unwrap_err();

    // Start backup regon.
    backup_mgr2.start_backup_region(region.get_id()).unwrap();
    cluster.must_split(&region, b"k3");
    // Split is right derived by default.
    let region = cluster.get_region(b"k3");
    let region3 = cluster.get_region(b"k2");
    list_region(region.get_id()).unwrap();
    list_region(region3.get_id()).unwrap();
    list_region(region2.get_id()).unwrap_err();
}
