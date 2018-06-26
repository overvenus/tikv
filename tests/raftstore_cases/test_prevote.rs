use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use super::cluster::{Cluster, Simulator};
use super::server::new_server_cluster;
use super::transport_simulate::*;
use super::util::*;
use raft::eraftpb::MessageType;

fn test_prevote<T: Simulator>(cluster: &mut Cluster<T>, prevote_enabled: bool) {
    cluster.cfg.raft_store.prevote = prevote_enabled;

    // Setup a notifier
    let (tx, rx) = mpsc::channel();
    let response_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));
    let request_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVote,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));

    // We must start the cluster before adding send filters, otherwise it panics.
    cluster.run();
    cluster
        .sim
        .write()
        .unwrap()
        .add_send_filter(2, response_notifier);
    cluster
        .sim
        .write()
        .unwrap()
        .add_send_filter(2, request_notifier);

    // Since the cluster is already started we might have missed some or all of the election.
    // In order to resolve this we need to essentially force an unplanned election.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.add_send_filter(IsolationFilterFactory::new(1));

    // Once we see a response on the wire we know an election will happen soon.
    let recieved = rx.recv_timeout(Duration::from_secs(2));
    assert_eq!(
        recieved.is_ok(),
        prevote_enabled,
        "Didn't recieve a PreVote or PreVoteResponse",
    );

    // Cleanup and make a new notifier.
    cluster.clear_send_filters();
    let (tx, rx) = mpsc::channel();
    let response_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));
    let request_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVote,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));

    // Make a node a leader, then kill it, letting it ask for a prevote.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.stop_node(1);
    cluster.run_node(1);

    // The remaining nodes should hold a new election.
    cluster
        .sim
        .write()
        .unwrap()
        .add_send_filter(3, response_notifier);
    cluster
        .sim
        .write()
        .unwrap()
        .add_send_filter(3, request_notifier);

    let recieved = rx.recv_timeout(Duration::from_secs(2));
    assert_eq!(recieved.is_ok(), prevote_enabled);
}

#[test]
fn test_server_prevote() {
    let mut cluster = new_server_cluster(0, 3);
    test_prevote(&mut cluster, true);
}

#[test]
fn test_server_no_prevote() {
    let mut cluster = new_server_cluster(0, 3);
    test_prevote(&mut cluster, false);
}

// Test isolating a minority of the cluster and make sure that the remove themselves.
fn test_pair_isolated<T: Simulator>(cluster: &mut Cluster<T>) {
    let region = 1;
    let pd_client = Arc::clone(&cluster.pd_client);

    // We must start the cluster before adding send filters, otherwise it panics.
    cluster.run();

    cluster.must_transfer_leader(region, new_peer(1, 1));

    cluster.must_put(b"k1", b"v1");

    // Split the nodes and ensure they will eventually remove themselves if isolated.
    cluster.partition(vec![1, 2, 3], vec![4, 5]);

    // Verify that pd has removed them.
    pd_client.must_remove_peer(region, new_peer(4, 4));
    pd_client.must_remove_peer(region, new_peer(5, 5));

    // Verify the nodes have self removed.
    cluster.must_remove_region(4, region);
    cluster.must_remove_region(5, region);
}

#[test]
fn test_server_pair_isolated() {
    let mut cluster = new_server_cluster(0, 5);
    test_pair_isolated(&mut cluster);
}
