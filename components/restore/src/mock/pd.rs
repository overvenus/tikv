// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::{metapb, pdpb};
use tikv::pd::{PdClient, PdFuture, RegionStat, Result as PdResult};

use futures::future;

pub struct PdCli {
    cluster_id: u64,
}

impl PdCli {
    pub fn new(cluster_id: u64) -> PdCli {
        PdCli { cluster_id }
    }
}

impl PdClient for PdCli {
    fn get_cluster_id(&self) -> PdResult<u64> {
        info!("get_cluster_id"; "cluster_id" => self.cluster_id);
        Ok(self.cluster_id)
    }
    fn bootstrap_cluster(&self, _: metapb::Store, _: metapb::Region) -> PdResult<()> {
        info!("bootstrap_cluster");
        Ok(())
    }
    fn is_cluster_bootstrapped(&self) -> PdResult<bool> {
        info!("is_cluster_bootstrapped");
        Ok(true)
    }
    fn alloc_id(&self) -> PdResult<u64> {
        info!("alloc_id");
        Ok(0)
    }
    fn put_store(&self, store: metapb::Store) -> PdResult<()> {
        info!("put_store"; "store" => ?store);
        Ok(())
    }
    fn get_store(&self, store_id: u64) -> PdResult<metapb::Store> {
        info!("get_store"; "store_id" => store_id);
        Ok(Default::default())
    }
    fn get_cluster_config(&self) -> PdResult<metapb::Cluster> {
        info!("get_cluster_config");
        Ok(Default::default())
    }
    fn get_region(&self, _: &[u8]) -> PdResult<metapb::Region> {
        info!("get_region");
        Ok(Default::default())
    }
    fn get_region_by_id(&self, _: u64) -> PdFuture<Option<metapb::Region>> {
        info!("get_region_by_id");
        Box::new(future::result(Ok(Default::default())))
    }
    fn region_heartbeat(&self, _: metapb::Region, _: metapb::Peer, _: RegionStat) -> PdFuture<()> {
        info!("region_heartbeat");
        Box::new(future::result(Ok(())))
    }
    fn handle_region_heartbeat_response<F>(&self, _: u64, _: F) -> PdFuture<()>
    where
        F: Fn(pdpb::RegionHeartbeatResponse) + Send + 'static,
    {
        info!("handle_region_heartbeat_response");
        Box::new(future::result(Ok(())))
    }
    fn ask_split(&self, _: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        Box::new(future::result(Ok(Default::default())))
    }
    fn ask_batch_split(
        &self,
        _: metapb::Region,
        _: usize,
    ) -> PdFuture<pdpb::AskBatchSplitResponse> {
        info!("ask_batch_split");
        Box::new(future::result(Ok(Default::default())))
    }
    fn store_heartbeat(&self, _: pdpb::StoreStats) -> PdFuture<()> {
        info!("store_heartbeat");
        Box::new(future::result(Ok(Default::default())))
    }
    fn report_batch_split(&self, _: Vec<metapb::Region>) -> PdFuture<()> {
        info!("report_batch_split");
        Box::new(future::result(Ok(Default::default())))
    }
    fn get_gc_safe_point(&self) -> PdFuture<u64> {
        info!("get_gc_safe_point");
        Box::new(future::result(Ok(0)))
    }
    fn get_store_stats(&self, _: u64) -> PdResult<pdpb::StoreStats> {
        info!("get_store_stats");
        Ok(Default::default())
    }
    fn get_operator(&self, _: u64) -> PdResult<pdpb::GetOperatorResponse> {
        info!("get_operator");
        Ok(Default::default())
    }
}
