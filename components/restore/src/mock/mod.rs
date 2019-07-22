// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod pd;
mod trans;

pub use pd::PdCli;
pub use trans::Trans;

#[cfg(test)]
pub fn fixture_region() -> (kvproto::metapb::Region, u64) {
    use tikv::raftstore::store::util;
    let mut region = kvproto::metapb::Region::new();
    region.mut_region_epoch().set_version(1);
    region.mut_region_epoch().set_conf_ver(1);
    region.mut_peers().push(util::new_peer(1, 2));
    region.mut_peers().push(util::new_peer(2, 3));
    region.mut_peers().push(util::new_learner_peer(3, 4));
    (region, 3)
}
