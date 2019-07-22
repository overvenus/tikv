// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::*;
use std::thread;

use kvproto::metapb;
use tikv::config::TiKvConfig;
use tikv::import::{ImportSSTService, SSTImporter};
use tikv::pd::{Error as PdError, PdClient, PdTask, INVALID_ID};
use tikv::raftstore::coprocessor::CoprocessorHost;
use tikv::raftstore::store::fsm::store::StoreMeta;
use tikv::raftstore::store::fsm::*;
use tikv::raftstore::store::*;
use tikv::raftstore::Result;
use tikv_util::worker::FutureWorker;

use crate::mock::*;
use crate::observer;

/// A wrapper for the raftstore which serves restore tasks.
pub struct RestoreSystem {
    cluster_id: u64,
    store_id: u64,
    cfg: Config,
    router: RaftRouter,
    system: RaftBatchSystem,
    engines: engine::Engines,
    snap_mgr: SnapManager,
}

impl RestoreSystem {
    pub fn new(
        cluster_id: u64,
        store_id: u64,
        cfg: Config,
        engines: engine::Engines,
        router: RaftRouter,
        system: RaftBatchSystem,
        snap_mgr: SnapManager,
    ) -> Self {
        RestoreSystem {
            cluster_id,
            store_id,
            cfg,
            router,
            system,
            engines,
            snap_mgr,
        }
    }

    pub fn bootstrap(&mut self) -> Result<()> {
        info!("bootstrap restore system";
            "cluster_id" => self.cluster_id,
            "store_id" => self.store_id);
        bootstrap_store(&self.engines, self.cluster_id, self.store_id)
    }

    pub fn start(&mut self) -> Result<mpsc::Receiver<(u64, metapb::Region)>> {
        info!("start restore system";
            "cluster_id" => self.cluster_id,
            "store_id" => self.store_id);
        let mut meta = metapb::Store::new();
        meta.set_id(self.store_id);
        let trans = Trans::new();
        let pd_client = Arc::new(PdCli::new(self.cluster_id));
        let pd_worker = FutureWorker::new("mock-pd-worker");
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(20)));
        let mut coprocessor_host = CoprocessorHost::new(Default::default(), self.router.clone());
        let (restore_observer, rx) = observer::RestoreObserver::new();
        coprocessor_host
            .registry
            .register_admin_observer(500, Box::new(restore_observer.clone()));
        coprocessor_host
            .registry
            .register_query_observer(500, Box::new(restore_observer));
        let importer = {
            let dir = Path::new(self.engines.kv.path()).join("import-sst");
            Arc::new(SSTImporter::new(dir).unwrap())
        };
        self.system
            .spawn(
                meta,
                self.cfg.clone(),
                self.engines.clone(),
                trans,
                pd_client,
                self.snap_mgr.clone(),
                pd_worker,
                store_meta,
                coprocessor_host,
                importer,
                None,
            )
            .map(|_| rx)
    }

    pub fn stop(&mut self) -> Result<()> {
        info!("stop restore system");
        self.system.shutdown();
        Ok(())
    }
}
