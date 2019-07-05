// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use kvproto::backup::BackupMeta;
use petgraph::prelude::NodeIndex;
use protobuf::Message;

use crate::Result;
use crate::Storage;

mod eval;

pub use eval::{dot, EvalGraph, EvalNode};

pub struct RestoreManager {
    storage: Box<dyn Storage>,
}

impl RestoreManager {
    pub fn new(storage: Box<dyn Storage>) -> Result<RestoreManager> {
        info!("create restore executor");
        Ok(RestoreManager { storage })
    }

    fn backup_meta(&self) -> Result<BackupMeta> {
        let mut buf = Vec::with_capacity(1024);
        self.storage
            .read_file(Path::new(crate::BACKUP_META_NAME), &mut buf)
            .unwrap();
        let mut meta = BackupMeta::new();
        meta.merge_from_bytes(&buf).unwrap();
        Ok(meta)
    }

    pub fn eval_graph(&self) -> Result<EvalGraph> {
        let meta = self.backup_meta().unwrap();
        let g = eval::build_eval_graph(meta);
        Ok(g)
    }

    pub fn total_order_eval(&self) -> Result<(EvalGraph, Vec<NodeIndex<u32>>)> {
        let g = self.eval_graph()?;
        eval::toposort(&g).map(|od| (g, od))
    }
}
