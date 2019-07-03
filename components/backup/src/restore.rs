use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;

use kvproto::backup::{BackupEvent, BackupEvent_Event, BackupMeta};
use petgraph::Graph;
use protobuf::Message;

use crate::Result;
use crate::Storage;

pub struct RestoreManager {
    storage: Box<dyn Storage>,
}

#[derive(Debug)]
pub enum EvalNode {
    Event(BackupEvent),
    Logs {
        region_id: u64,
        start_index: u64,
        end_index: u64,
    },
}

pub type EvalGraph = Graph<EvalNode, &'static str>;

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

    pub fn total_order_eval(&self) -> Result<EvalGraph> {
        let mut meta = self.backup_meta().unwrap();
        meta.mut_events()
            .sort_by(|l, r| l.get_dependency().cmp(&r.get_dependency()));
        let cap = meta.get_events().len();
        let mut region_events = HashMap::new();
        let mut region_merges = HashMap::new();
        let mut g = Graph::with_capacity(cap, cap);
        let mut prev = None;
        for e in meta.take_events().into_vec() {
            let region_id = e.get_region_id();
            let index = e.get_index();
            let event = e.get_event();
            let related_region_ids = e.get_related_region_ids().to_vec();

            // Add e based on dependency.
            let idx = g.add_node(EvalNode::Event(e));
            if let Some(p) = prev {
                g.add_edge(p, idx, "");
            }
            prev = Some(idx);

            // Link Logs node.
            let logs = EvalNode::Logs {
                region_id,
                start_index: index + 1,
                end_index: 0,
            };
            let log_idx = g.add_node(logs);
            g.add_edge(idx, log_idx, "");

            if let Some(parent) = region_events.insert(region_id, log_idx) {
                assert_ne!(parent, idx);
                match g.node_weight_mut(parent).unwrap() {
                    EvalNode::Event(e) => {
                        // Commit merge may have deplicated events.
                        if e.get_index() == index {
                            assert_eq!(BackupEvent_Event::CommitMerge, e.get_event());
                            assert_eq!(BackupEvent_Event::CommitMerge, event);
                            continue;
                        }
                    }
                    // Update parent Logs node.
                    EvalNode::Logs {
                        region_id: id,
                        start_index,
                        ref mut end_index,
                    } => {
                        *end_index = index - 1;
                        assert_eq!(*id, region_id);
                        assert!(*start_index <= index - 1);
                    }
                }
            }
            match event {
                BackupEvent_Event::Split => {
                    for id in related_region_ids {
                        if id == region_id {
                            continue;
                        }
                        let logs = EvalNode::Logs {
                            region_id: id,
                            // Magic index, see more in peer_storage.rs
                            start_index: 6,
                            end_index: 0,
                        };
                        let sidx = g.add_node(logs);
                        g.add_edge(idx, sidx, "");
                    }
                }
                BackupEvent_Event::PrepareMerge => {
                    let target = related_region_ids[0];
                    if let Some((cidx, event, id)) = region_merges.remove(&target) {
                        assert_eq!(region_id, id);
                        assert_eq!(BackupEvent_Event::CommitMerge, event);
                        g.add_edge(idx, cidx, "");
                    } else {
                        region_merges.insert(region_id, (idx, event, target));
                    }
                }
                BackupEvent_Event::CommitMerge => {
                    let source = related_region_ids[0];
                    if let Some((pidx, event, id)) = region_merges.remove(&source) {
                        assert_eq!(region_id, id);
                        assert_eq!(BackupEvent_Event::PrepareMerge, event);
                        g.add_edge(pidx, idx, "");
                    } else {
                        region_merges.insert(region_id, (idx, event, source));
                    }
                }
                _ => (),
            }
        }
        Ok(g)
    }
}

pub fn dot<N: Debug, E: Debug>(g: &Graph<N, E>) -> String {
    format!("{:?}", petgraph::dot::Dot::new(g))
}
