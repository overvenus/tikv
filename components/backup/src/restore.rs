// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

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

pub type EvalGraph = Graph<EvalNode, ()>;

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
        let meta = self.backup_meta().unwrap();
        Ok(total_order_eval(meta))
    }
}

pub fn dot<N: Debug, E: Debug>(g: &Graph<N, E>) -> String {
    format!(
        "{:?}",
        petgraph::dot::Dot::with_config(g, &[petgraph::dot::Config::EdgeNoLabel])
    )
}

fn total_order_eval(mut meta: BackupMeta) -> EvalGraph {
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
            g.add_edge(p, idx, ());
        }
        prev = Some(idx);

        // Link Logs node.
        let logs = EvalNode::Logs {
            region_id,
            start_index: index + 1,
            end_index: 0,
        };
        let log_idx = g.add_node(logs);
        g.add_edge(idx, log_idx, ());

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
                    g.add_edge(parent, idx, ());
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
                    g.add_edge(idx, sidx, ());
                }
            }
            BackupEvent_Event::PrepareMerge => {
                let target = related_region_ids[0];
                if let Some((cidx, event, id)) = region_merges.remove(&target) {
                    assert_eq!(region_id, id);
                    assert_eq!(BackupEvent_Event::CommitMerge, event);
                    g.add_edge(idx, cidx, ());
                } else {
                    region_merges.insert(region_id, (idx, event, target));
                }
            }
            BackupEvent_Event::CommitMerge => {
                let source = related_region_ids[0];
                if let Some((pidx, event, id)) = region_merges.remove(&source) {
                    assert_eq!(region_id, id);
                    assert_eq!(BackupEvent_Event::PrepareMerge, event);
                    g.add_edge(pidx, idx, ());
                } else {
                    region_merges.insert(region_id, (idx, event, source));
                }
            }
            _ => (),
        }
    }
    g
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::BackupMetaBuilder;

    fn check_graph(g: EvalGraph, s: &str) {
        assert_eq!(s.trim(), dot(&g).trim())
    }

    #[test]
    fn test_total_order_eval_basic() {
        let mut builder = BackupMetaBuilder::new();
        builder.snapshot(1, 10);
        check_graph(
            total_order_eval(builder.build()),
            r#"digraph {
    0 [label="Event(region_id: 1 index: 10 event: Snapshot dependency: 1)"]
    1 [label="Logs { region_id: 1, start_index: 11, end_index: 0 }"]
    0 -> 1
}"#,
        );

        builder.snapshot(2, 10);
        check_graph(
            total_order_eval(builder.build()),
            r#"digraph {
    0 [label="Event(region_id: 1 index: 10 event: Snapshot dependency: 1)"]
    1 [label="Logs { region_id: 1, start_index: 11, end_index: 0 }"]
    2 [label="Event(region_id: 2 index: 10 event: Snapshot dependency: 2)"]
    3 [label="Logs { region_id: 2, start_index: 11, end_index: 0 }"]
    0 -> 1
    0 -> 2
    2 -> 3
}"#,
        );

        builder.clear_event();
        builder.snapshot(1, 10);
        builder.split(1, 12, vec![1, 3]);
        check_graph(
            total_order_eval(builder.build()),
r#"digraph {
    0 [label="Event(region_id: 1 index: 10 event: Snapshot dependency: 3)"]
    1 [label="Logs { region_id: 1, start_index: 11, end_index: 11 }"]
    2 [label="Event(region_id: 1 index: 12 related_region_ids: 1 related_region_ids: 3 event: Split dependency: 4)"]
    3 [label="Logs { region_id: 1, start_index: 13, end_index: 0 }"]
    4 [label="Logs { region_id: 3, start_index: 6, end_index: 0 }"]
    0 -> 1
    0 -> 2
    2 -> 3
    1 -> 2
    2 -> 4
}"#);
        // println!("{}", dot(&total_order_eval(builder.build())));
    }
}
