// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::backup::*;

#[derive(Clone, Default)]
pub struct BackupMetaBuilder {
    dep_counter: u64,

    cluster_id: u64,
    cluster_version: String,
    state: BackupState,
    start_dependency: u64,
    complete_dependency: u64,
    incremental_dependencies: Vec<u64>,
    events: Vec<BackupEvent>,
}

impl BackupMetaBuilder {
    pub fn new() -> BackupMetaBuilder {
        BackupMetaBuilder::default()
    }

    pub fn build(&self) -> BackupMeta {
        let mut meta = BackupMeta::new();
        meta.set_cluster_id(self.cluster_id);
        meta.set_cluster_version(self.cluster_version.clone());
        meta.set_state(self.state);
        meta.set_start_dependency(self.start_dependency);
        meta.set_complete_dependency(self.complete_dependency);
        meta.set_incremental_dependencies(self.incremental_dependencies.clone());
        meta.set_events(self.events.clone().into());
        meta
    }

    fn alloc_dep(&mut self) -> u64 {
        self.dep_counter += 1;
        self.dep_counter
    }

    pub fn clear_event(&mut self) -> &mut Self {
        self.events.clear();
        self
    }

    pub fn add_event(
        &mut self,
        region_id: u64,
        index: u64,
        e: BackupEvent_Event,
        related_region_ids: Vec<u64>,
    ) -> &mut Self {
        let mut event = BackupEvent::new();
        event.set_region_id(region_id);
        event.set_index(index);
        event.set_event(e);
        event.set_related_region_ids(related_region_ids);
        event.set_dependency(self.alloc_dep());
        self.events.push(event);
        self
    }

    pub fn snapshot(&mut self, region_id: u64, index: u64) -> &mut Self {
        self.add_event(region_id, index, BackupEvent_Event::Snapshot, vec![])
    }

    pub fn split(&mut self, region_id: u64, index: u64, related_region_ids: Vec<u64>) -> &mut Self {
        self.add_event(
            region_id,
            index,
            BackupEvent_Event::Split,
            related_region_ids,
        )
    }

    pub fn prepare_merge(&mut self, region_id: u64, index: u64, target_region: u64) -> &mut Self {
        self.add_event(
            region_id,
            index,
            BackupEvent_Event::PrepareMerge,
            vec![target_region],
        )
    }

    pub fn commit_merge(&mut self, region_id: u64, index: u64, source_region: u64) -> &mut Self {
        self.add_event(
            region_id,
            index,
            BackupEvent_Event::CommitMerge,
            vec![source_region],
        )
    }

    pub fn rollback_merge(&mut self, region_id: u64, index: u64, target_region: u64) -> &mut Self {
        self.add_event(
            region_id,
            index,
            BackupEvent_Event::RollbackMerge,
            vec![target_region],
        )
    }
}

macro_rules! builder {
    ($($field:ident: $t:ty,)*) => {
        impl BackupMetaBuilder {
            $(builder!{
                _doc concat!("sets ", stringify!($field)),
                pub fn $field(&mut self, v: $t) -> &mut Self {
                    self.$field = v;
                    self
                }
            })*
        }
    };
    (_doc $x:expr, $($tt:tt)*) => {
        #[doc = $x]
        $($tt)*
    };
}

builder!(
    cluster_id: u64,
    cluster_version: String,
    state: BackupState,
    start_dependency: u64,
    complete_dependency: u64,
    incremental_dependencies: Vec<u64>,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        let mut meta = BackupMeta::new();
        // u64,
        meta.cluster_id = 1;
        // ::std::string::String,
        meta.cluster_version = "a".to_owned();
        // BackupState,
        meta.state = BackupState::Start;
        // u64,
        meta.start_dependency = 1;
        // u64,
        meta.complete_dependency = 1;
        // ::std::vec::Vec<u64>,
        meta.incremental_dependencies = vec![1];
        // ::protobuf::RepeatedField<BackupEvent>,
        let mut event = BackupEvent::new();
        event.set_dependency(1);
        event.set_region_id(1);
        event.set_index(1);
        event.set_event(BackupEvent_Event::Split);
        event.set_related_region_ids(vec![1].into());
        meta.events = vec![event].into();

        let m = BackupMetaBuilder::new()
            .cluster_id(1)
            .cluster_version("a".to_owned())
            .state(BackupState::Start)
            .start_dependency(1)
            .complete_dependency(1)
            .incremental_dependencies(vec![1])
            .split(1, 1, vec![1])
            .build();

        assert_eq!(meta, m);
    }
}
