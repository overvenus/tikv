// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::{Entry, HashMap};
use std::path::Path;

use kvproto::backup::{BackupEvent_Event, BackupMeta};

use super::*;

pub fn check_meta(mut meta: BackupMeta) -> Result<RegionEvents> {
    let mut err = String::new();
    meta.mut_events()
        .sort_by(|l, r| l.get_dependency().cmp(&r.get_dependency()));
    // First, we check if there is any duplicate dependency with different events.
    let mut map = HashMap::with_capacity(meta.get_events().len());
    for e in meta.get_events() {
        match map.entry(e.get_dependency()) {
            Entry::Occupied(value) => {
                let v = value.get();
                err += &format!("dup dependency {:?}, {:?}\n\n", v, e);
                if *v != e {
                    err += &format!(
                        "different events with the same dependency {:?}, {:?}\n\n",
                        v, e
                    );
                }
            }
            Entry::Vacant(v) => {
                v.insert(e);
            }
        }
    }
    drop(map);

    // Then, we check if there any events has a cycle. Eg:
    //   1. region A has two events: {dep: 1, index: 2} {dep: 2, index: 1}
    //   2. region A splits into A and B:
    //      - {region: A, dep: 10, event: Split, related_region: B}
    //      - B exist an event whose dep is less than 10.
    //   3. region A merges into B:
    //      - Prepare merge must be the last event or the next must be rollback
    //      - Rollback merge's dep < prepare merge
    //      - B's commit merge dep < A's prepare merge
    //      - B has commit merge and A has rollback merge
    let mut region_events = HashMap::new();
    for e in meta.take_events().into_vec() {
        region_events
            .entry(e.get_region_id())
            .or_insert_with(Vec::new)
            .push(e);
    }
    for (region_id, events) in &region_events {
        for i in 0..events.len() {
            let cur = &events[i];
            if i != 0 {
                let prev = &events[i - 1];
                if cur.get_index() < prev.get_index() {
                    err += &format!(
                        "cycle detected dep1 < dep2, index2 < index1 {:?}, {:?}\n\n",
                        prev, cur
                    );
                }
            }
            match cur.get_event() {
                BackupEvent_Event::Split => {
                    for id in cur.get_related_region_ids() {
                        if id == region_id {
                            // Do not check self.
                            continue;
                        }
                        if let Some(events) = region_events.get(id) {
                            if events.is_empty() {
                                continue;
                            }
                            let head = &events[0];
                            if head.get_dependency() < cur.get_dependency() {
                                err += &format!(
                                    "cycle detected region {} splits {:?},\
                                     {} has smaller dependency, {:?}, {:?}\n\n",
                                    region_id,
                                    cur.get_related_region_ids(),
                                    head.get_region_id(),
                                    cur,
                                    head
                                );
                            }
                        }
                    }
                }
                BackupEvent_Event::PrepareMerge => {
                    let mut has_rollbacked = false;
                    if i != events.len() - 1 {
                        if events[i + 1].get_event() != BackupEvent_Event::RollbackMerge {
                            err += &format!(
                                "bad merge {:?}, unexpected {:?} after {:?}",
                                cur,
                                events[i + 1],
                                cur
                            );
                        } else {
                            has_rollbacked = true;
                        }
                    }
                    let target_id = cur.get_related_region_ids()[0];
                    if let Some(target_events) = region_events.get(&target_id) {
                        if !has_rollbacked {
                            if let Some(e) = target_events.iter().find(|e| {
                                e.get_event() == BackupEvent_Event::CommitMerge
                                    && e.get_related_region_ids()[0] == *region_id
                            }) {
                                if e.get_dependency() < cur.get_dependency() {
                                    err += &format!(
                                        "bad merge {:?}, commit dep {} < prepare dep {}",
                                        cur,
                                        e.get_dependency(),
                                        cur.get_dependency()
                                    );
                                }
                            }
                        }
                    }
                    // TODO(backup): what if target is not backuped yet?
                }
                BackupEvent_Event::RollbackMerge => {
                    // TODO(backup): what if it's the first event?
                    let prev = &events[i - 1];
                    if prev.get_event() != BackupEvent_Event::PrepareMerge {
                        err += &format!("bad merge {:?}, prepare merge not found", cur);
                    }
                }
                BackupEvent_Event::CommitMerge => {
                    let source_id = cur.get_related_region_ids()[0];
                    if let Some(source_events) = region_events.get(&source_id) {
                        if let Some(last) = source_events.last() {
                            if last.get_event() != BackupEvent_Event::PrepareMerge {
                                err += &format!("bad merge {:?}, commit found but no prepare", cur);
                            }
                        }
                    } else {
                        err += &format!("bad merge {:?}, commit but no source", cur);
                    }
                }
                // TODO(backup): Check other events.
                _ => (),
            }
        }
    }
    if err.is_empty() {
        Ok(region_events)
    } else {
        Err(Error::Other(err.into()))
    }
}

/// The very first raft log index of TiKV.
const TIKV_INITIAL_INDEX: u64 = 6;

pub fn check_data(region_events: RegionEvents, base: &Path, storage: &dyn Storage) -> Result<()> {
    let mut err = String::new();
    let mut index_vec = Vec::new();
    let mut snap_vec = Vec::new();
    // Check raft log and snapshot.
    // 1. raft log must be continuous, no gap is allowed, unless
    // 2. there is a snapshot, and gap is only allowed between
    //    raft log and sanpshot.
    // TODO(backup): check snapshot events.
    for region_id in region_events.keys() {
        let path = region_path(base, *region_id);
        let list = match storage.list_dir(&path) {
            Ok(list) => list,
            Err(e) => {
                err += &format!("fail to list {}, error {:?}\n", path.display(), e);
                continue;
            }
        };
        index_vec.clear();
        snap_vec.clear();
        for p in list {
            let name = p.file_name().unwrap().to_str().unwrap();
            if name.contains('@') {
                let index: u64 = name.split('@').next().unwrap().parse().unwrap();
                snap_vec.push(index);
            } else {
                let mut is = name.split('_');
                let index1: u64 = is.next().unwrap().parse().unwrap();
                let index2: u64 = is.next().unwrap().parse().unwrap();
                index_vec.push((index1, index2));
            }
        }
        index_vec.sort_by(|r, l| r.cmp(&l));
        snap_vec.sort();

        let mut index1 = 0;
        let mut index2 = 0;
        for (i1, i2) in index_vec.drain(..) {
            if i1 > i2 {
                err += &format!(
                    "out of order logs region_id: {} ({}_{})\n",
                    region_id, i1, i2
                );
            }
            // Ignore duplicate raft logs, it is caused by commit merge.
            if index2 + 1 != i1 && index2 != i1 {
                // [0, TIKV_INITIAL_INDEX] is suppose to be empty.
                if !(index2 == 0 && i1 == TIKV_INITIAL_INDEX
                    || snap_vec.iter().any(|i| i + 1 == i1))
                {
                    err += &format!(
                        "gap between logs region_id: {} [{}_{}, {}_{}]\n",
                        region_id, index1, index2, i1, i2,
                    );
                }
            }
            index1 = i1;
            index2 = i2;
        }
    }
    if err.is_empty() {
        Ok(())
    } else {
        Err(Error::Other(err.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_meta() {
        fn check(err: &Error, expect: &str) {
            let msg = format!("{:?}", err);
            assert!(msg.contains(expect), "{}", msg)
        }

        let mut dup_dep = BackupMeta::new();
        let mut event1 = BackupEvent::new();
        event1.set_region_id(1);
        event1.set_dependency(1);
        event1.set_event(BackupEvent_Event::Split);
        dup_dep.mut_events().push(event1.clone());
        let mut event2 = event1;
        event2.set_event(BackupEvent_Event::Snapshot);
        dup_dep.mut_events().push(event2);
        let err = check_meta(dup_dep).unwrap_err();
        check(&err, "dup dependency");
        check(&err, "different events");

        let mut cycle_dep = BackupMeta::new();
        let mut event1 = BackupEvent::new();
        event1.set_region_id(1);
        event1.set_dependency(2);
        event1.set_index(2);
        cycle_dep.mut_events().push(event1.clone());
        let mut event2 = event1;
        event2.set_dependency(3);
        event2.set_index(1);
        cycle_dep.mut_events().push(event2);
        check(
            &check_meta(cycle_dep).unwrap_err(),
            "dep1 < dep2, index2 < index1",
        );

        let mut cycle_split = BackupMeta::new();
        let mut event1 = BackupEvent::new();
        event1.set_region_id(1);
        event1.set_dependency(2);
        event1.set_event(BackupEvent_Event::Split);
        event1.set_related_region_ids(vec![2]);
        cycle_split.mut_events().push(event1.clone());
        let mut event2 = event1;
        event2.set_region_id(2);
        event2.set_dependency(1);
        event2.set_index(1);
        cycle_split.mut_events().push(event2);
        check(&check_meta(cycle_split).unwrap_err(), "splits");

        // Prepare merge must be the last event or the next must be rollback
        let mut bad_merge = BackupMeta::new();
        let mut event0 = BackupEvent::new();
        event0.set_region_id(1);
        event0.set_dependency(1);
        event0.set_event(BackupEvent_Event::Snapshot);
        bad_merge.mut_events().push(event0.clone());
        let mut event1 = BackupEvent::new();
        event1.set_region_id(1);
        event1.set_dependency(3);
        event1.set_event(BackupEvent_Event::PrepareMerge);
        event1.set_related_region_ids(vec![2]);
        bad_merge.mut_events().push(event1.clone());

        let mut unexpected_merge = bad_merge.clone();
        let mut event2 = BackupEvent::new();
        event2.set_region_id(1);
        event2.set_dependency(4);
        event2.set_event(BackupEvent_Event::Snapshot);
        unexpected_merge.mut_events().push(event2.clone());
        check(
            &check_meta(unexpected_merge.clone()).unwrap_err(),
            "unexpected",
        );

        let mut good_merge = unexpected_merge.clone();
        good_merge.mut_events()[2].set_event(BackupEvent_Event::RollbackMerge);
        check_meta(good_merge.clone()).unwrap();
        good_merge.mut_events().pop();
        check_meta(good_merge.clone()).unwrap();

        // Rollback merge's dep < prepare merge
        let mut no_prepare_merge = bad_merge.clone();
        let mut event3 = event2.clone();
        event3.set_dependency(2);
        event3.set_event(BackupEvent_Event::RollbackMerge);
        no_prepare_merge.mut_events().push(event3);
        check(
            &check_meta(no_prepare_merge.clone()).unwrap_err(),
            "prepare merge not found",
        );

        // B's commit merge dep < A's prepare merge
        let mut cycle_merge = bad_merge.clone();
        let mut eventb = BackupEvent::new();
        eventb.set_region_id(2);
        eventb.set_dependency(2);
        eventb.set_event(BackupEvent_Event::CommitMerge);
        eventb.set_related_region_ids(vec![1]);
        cycle_merge.mut_events().push(eventb);
        check(&check_meta(cycle_merge.clone()).unwrap_err(), "commit dep");

        let mut good_merge = cycle_merge.clone();
        good_merge.mut_events()[2].set_dependency(4);
        check_meta(good_merge.clone()).unwrap();

        // B has commit merge and A has rollback merge
        let mut commit_rollback_merge = good_merge.clone();
        let mut event4 = BackupEvent::new();
        event4.set_region_id(1);
        event4.set_dependency(5);
        event4.set_event(BackupEvent_Event::RollbackMerge);
        commit_rollback_merge.mut_events().push(event4);
        check(
            &check_meta(commit_rollback_merge.clone()).unwrap_err(),
            "commit found but no prepare",
        );
    }

    #[test]
    fn test_check_data() {
        #[derive(Debug)]
        struct Case {
            snaps: Vec<(u64, u64)>, // index and term
            logs: Vec<(u64, u64)>,
            expect: Option<&'static str>,
        }
        let cases = vec![
            Case {
                snaps: vec![(1, 1), (10, 1)],
                logs: vec![(2, 3), (4, 4), (11, 13)],
                expect: None, // ok
            },
            Case {
                snaps: vec![],
                logs: vec![(TIKV_INITIAL_INDEX, TIKV_INITIAL_INDEX + 1)],
                expect: None,
            },
            Case {
                snaps: vec![],
                logs: vec![
                    (TIKV_INITIAL_INDEX, TIKV_INITIAL_INDEX + 2),
                    (TIKV_INITIAL_INDEX + 3, TIKV_INITIAL_INDEX + 3), // duplicated entry
                    (TIKV_INITIAL_INDEX + 3, TIKV_INITIAL_INDEX + 4),
                ],
                expect: None,
            },
            Case {
                snaps: vec![],
                logs: vec![(11, 11), (12, 12), (13, 13), (6, 8), (9, 10), (9, 9)],
                expect: None,
            },
            Case {
                snaps: vec![(805, 1)],
                logs: vec![(806, 806), (807, 812), (812, 812), (813, 836)],
                expect: None,
            },
            Case {
                snaps: vec![],
                logs: vec![(2, 3)],
                expect: Some("gap between"),
            },
            Case {
                snaps: vec![(1, 1)],
                logs: vec![(2, 3), (4, 4), (11, 13)],
                expect: Some("gap between"),
            },
            Case {
                snaps: vec![(1, 1)],
                logs: vec![(2, 3), (4, 4), (5, 5)],
                expect: None,
            },
            Case {
                snaps: vec![(1, 1)],
                logs: vec![(2, 3), (4, 3)],
                expect: Some("out of order"),
            },
        ];
        for case in cases {
            let tmp = TempDir::new("test_check_data").unwrap();
            let base = Path::new("current");
            let storage = LocalStorage::new(tmp.path()).unwrap();
            storage.make_dir(&region_path(base, 1)).unwrap();
            for s in &case.snaps {
                storage.make_dir(&snapshot_dir(base, 1, s.0, 0)).unwrap();
            }
            for l in &case.logs {
                storage
                    .save_file(&log_path(base, 1, l.0, l.1), &[])
                    .unwrap();
            }
            let mut region_events = HashMap::new();
            region_events.insert(1, Default::default());
            if let Err(e) = check_data(region_events, base, &storage) {
                assert!(
                    format!("{:?}", e).contains(
                        case.expect
                            .unwrap_or_else(|| panic!("{:?} | {:?}", e, case))
                    ),
                    "{:?} | {:?}",
                    e,
                    case
                );
            } else {
                assert!(case.expect.is_none(), "{:?}", case);
            }
        }
    }
}
