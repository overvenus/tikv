// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::collections::*;
use std::mem;
use std::ops::Bound::{Included, Unbounded};
use std::path::*;
use std::sync::*;
use std::time::*;

use kvproto::backup::*;
use protobuf::Message;
use raft::eraftpb::Entry;
use raft::eraftpb::Snapshot;

use super::eval::*;
use crate::{log_path, region_path, snapshot_dir, Error, Result, Storage};

const LOGS_BATCH: u64 = 100;

// region_id -> BtreeMap (end index -> start index)
struct FilesCache(HashMap<u64, BTreeMap<u64, u64>>);

impl FilesCache {
    fn cache(&mut self, base: &Path, storage: &dyn Storage, region_id: u64) -> &BTreeMap<u64, u64> {
        self.0.entry(region_id).or_insert_with(|| {
            let files = storage.list_dir(&region_path(base, region_id)).unwrap();
            let mut map = BTreeMap::new();
            trace!("build file cahce {:?}", files);
            for f in files {
                let name = f.file_name().unwrap().to_str().unwrap();
                if name.contains('@') {
                    // Skip snapshot
                    continue;
                }
                trace!("file {:?}", f.file_name());
                let mut is = f.file_name().unwrap().to_str().unwrap().split('_');
                let start: u64 = is.next().unwrap().parse().unwrap();
                let end: u64 = is.next().unwrap().parse().unwrap();
                let prev = map.insert(end, start);
                assert!(prev.is_none(), "{:?}", (start, end, prev));
            }
            map
        })
    }
}

#[derive(Debug)]
pub struct Wait {
    rx: mpsc::Receiver<()>,
    count: usize,
}

impl Wait {
    fn new(count: usize) -> (mpsc::Sender<()>, Wait) {
        assert!(count > 0);
        let (tx, rx) = mpsc::channel();
        (tx, Wait { count, rx })
    }

    fn wait_timeout(&mut self, t: Duration) -> Result<bool> {
        while self.count > 0 {
            match self.rx.recv_timeout(t) {
                Ok(_) => (),
                Err(mpsc::RecvTimeoutError::Timeout) => return Ok(false),
                Err(e) => return Err(Error::Other(e.into())),
            }
            self.count -= 1;
        }
        Ok(true)
    }
}

// TODO: how about Box<dyn Read>?
#[derive(PartialEq, Default)]
pub struct SnapFiles {
    pub meta: (String, Snapshot),
    pub default: (String, Vec<u8>),
    pub write: (String, Vec<u8>),
    pub lock: (String, Vec<u8>),
}

impl std::fmt::Debug for SnapFiles {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("SnapFiles")
            .field("meta", &self.meta)
            .field("default", &(&self.default.0, self.default.1.len()))
            .field("write", &(&self.write.0, self.write.1.len()))
            .field("lock", &(&self.lock.0, self.lock.1.len()))
            .finish()
    }
}

#[derive(Debug)]
pub enum Data {
    Logs(Vec<Entry>),
    Snapshot(SnapFiles),
}

#[derive(Debug)]
pub struct Task {
    pub region_id: u64,
    pub data: Data,
    notify: mpsc::Sender<()>,
}

impl Task {
    pub fn done(&self) {
        self.notify.send(()).unwrap();
    }

    pub fn is_snapshot(&self) -> bool {
        match self.data {
            Data::Snapshot(_) => true,
            Data::Logs(_) => false,
        }
    }

    pub fn take_entries(&mut self) -> Vec<Entry> {
        match self.data {
            Data::Logs(ref mut es) => mem::replace(es, vec![]),
            Data::Snapshot(_) => vec![],
        }
    }

    pub fn take_snapshot(&mut self) -> SnapFiles {
        match self.data {
            Data::Logs(_) => Default::default(),
            Data::Snapshot(ref mut snap) => mem::replace(snap, Default::default()),
        }
    }
}

fn tasks_from_eval_node(
    node: &EvalNode,
    base: &Path,
    storage: &dyn Storage,
    files_cache: &mut FilesCache,
) -> Option<(Vec<Task>, Wait)> {
    let read_snap = |region_id, index, dependency| {
        let path = snapshot_dir(&base, region_id, index, dependency);
        let mut meta = (String::new(), Snapshot::new());
        let mut default = (String::new(), Vec::new());
        let mut write = (String::new(), Vec::new());
        let mut lock = (String::new(), Vec::new());
        for p in storage.list_dir(&path).unwrap().into_iter() {
            let file_name = p.file_name().unwrap();
            let name = file_name.to_str().unwrap();
            if p.extension().unwrap() == "meta" {
                let mut buf = vec![];
                storage.read_file(&p, &mut buf).unwrap();
                meta.1.merge_from_bytes(&buf).unwrap();
                meta.0 = name.to_owned();
                let mut snap_data = kvproto::raft_serverpb::RaftSnapshotData::new();
                snap_data.merge_from_bytes(meta.1.get_data()).unwrap();
            } else if name.contains("default") {
                // default cf
                default.0 = name.to_owned();
                storage.read_file(&p, &mut default.1).unwrap();
            } else if name.contains("write") {
                // write cf
                write.0 = name.to_owned();
                storage.read_file(&p, &mut write.1).unwrap();
            } else {
                // lock cf
                lock.0 = name.to_owned();
                storage.read_file(&p, &mut lock.1).unwrap();
            }
        }
        SnapFiles {
            meta,
            default,
            write,
            lock,
        }
    };

    match node {
        EvalNode::Event(event) => {
            let region_id = event.get_region_id();
            let index = event.get_index();
            let dependency = event.get_dependency();
            let (notify, wait) = Wait::new(1);

            match event.get_event() {
                BackupEvent_Event::Snapshot => {
                    let snap = read_snap(region_id, index, dependency);
                    Some((
                        vec![Task {
                            region_id,
                            notify,
                            data: Data::Snapshot(snap),
                        }],
                        wait,
                    ))
                }
                _ => {
                    let files = files_cache.cache(base, storage, region_id);
                    let entries =
                        fetch_entries(storage, base, files, region_id, index, index).unwrap();
                    Some((
                        vec![Task {
                            region_id,
                            notify,
                            data: Data::Logs(entries),
                        }],
                        wait,
                    ))
                }
            }
        }
        // TODO: fetch logs lazily.
        EvalNode::Logs {
            region_id,
            start_index,
            end_index,
        } => {
            let region_id = *region_id;
            let start_index = *start_index;
            let end_index = *end_index;
            let files = files_cache.cache(base, storage, region_id);
            let last = if end_index == 0 {
                if let Some((end, _)) = files.iter().rev().next() {
                    *end
                } else {
                    return None;
                }
            } else {
                end_index
            };
            let total = last - start_index + 1;
            let quotient = total / LOGS_BATCH;
            let remainder = total % LOGS_BATCH;
            let count = quotient + if remainder > 0 { 1 } else { 0 };
            let (notify, wait) = Wait::new(count as _);
            let mut tasks = Vec::with_capacity(count as _);
            let mut s = start_index;
            for _ in 0..quotient {
                let entries =
                    fetch_entries(storage, base, files, region_id, s, s + LOGS_BATCH - 1).unwrap();
                tasks.push(Task {
                    region_id,
                    notify: notify.clone(),
                    data: Data::Logs(entries),
                });
                s += LOGS_BATCH;
            }
            if remainder > 0 {
                let entries =
                    fetch_entries(storage, base, files, region_id, last - remainder + 1, last)
                        .unwrap();
                tasks.push(Task {
                    region_id,
                    notify: notify.clone(),
                    data: Data::Logs(entries),
                });
            }
            Some((tasks, wait))
        }
    }
}

fn extract_range_from_slice(fstart: u64, fend: u64, start: u64, end: u64) -> (usize, usize) {
    let s = (cmp::max(fstart, start) - fstart) as usize;
    let e = if fstart == fend || end == fstart {
        1
    } else if fend <= end || end == 0 {
        fend - fstart + 1
    } else if end < fend {
        end - fstart + 1
    } else {
        unreachable!()
    } as usize;

    (s, e)
}

fn fetch_entries(
    storage: &dyn Storage,
    base: &Path,
    files: &BTreeMap<u64, u64>,
    region_id: u64,
    start: u64,
    end: u64,
) -> Result<Vec<Entry>> {
    let mut es = vec![];
    let mut buf = vec![];
    for (fend, fstart) in files.range((Included(start), Unbounded)) {
        let fstart = *fstart;
        let fend = *fend;
        if fstart > end && end != 0 {
            break;
        }
        let path = log_path(base, region_id, fstart, fend);
        buf.clear();
        storage.read_file(&path, &mut buf)?;
        let mut batch = EntryBatch::new();
        batch.merge_from_bytes(&buf).unwrap();
        let entries = batch.entries.to_vec();
        let (s, e) = extract_range_from_slice(fstart, fend, start, end);
        debug!(
            "fetch_entries {:?} {:?} {:?} {:?}",
            (fstart, fend),
            (s, e),
            (start, end),
            entries
        );
        es.extend_from_slice(&entries[s..e]);
    }
    Ok(es)
}

pub trait Runnable {
    fn run(&mut self, task: Task);
}

pub struct Executor {
    storage: Arc<dyn Storage>,
    graph: EvalGraph,
    order: Vec<NodeIndex>,
    base: PathBuf,
}

impl Executor {
    pub(crate) fn new(
        storage: Arc<dyn Storage>,
        graph: EvalGraph,
        order: Vec<NodeIndex>,
        base: PathBuf,
    ) -> Executor {
        Executor {
            storage,
            graph,
            order,
            base,
        }
    }

    pub fn tasks(self) -> impl Iterator<Item = (Vec<Task>, Wait)> {
        let Executor {
            storage,
            graph,
            order,
            base,
        } = self;

        let mut region_files = FilesCache(HashMap::new());
        order.into_iter().filter_map(move |idx| {
            let node = &graph[idx];
            tasks_from_eval_node(node, &base, &storage, &mut region_files)
        })
    }

    pub fn execute<R: Runnable>(self, mut runnable: R) {
        for (tasks, mut w) in self.tasks() {
            let len = tasks.len();
            for t in tasks {
                runnable.run(t);
            }
            loop {
                let done = w.wait_timeout(Duration::from_secs(1)).unwrap();
                if done {
                    break;
                } else {
                    warn!("execute task too long"; "tasks" => len);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    use kvproto::metapb::*;
    use kvproto::raft_serverpb::*;

    fn entries_batch_bytes(start: u64, end: u64) -> Vec<u8> {
        entries_batch(start, end).write_to_bytes().unwrap()
    }
    fn entries_batch(start: u64, end: u64) -> EntryBatch {
        let mut batch = EntryBatch::new();
        for i in start..=end {
            let mut entry = Entry::new();
            entry.set_index(i);
            batch.entries.push(entry);
        }
        batch
    }
    fn snapshot_bytes(region_id: u64, index: u64) -> Vec<u8> {
        snapshot(region_id, index).meta.1.write_to_bytes().unwrap()
    }
    fn snapshot(region_id: u64, index: u64) -> SnapFiles {
        let mut snap = Snapshot::new();
        snap.mut_metadata().set_index(index);
        let mut data = RaftSnapshotData::new();
        let mut region = Region::new();
        region.set_id(region_id);
        data.set_region(region);
        snap.set_data(data.write_to_bytes().unwrap());
        let cf = (String::new(), Vec::new());
        SnapFiles {
            meta: (String::new(), snap),
            default: cf.clone(),
            write: cf.clone(),
            lock: cf,
        }
    }

    struct StorageBuilder {
        bm: BackupManager,
        map: HashMap<u64, BTreeMap<u64, u64>>,
        base: PathBuf,
    }
    impl StorageBuilder {
        fn new(base: &Path) -> Self {
            let ls = LocalStorage::new(base).unwrap();
            let map = HashMap::default();
            let bm = BackupManager::new(1, base, Box::new(ls)).unwrap();
            bm.step(BackupState::Start).unwrap();
            let base = base.to_owned();
            StorageBuilder { bm, map, base }
        }
        fn logs(&mut self, region_id: u64, start: u64, end: u64) -> &mut Self {
            self.bm.start_backup_region(region_id).unwrap();
            self.bm
                .save_logs(region_id, start, end, &entries_batch_bytes(start, end))
                .unwrap();
            self.map.entry(region_id).or_default().insert(end, start);
            self
        }
        fn snapshot(&mut self, region_id: u64, index: u64) -> &mut Self {
            let tmp_dir = self.bm.tmp_dir(&format!("{}{}", region_id, index)).unwrap();
            // Meta file
            let meta_dst = tmp_dir.path().join(format!("{}_{}.meta", region_id, index));
            // Save raft snapshot.
            self.bm
                .storage
                .save_file(&meta_dst, &snapshot_bytes(region_id, index))
                .unwrap();
            self.bm.start_backup_region(region_id).unwrap();
            self.bm
                .save_snapshot(region_id, index, tmp_dir.path())
                .unwrap();
            self
        }
        #[allow(dead_code)]
        fn debug(&self, path: &str) {
            let mut opt = fs_extra::dir::CopyOptions::new();
            opt.copy_inside = true;
            fs_extra::copy_items(&vec![&self.base], path, &opt).unwrap();
        }
        fn build(self) -> (BackupManager, FilesCache) {
            (self.bm, FilesCache(self.map))
        }
    }

    #[test]
    fn test_fetch_entries() {
        let temp_dir = TempDir::new("test_fetch_entries").unwrap();
        let path = temp_dir.path();
        let region_id = 1;
        let mut builder = StorageBuilder::new(path);
        builder
            .logs(region_id, 6, 6)
            .logs(region_id, 7, 9)
            .logs(region_id, 10, 14);
        let (bm, FilesCache(map)) = builder.build();

        let ls = LocalStorage::new(&path).unwrap();
        let current = bm.current_dir();
        let check = |start, end| {
            assert_eq!(
                fetch_entries(&ls, current, &map[&region_id], region_id, start, end).unwrap(),
                entries_batch(start, if end == 0 { 14 } else { end })
                    .entries
                    .to_vec(),
            );
        };

        for s in 6..=14 {
            for e in s..=14 {
                check(s, e);
            }
        }
    }

    #[test]
    fn test_tasks_from_eval_node() {
        let temp_dir = TempDir::new("test_tasks_from_eval_node").unwrap();
        let path = temp_dir.path();
        let region_id = 1;
        let region_id2 = 2;
        let snap_index = 5 * LOGS_BATCH;
        let mut builder = StorageBuilder::new(path);
        builder
            .logs(region_id, 6, 6)
            .logs(region_id, 7, 9)
            .logs(region_id, 10, 14)
            .logs(region_id, 15, 15 + 4 * LOGS_BATCH)
            .snapshot(region_id, snap_index);
        // The latest dependency always larger than the last dep by 1.
        let snap_dep = builder.bm.dependency.get() - 1;
        // Add snapshot only region2.
        builder.snapshot(region_id2, snap_index);
        let (bm, FilesCache(map)) = builder.build();
        let ls = LocalStorage::new(&path).unwrap();
        let current = bm.current_dir();

        // Check snasphot only region2
        let logs_node = EvalNode::Logs {
            region_id: region_id2,
            start_index: snap_index + 1,
            end_index: 0,
        };
        let mut cache = FilesCache(HashMap::default());
        assert!(tasks_from_eval_node(&logs_node, current, &ls, &mut cache).is_none());

        // Check whether it can convert eval node to tasks correctly.
        let check = |start, end, count| {
            let logs_node = EvalNode::Logs {
                region_id,
                start_index: start,
                end_index: end,
            };
            let mut cache = FilesCache(HashMap::default());
            let (mut tasks, mut w) =
                tasks_from_eval_node(&logs_node, current, &ls, &mut cache).unwrap();
            assert_eq!(map[&region_id], cache.0[&region_id]);
            assert_eq!(tasks.len(), count);
            let mut s = start;
            for i in 0..count {
                let e = if end != 0 {
                    cmp::min(end, s + LOGS_BATCH - 1)
                } else {
                    s + LOGS_BATCH - 1
                };
                let es = tasks[i].take_entries();
                assert!(es.len() <= LOGS_BATCH as _);
                assert_eq!(
                    fetch_entries(&ls, current, &map[&region_id], region_id, s, e).unwrap(),
                    es,
                );
                s = e + 1;
                tasks[i].done();
            }
            assert!(w.wait_timeout(Duration::from_millis(1)).unwrap());
        };
        for s in 6..=14 {
            for e in s..=14 {
                check(s, e, 1);
                check(s, e + LOGS_BATCH, 2);
                check(s, e + LOGS_BATCH * 2, 3);
                check(s, e + LOGS_BATCH * 3, 4);
            }
        }

        // Special case, end_index = 0, get all raft logs.
        check(6, 0, 5);

        // Check convert event node to task
        let check_event = |region_id, index, e, dep| {
            let mut event = BackupEvent::new();
            event.set_event(e);
            event.set_region_id(region_id);
            event.set_index(index);
            event.set_dependency(dep);
            let snap_node = EvalNode::Event(event);;
            let mut cache = FilesCache(HashMap::default());
            let (mut tasks, _) =
                tasks_from_eval_node(&snap_node, current, &ls, &mut cache).unwrap();
            assert_eq!(tasks.len(), 1);
            if e == BackupEvent_Event::Snapshot {
                assert_eq!(
                    tasks[0].take_snapshot().meta.1,
                    snapshot(region_id, snap_index).meta.1
                );
            } else {
                assert_eq!(tasks.len(), 1);
                assert_eq!(
                    tasks[0].take_entries(),
                    entries_batch(index, index).entries.into_vec(),
                );
            }
        };

        // Check convert event node to task
        check_event(region_id, snap_index, BackupEvent_Event::Snapshot, snap_dep);
        check_event(region_id, 6, BackupEvent_Event::Split, 0);
    }

    #[test]
    fn test_wait() {
        for count in 1..20 {
            let (sender, mut w) = Wait::new(count as _);
            for _ in 0..count {
                assert!(!w.wait_timeout(Duration::from_millis(1)).unwrap());
                sender.send(()).unwrap();
            }
            assert!(w.wait_timeout(Duration::from_millis(count)).unwrap());
        }

        // Drop sender should wait an error.
        let (_, mut w) = Wait::new(1);
        w.wait_timeout(Duration::from_millis(100)).unwrap_err();
    }
}
