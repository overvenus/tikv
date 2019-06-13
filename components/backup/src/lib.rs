// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]

#[macro_use(
    kv,
    slog_kv,
    slog_debug,
    slog_info,
    slog_error,
    slog_record,
    slog_b,
    slog_log,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate quick_error;

use std::collections::hash_map::{Entry, HashMap};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;
use std::{error, result};

use kvproto::backup::{BackupEvent, BackupEvent_Event, BackupMeta, BackupState, Error as ErrorPb};
use protobuf::Message;
use rand::Rng;
use tempdir::TempDir;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{}", err)
        }
        Io(err: IoError) {
            from()
            cause(err)
            display("{}", err)
            description(err.description())
        }
        Step(current: BackupState, request: BackupState) {
            display("current {:?}, request {:?}", current, request)
            description("can not step backup state")
        }
        ClusterID(current: u64, request: u64) {
            display("current {:?}, request {:?}", current, request)
            description("cluster ID mismatch")
        }
    }
}

impl Into<ErrorPb> for Error {
    fn into(self) -> ErrorPb {
        let mut err = ErrorPb::new();
        match self {
            Error::Step(current, request) => {
                err.mut_state_step_error().set_current(current);
                err.mut_state_step_error().set_request(request);
            }
            Error::ClusterID(current, request) => {
                err.mut_cluster_id_error().set_current(current);
                err.mut_cluster_id_error().set_request(request);
            }
            other => {
                err.set_msg(format!("{:?}", other));
            }
        }
        err
    }
}

pub type Result<T> = result::Result<T, Error>;

pub trait Dependency: Sync + Send {
    fn get(&self) -> u64;
    fn alloc_number(&self) -> u64;
}

impl Dependency for AtomicU64 {
    fn get(&self) -> u64 {
        self.load(Ordering::Relaxed)
    }

    fn alloc_number(&self) -> u64 {
        self.fetch_add(1, Ordering::Relaxed)
    }
}

fn maybe_create_dir(path: &Path) -> IoResult<()> {
    if let Err(e) = fs::create_dir_all(path) {
        if e.kind() != ErrorKind::AlreadyExists {
            return Err(e);
        }
    }
    Ok(())
}

const CURRENT_DIR: &str = "current";
const BACKUP_META_NAME: &str = "backup.meta";
const LOCAL_STORAGE_TMP_DIR: &str = "localtmp";
const LOCAL_STORAGE_TEP_FILE_SUFFIX: &str = "tmp";

// TODO(backup): Simplify the trait.
pub trait Storage: Sync + Send {
    fn rename_dir(&self, from: &Path, to: &Path) -> IoResult<()>;
    fn make_dir(&self, path: &Path) -> IoResult<()>;
    fn list_dir(&self, path: &Path) -> IoResult<Vec<PathBuf>>;
    fn save_dir(&self, path: &Path, src: &Path) -> IoResult<()>;
    fn save_file(&self, path: &Path, content: &[u8]) -> IoResult<()>;
    fn read_file(&self, path: &Path, buf: &mut Vec<u8>) -> IoResult<()>;
}

impl Storage for Box<dyn Storage> {
    fn rename_dir(&self, from: &Path, to: &Path) -> IoResult<()> {
        (**self).rename_dir(from, to)
    }
    fn make_dir(&self, path: &Path) -> IoResult<()> {
        (**self).make_dir(path)
    }
    fn list_dir(&self, path: &Path) -> IoResult<Vec<PathBuf>> {
        (**self).list_dir(path)
    }
    fn save_dir(&self, path: &Path, src: &Path) -> IoResult<()> {
        (**self).save_dir(path, src)
    }
    fn save_file(&self, path: &Path, content: &[u8]) -> IoResult<()> {
        (**self).save_file(path, content)
    }
    fn read_file(&self, path: &Path, buf: &mut Vec<u8>) -> IoResult<()> {
        (**self).read_file(path, buf)
    }
}

#[derive(Clone)]
pub struct LocalStorage {
    base: PathBuf,
    tmp: PathBuf,
}

impl LocalStorage {
    pub fn new(base: &Path) -> IoResult<LocalStorage> {
        info!("create local storage"; "base" => base.display());
        let tmp = base.join(LOCAL_STORAGE_TMP_DIR);
        maybe_create_dir(&tmp)?;
        Ok(LocalStorage {
            base: base.to_owned(),
            tmp,
        })
    }

    // TODO(backup): gc tmp files.
    fn tmp_path(&self, path: &Path) -> PathBuf {
        let uid: u64 = rand::thread_rng().gen();
        let tmp_suffix = format!("{}{:016x}", LOCAL_STORAGE_TEP_FILE_SUFFIX, uid);
        self.tmp.join(path).with_extension(tmp_suffix)
    }
}

// TODO(backup): fsync dirs.
impl Storage for LocalStorage {
    fn rename_dir(&self, from: &Path, to: &Path) -> IoResult<()> {
        let from = self.base.join(from);
        let to = self.base.join(to);
        fs::rename(from, to)
    }

    fn make_dir(&self, path: &Path) -> IoResult<()> {
        let path = self.base.join(path);
        fs::create_dir_all(path)
    }

    fn list_dir(&self, path: &Path) -> IoResult<Vec<PathBuf>> {
        let path = self.base.join(path);
        let rd = path.read_dir()?;
        let mut buf = vec![];
        for e in rd {
            let e = e?;
            let p = e.path();
            buf.push(p.strip_prefix(&self.base).unwrap().to_owned());
        }
        Ok(buf)
    }

    fn save_dir(&self, path: &Path, src: &Path) -> IoResult<()> {
        let n = path.file_name().unwrap();
        let tmp = self.tmp_path(Path::new(n));
        fs::create_dir_all(&tmp).unwrap();
        src.metadata().unwrap();
        for entry in src.read_dir()? {
            let entry = entry?;
            let ty = entry.file_type()?;
            assert!(ty.is_file());
            let name = entry.file_name();
            let tmp_dst = tmp.join(name);
            fs::copy(entry.path(), tmp_dst)?;
        }
        fs::rename(tmp, self.base.join(path))
    }

    fn save_file(&self, path: &Path, content: &[u8]) -> IoResult<()> {
        // Sanitize check, do not save file if its parent not found.
        if let Some(p) = path.parent() {
            fs::metadata(self.base.join(p))?;
        }
        let name = path.file_name().unwrap();
        let tmp_path = self.tmp_path(Path::new(name));
        let mut tmp_f = File::create(&tmp_path)?;
        // TODO: handle errors.
        tmp_f.write_all(content)?;
        tmp_f.sync_all()?;
        fs::rename(tmp_path, self.base.join(path))
    }

    fn read_file(&self, path: &Path, buf: &mut Vec<u8>) -> IoResult<()> {
        let mut file = File::open(self.base.join(path))?;
        file.read_to_end(buf).map(|_| ())
    }
}

struct Meta {
    backup_meta: BackupMeta,
    backup_regions: HashSet<u64>,
    buf: Vec<u8>,
}

impl Meta {
    fn state(&self) -> BackupState {
        self.backup_meta.get_state()
    }

    fn step(&mut self, request: BackupState) -> Result<BackupState> {
        let current = self.state();
        assert!(current != BackupState::Unknown);
        match (current, request) {
            (BackupState::Stop, BackupState::Stop)
            | (BackupState::Stop, BackupState::StartFullBackup)
            | (BackupState::StartFullBackup, BackupState::FinishFullBackup)
            | (BackupState::FinishFullBackup, BackupState::StartFullBackup)
            | (BackupState::FinishFullBackup, BackupState::IncrementalBackup)
            | (BackupState::IncrementalBackup, BackupState::IncrementalBackup)
            | (_, BackupState::Stop) => (),
            (current, request) => {
                return Err(Error::Step(current, request));
            }
        }
        self.backup_meta.set_state(request);
        Ok(request)
    }

    fn last_dependency(&self) -> u64 {
        let events = self.backup_meta.get_events();
        events.iter().map(|e| e.get_dependency()).max().unwrap_or(0)
    }

    fn save_to(&mut self, path: &Path, storage: &dyn Storage) -> Result<()> {
        self.buf.clear();
        self.backup_meta.write_to_vec(&mut self.buf).unwrap();
        storage.save_file(&path, &self.buf)?;
        Ok(())
    }

    fn start(&mut self, region_id: u64) {
        self.backup_regions.insert(region_id);
    }

    fn stop(&mut self, region_id: u64) {
        self.backup_regions.remove(&region_id);
    }

    fn is_started(&self, region_id: u64) -> bool {
        self.backup_regions.contains(&region_id)
    }
}

pub struct BackupManager {
    pub dependency: Box<dyn Dependency>,
    pub storage: Box<dyn Storage>,
    current: PathBuf,
    meta: RwLock<Meta>,
    backuping: AtomicBool,
    cluster_id: u64,

    auxiliary: PathBuf,
}

// TODO(backup): change unwrap to ?.
impl BackupManager {
    pub fn new(cluster_id: u64, base: &Path, storage: Box<dyn Storage>) -> Result<BackupManager> {
        info!("create backup manager"; "base" => base.display());

        let current = Path::new(CURRENT_DIR).to_owned();
        // TODO(backup): do not create the dir until start_full_backup.
        if let Err(e) = storage.make_dir(&current) {
            if e.kind() != ErrorKind::AlreadyExists {
                return Err(Error::Io(e));
            }
        }
        let auxiliary = base.join("auxiliary");
        maybe_create_dir(&auxiliary).unwrap();

        let mut backup_meta = BackupMeta::new();
        let meta_path = current.join(BACKUP_META_NAME);
        let mut content = vec![];
        if let Err(e) = storage.read_file(meta_path.as_path(), &mut content) {
            if e.kind() != ErrorKind::NotFound {
                error!("fail to start backup"; "error" => ?e);
                return Err(Error::Io(e));
            }
            // The initial state is Stop.
            backup_meta.set_state(BackupState::Stop);
            info!("new backup");
        } else {
            backup_meta
                .merge_from_bytes(&content)
                .map_err(|e| IoError::new(ErrorKind::InvalidData, format!("{:?}", e)))
                .unwrap();
            info!("continue backup");
        }

        debug!("backup meta"; "meta" => ?backup_meta);
        let meta = Meta {
            backup_meta,
            backup_regions: HashSet::new(),
            buf: vec![],
        };
        let last_number = meta.last_dependency();
        let dependency = Box::new(AtomicU64::new(last_number + 1));
        info!("backup last number"; "last_number" => last_number);
        assert!(
            meta.state() != BackupState::Unknown,
            "{:?}",
            meta.backup_meta
        );
        // TODO: Stop backup by default.
        // let backuping = AtomicBool::new(BackupState::Stop != meta.state());
        let backuping = AtomicBool::new(true);
        Ok(BackupManager {
            dependency,
            storage,
            auxiliary,
            current,
            backuping,
            cluster_id,
            meta: RwLock::new(meta),
        })
    }

    fn on_state_change(&self, new: BackupState) {
        assert!(new != BackupState::Unknown, "{:?}", new);
        self.backuping
            .store(new != BackupState::Stop, Ordering::Release);
    }

    pub fn check_data(&self, region_events: RegionEvents) -> Result<()> {
        check_data(region_events, &self.current, &self.storage)
    }

    pub fn check_meta(&self) -> Result<RegionEvents> {
        check_meta(self.backup_meta())
    }

    pub fn backup_meta(&self) -> BackupMeta {
        self.meta.read().unwrap().backup_meta.clone()
    }

    pub fn tmp_dir(&self, prefix: &str) -> Result<TempDir> {
        let dir = TempDir::new_in(&self.auxiliary, prefix)?;
        Ok(dir)
    }

    pub fn current_dir(&self) -> &Path {
        &self.current
    }

    pub fn region_path(&self, region_id: u64) -> PathBuf {
        region_path(&self.current, region_id)
    }

    pub fn start_backup_region(&self, region_id: u64) -> Result<()> {
        if !self.backuping.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Err(e) = self
            .storage
            .make_dir(&self.current.join(&format!("{}", region_id)))
        {
            if e.kind() != ErrorKind::AlreadyExists {
                return Err(Error::Io(e));
            }
        }
        self.meta.write().unwrap().start(region_id);
        Ok(())
    }

    pub fn stop_backup_region(&self, region_id: u64) -> Result<()> {
        if !self.backuping.load(Ordering::Acquire) {
            return Ok(());
        }
        self.meta.write().unwrap().stop(region_id);
        Ok(())
    }

    pub fn log_path(&self, region_id: u64, first: u64, last: u64) -> PathBuf {
        log_path(&self.current, region_id, first, last)
    }

    pub fn save_logs(&self, region_id: u64, first: u64, last: u64, content: &[u8]) -> Result<()> {
        if !self.backuping.load(Ordering::Acquire) {
            return Ok(());
        }
        if !self.meta.read().unwrap().is_started(region_id) {
            return Ok(());
        }
        let dst = self.log_path(region_id, first, last);
        self.storage.save_file(&dst, content)?;
        Ok(())
    }

    pub fn snapshot_dir(&self, region_id: u64, term: u64, index: u64, dependency: u64) -> PathBuf {
        snapshot_dir(&self.current, region_id, term, index, dependency)
    }

    pub fn save_snapshot(&self, region_id: u64, term: u64, index: u64, src: &Path) -> Result<()> {
        if !self.backuping.load(Ordering::Acquire) {
            return Ok(());
        }
        if !self.meta.read().unwrap().is_started(region_id) {
            return Ok(());
        }
        let dep = self.dependency.alloc_number();
        let dst = self.snapshot_dir(region_id, term, index, dep);
        self.storage.save_dir(&dst, src).unwrap();
        let mut event = BackupEvent::new();
        event.set_region_id(region_id);
        event.set_index(index);
        event.set_dependency(dep);
        event.set_event(BackupEvent_Event::Snapshot);
        self.save_events(vec![event])?;
        Ok(())
    }

    pub fn save_events<I>(&self, events: I) -> Result<()>
    where
        I: IntoIterator<Item = BackupEvent>,
    {
        if !self.backuping.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut meta = self.meta.write().unwrap();
        for event in events {
            meta.backup_meta.mut_events().push(event);
        }
        let meta_path = self.current.join(BACKUP_META_NAME);
        meta.save_to(&meta_path, &self.storage).unwrap();
        Ok(())
    }

    pub fn step(&self, request: BackupState) -> Result<u64> {
        let mut meta = self.meta.write().unwrap();
        let state = meta.step(request)?;
        self.on_state_change(state);
        let dep = self.dependency.alloc_number();
        match state {
            BackupState::Stop => {
                self.backuping.store(false, Ordering::Release);
                meta.backup_regions.clear();
            }
            BackupState::StartFullBackup => {
                if meta.backup_meta.get_start_full_backup_dependency() != 0 {
                    // Rotate the current dir.
                    self.storage
                        .rename_dir(
                            self.current_dir(),
                            Path::new(&format!("{}", meta.last_dependency())),
                        )
                        .unwrap();
                }
                self.storage.make_dir(self.current_dir()).unwrap();
                meta.backup_meta.set_start_full_backup_dependency(dep);
                self.backuping.store(true, Ordering::Release);
            }
            BackupState::FinishFullBackup => {
                meta.backup_meta.set_finish_full_backup_dependency(dep);
                assert!(self.backuping.load(Ordering::Acquire));
            }
            BackupState::IncrementalBackup => {
                meta.backup_meta.mut_inc_backup_dependencies().push(dep);
                assert!(self.backuping.load(Ordering::Acquire));
            }
            BackupState::Unknown => panic!("unexpected state unknown"),
        }
        let meta_path = self.current.join(BACKUP_META_NAME);
        meta.save_to(&meta_path, &self.storage).unwrap();
        Ok(dep)
    }

    pub fn check_cluster_id(&self, cluster_id: u64) -> Result<()> {
        if self.cluster_id != cluster_id {
            Err(Error::ClusterID(self.cluster_id, cluster_id))
        } else {
            Ok(())
        }
    }
}

fn region_path(base: &Path, region_id: u64) -> PathBuf {
    base.join(format!("{}", region_id))
}

fn snapshot_dir(base: &Path, region_id: u64, term: u64, index: u64, dependency: u64) -> PathBuf {
    base.join(format!("{}/{}@{}_{}", region_id, index, term, dependency))
}

fn log_path(base: &Path, region_id: u64, first: u64, last: u64) -> PathBuf {
    base.join(format!("{}/{}_{}", region_id, first, last))
}

type RegionEvents = HashMap<u64, Vec<BackupEvent>>;

fn check_meta(mut meta: BackupMeta) -> Result<RegionEvents> {
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

fn check_data(region_events: RegionEvents, base: &Path, storage: &dyn Storage) -> Result<()> {
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
    fn test_sync_send_backup_mgr() {
        fn t<T: Sync + Send>(_: Option<T>) {}
        t::<BackupManager>(None);
    }

    #[test]
    fn test_meta_step() {
        use std::collections::HashSet;
        let mut meta = Meta {
            backup_meta: Default::default(),
            backup_regions: HashSet::new(),
            buf: vec![],
        };
        let mut correct = HashSet::new();
        correct.insert((BackupState::Stop, BackupState::StartFullBackup));
        correct.insert((BackupState::StartFullBackup, BackupState::FinishFullBackup));
        correct.insert((BackupState::FinishFullBackup, BackupState::StartFullBackup));
        correct.insert((
            BackupState::FinishFullBackup,
            BackupState::IncrementalBackup,
        ));
        correct.insert((
            BackupState::IncrementalBackup,
            BackupState::IncrementalBackup,
        ));
        correct.insert((BackupState::Stop, BackupState::Stop));
        correct.insert((BackupState::StartFullBackup, BackupState::Stop));
        correct.insert((BackupState::FinishFullBackup, BackupState::Stop));
        correct.insert((BackupState::IncrementalBackup, BackupState::Stop));

        let all = vec![
            BackupState::Unknown,
            BackupState::Stop,
            BackupState::StartFullBackup,
            BackupState::FinishFullBackup,
            BackupState::IncrementalBackup,
        ];

        for s1 in all.clone() {
            if s1 == BackupState::Unknown {
                continue;
            }
            for s2 in all.clone() {
                meta.backup_meta.set_state(s1);
                assert!(
                    meta.step(s2).is_ok() == correct.contains(&(s1, s2)),
                    "{:?} should be {}",
                    (s1, s2),
                    correct.contains(&(s1, s2))
                );
            }
        }
    }

    #[test]
    fn test_local_storage() {
        let temp_dir = TempDir::new("test_local_storage").unwrap();
        let path = temp_dir.path();
        let magic_contents = "5678";
        let ls = LocalStorage::new(&path).unwrap();

        // Test tmp_path
        let tp = ls.tmp_path(Path::new("t.sst"));
        assert_eq!(tp.parent().unwrap(), path.join(LOCAL_STORAGE_TMP_DIR));
        assert!(tp.file_name().unwrap().to_str().unwrap().starts_with('t'));
        assert!(tp
            .as_path()
            .extension()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with(LOCAL_STORAGE_TEP_FILE_SUFFIX));

        // Test save_file
        ls.save_file(Path::new("a.log"), magic_contents.as_bytes())
            .unwrap();
        assert_eq!(
            fs::read(path.join("a.log")).unwrap(),
            magic_contents.as_bytes()
        );
        ls.save_file(Path::new("a/a.log"), magic_contents.as_bytes())
            .unwrap_err();
        let list = ls.list_dir(Path::new(LOCAL_STORAGE_TMP_DIR)).unwrap();
        assert!(list.is_empty(), "{:?}", list);

        // Test make_dir
        ls.make_dir(Path::new("z/z")).unwrap();
        ls.make_dir(Path::new("b")).unwrap();
        ls.save_file(Path::new("b/b.log"), magic_contents.as_bytes())
            .unwrap();

        // Test save_dir
        let src = path.join("src");
        make_snap_dir(&src, magic_contents.as_bytes());
        ls.save_dir(Path::new("snap1"), &src).unwrap();
        for e in ls.list_dir(Path::new("snap1")).unwrap() {
            let name = e.file_name().unwrap().to_owned();
            assert!(name == "a.sst" || name == "b.sst");
            let mut buf = vec![];
            ls.read_file(&e, &mut buf).unwrap();
            assert_eq!(buf, magic_contents.as_bytes());
        }
        let list = ls.list_dir(Path::new(LOCAL_STORAGE_TMP_DIR)).unwrap();
        assert!(list.is_empty(), "{:?}", list);
    }

    fn make_snap_dir(path: &Path, contents: &[u8]) {
        fs::create_dir(path).unwrap();
        fs::write(path.join("a.sst"), contents).unwrap();
        fs::write(path.join("b.sst"), contents).unwrap();
    }

    #[test]
    fn test_backup_mgr_step() {
        let temp_dir = TempDir::new("test_backup_mgr").unwrap();
        let path = temp_dir.path();

        let ls = LocalStorage::new(&path).unwrap();
        let bm = BackupManager::new(0, &path, Box::new(ls)).unwrap();

        let check_file_count = |count| {
            let files = bm.storage.list_dir(Path::new("")).unwrap();
            assert_eq!(count, files.len(), "{:?}", files);
            files.len()
        };
        // For now, current dir is created during initializing backup manager.
        let len = check_file_count(1 /* current */ + 1 /* auxiliary */ + 1 /* localtmp */);

        bm.step(BackupState::Stop).unwrap();
        let len = check_file_count(len);
        assert!(!bm.backuping.load(Ordering::Acquire));

        // Do not rotate if it is the first backup.
        bm.step(BackupState::StartFullBackup).unwrap();
        let len = check_file_count(len);
        assert!(bm.backuping.load(Ordering::Acquire));

        bm.step(BackupState::FinishFullBackup).unwrap();
        let len = check_file_count(len);
        assert!(bm.backuping.load(Ordering::Acquire));

        bm.step(BackupState::IncrementalBackup).unwrap();
        let len = check_file_count(len);
        assert!(bm.backuping.load(Ordering::Acquire));

        // Test rotate.
        bm.step(BackupState::Stop).unwrap();
        let len = check_file_count(len);
        assert!(!bm.backuping.load(Ordering::Acquire));

        bm.step(BackupState::StartFullBackup).unwrap();
        check_file_count(len + 1);
        assert!(bm.backuping.load(Ordering::Acquire));
    }

    #[test]
    fn test_backup_mgr() {
        let temp_dir = TempDir::new("test_backup_mgr").unwrap();
        let path = temp_dir.path();
        // let path = Path::new("test_backup_mgr");
        // fs::create_dir(path).unwrap();

        let ls = LocalStorage::new(&path).unwrap();
        let bm = BackupManager::new(0, &path, Box::new(ls)).unwrap();
        assert_eq!(bm.dependency.get(), 1);
        assert_eq!(bm.dependency.alloc_number(), 1);
        assert_eq!(bm.dependency.alloc_number(), 2);

        bm.step(BackupState::StartFullBackup).unwrap();

        let src = path.join("src");
        bm.start_backup_region(1).unwrap();
        assert!(bm.meta.read().unwrap().backup_regions.contains(&1));
        // Test if it ignores ErrorKind::AlreadyExists.
        bm.start_backup_region(1).unwrap();

        let magic_contents = b"a";
        make_snap_dir(&src, magic_contents);
        bm.save_snapshot(1, 2, 3, &src).unwrap();

        fn check_meta(bm: &BackupManager, dependency: u64, contents: &[u8]) {
            let mut buf = vec![];
            bm.storage
                .read_file(&bm.current.join(BACKUP_META_NAME), &mut buf)
                .unwrap();
            let mut meta = BackupMeta::new();
            meta.merge_from_bytes(&buf).unwrap();
            let events = meta.get_events();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].get_event(), BackupEvent_Event::Snapshot);
            assert_eq!(events[0].get_region_id(), 1);
            assert_eq!(events[0].get_index(), 3);
            assert_eq!(events[0].get_dependency(), dependency);

            let snap_dir = bm.snapshot_dir(1, 2, 3, dependency);
            let mut buf = vec![];
            bm.storage
                .read_file(&snap_dir.join("a.sst"), &mut buf)
                .unwrap();
            assert_eq!(buf, contents);
            buf.clear();
            bm.storage
                .read_file(&snap_dir.join("b.sst"), &mut buf)
                .unwrap();
            assert_eq!(buf, contents);
        }
        check_meta(&bm, bm.dependency.get() - 1, magic_contents);

        // After stop, it should not save further data.
        let mut buf = vec![];
        bm.stop_backup_region(1).unwrap();
        bm.save_logs(1, 101, 102, magic_contents).unwrap();
        bm.storage
            .read_file(&bm.log_path(1, 101, 102), &mut buf)
            .unwrap_err();
        drop(bm);

        // Restart BackupManager.
        let ls = LocalStorage::new(&path).unwrap();
        let bm = BackupManager::new(0, &path, Box::new(ls)).unwrap();
        check_meta(&bm, bm.dependency.get() - 1, magic_contents);

        buf.clear();
        bm.start_backup_region(1).unwrap();
        bm.save_logs(1, 10, 100, magic_contents).unwrap();
        bm.storage
            .read_file(&bm.log_path(1, 10, 100), &mut buf)
            .unwrap();
        assert_eq!(buf, magic_contents);
    }

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
                storage
                    .make_dir(&snapshot_dir(base, 1, s.1, s.0, 0))
                    .unwrap();
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
