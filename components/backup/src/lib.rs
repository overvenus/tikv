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

mod check;
mod errors;
mod restore;
mod storage;

use std::collections::hash_map::HashMap;
use std::collections::HashSet;
use std::fs;
use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;

use kvproto::backup::{BackupEvent, BackupEvent_Event, BackupMeta, BackupState};
use protobuf::Message;
use tempdir::TempDir;

pub use errors::{Error, Result};
pub use restore::{dot, RestoreManager};
pub use storage::{LocalStorage, Storage};

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
            | (BackupState::Stop, BackupState::Start)
            | (BackupState::Start, BackupState::Complete)
            | (BackupState::Complete, BackupState::Start)
            | (BackupState::Complete, BackupState::Incremental)
            | (BackupState::Incremental, BackupState::Incremental)
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
        let backuping = AtomicBool::new(BackupState::Stop != meta.state());
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
        check::check_data(region_events, &self.current, &self.storage)
    }

    pub fn check_meta(&self) -> Result<RegionEvents> {
        check::check_meta(self.backup_meta())
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

    pub fn is_region_started(&self, region_id: u64) -> bool {
        self.meta.read().unwrap().is_started(region_id)
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
        let meta_path = self.current.join(BACKUP_META_NAME);
        match state {
            BackupState::Stop => {
                self.backuping.store(false, Ordering::Release);
                meta.backup_regions.clear();
                if meta.backup_meta.get_start_dependency() != 0 {
                    meta.save_to(&meta_path, &self.storage).unwrap();
                    // Rotate the current dir.
                    self.rotate_current_dir(dep);
                }
                info!("stop backup");
                return Ok(dep);
            }
            BackupState::Start => {
                info!("start full backup");
                if let Err(e) = self.storage.make_dir(self.current_dir()) {
                    if e.kind() != ErrorKind::AlreadyExists {
                        return Err(Error::Io(e));
                    }
                }
                meta.backup_meta.set_start_dependency(dep);
                self.backuping.store(true, Ordering::Release);
            }
            BackupState::Complete => {
                meta.backup_meta.set_complete_dependency(dep);
                assert!(self.backuping.load(Ordering::Acquire));
                info!("complete full backup");
            }
            BackupState::Incremental => {
                meta.backup_meta.mut_incremental_dependencies().push(dep);
                assert!(self.backuping.load(Ordering::Acquire));
                info!("complete incremental backup");
            }
            BackupState::Unknown => panic!("unexpected state unknown"),
        }
        meta.save_to(&meta_path, &self.storage).unwrap();
        Ok(dep)
    }

    fn rotate_current_dir(&self, rotate_to: u64) {
        self.storage
            .rename_dir(self.current_dir(), Path::new(&format!("{}", rotate_to)))
            .unwrap();
        info!("rotate current dir"; "rotate_to" => rotate_to);
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

pub type RegionEvents = HashMap<u64, Vec<BackupEvent>>;

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
        correct.insert((BackupState::Stop, BackupState::Start));
        correct.insert((BackupState::Start, BackupState::Complete));
        correct.insert((BackupState::Complete, BackupState::Start));
        correct.insert((BackupState::Complete, BackupState::Incremental));
        correct.insert((BackupState::Incremental, BackupState::Incremental));
        correct.insert((BackupState::Stop, BackupState::Stop));
        correct.insert((BackupState::Start, BackupState::Stop));
        correct.insert((BackupState::Complete, BackupState::Stop));
        correct.insert((BackupState::Incremental, BackupState::Stop));

        let all = vec![
            BackupState::Unknown,
            BackupState::Stop,
            BackupState::Start,
            BackupState::Complete,
            BackupState::Incremental,
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

    pub fn make_snap_dir(path: &Path, contents: &[u8]) {
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
        // The current dir will not be created until starting full backup.
        let len = check_file_count(1 /* auxiliary */ + 1 /* localtmp */);

        bm.step(BackupState::Stop).unwrap();
        let len = check_file_count(len);
        assert!(!bm.backuping.load(Ordering::Acquire));

        // Do not rotate if it is the first backup.
        bm.step(BackupState::Start).unwrap();
        let len = check_file_count(len + 1 /* current */);
        assert!(bm.backuping.load(Ordering::Acquire));

        bm.step(BackupState::Complete).unwrap();
        let len = check_file_count(len);
        assert!(bm.backuping.load(Ordering::Acquire));

        bm.step(BackupState::Incremental).unwrap();
        let len = check_file_count(len);
        assert!(bm.backuping.load(Ordering::Acquire));

        // Test rotate.
        bm.step(BackupState::Stop).unwrap();
        let len = check_file_count(len);
        assert!(!bm.backuping.load(Ordering::Acquire));

        bm.step(BackupState::Start).unwrap();
        check_file_count(len + 1 /* current */);
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

        bm.step(BackupState::Start).unwrap();

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
}
