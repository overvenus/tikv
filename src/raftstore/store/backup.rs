use std::fs::{self, File};
use std::io::{Error, ErrorKind, Read, Result, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use kvproto::backup::{BackupEvent, BackupEvent_Event, BackupMeta};
use protobuf::Message;
use rand::Rng;
use tempdir::TempDir;

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

fn maybe_create_dir(path: &Path) -> Result<()> {
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

pub trait Storage: Sync + Send {
    fn make_dir(&self, path: &Path) -> Result<()>;
    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;
    fn save_dir(&self, path: &Path, src: &Path) -> Result<()>;
    fn save_file(&self, path: &Path, content: &[u8]) -> Result<()>;
    fn read_file(&self, path: &Path, buf: &mut Vec<u8>) -> Result<()>;
}

#[derive(Clone)]
pub struct LocalStorage {
    base: PathBuf,
    tmp: PathBuf,
}

impl LocalStorage {
    pub fn new(base: &Path) -> Result<LocalStorage> {
        info!("create local storage"; "base" => base.display());
        let tmp = base.join(LOCAL_STORAGE_TMP_DIR);
        maybe_create_dir(&tmp)?;
        Ok(LocalStorage {
            base: base.to_owned(),
            tmp,
        })
    }

    // TODO: gc tmp files.
    fn tmp_path(&self, path: &Path) -> PathBuf {
        let uid: u64 = rand::thread_rng().gen();
        let tmp_suffix = format!("{}{:016x}", LOCAL_STORAGE_TEP_FILE_SUFFIX, uid);
        self.tmp.join(path).with_extension(tmp_suffix)
    }
}

impl Storage for LocalStorage {
    fn make_dir(&self, path: &Path) -> Result<()> {
        let path = self.base.join(path);
        fs::create_dir_all(path)
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
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

    fn save_dir(&self, path: &Path, src: &Path) -> Result<()> {
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

    fn save_file(&self, path: &Path, content: &[u8]) -> Result<()> {
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

    fn read_file(&self, path: &Path, buf: &mut Vec<u8>) -> Result<()> {
        let mut file = File::open(self.base.join(path))?;
        file.read_to_end(buf).map(|_| ())
    }
}

struct Meta {
    backup_meta: BackupMeta,
    buf: Vec<u8>,
}

pub struct BackupManager {
    pub dependency: Box<dyn Dependency>,
    pub storage: Box<dyn Storage>,
    current: PathBuf,
    meta: Mutex<Meta>,

    auxiliary: PathBuf,
}

impl BackupManager {
    pub fn new(base: &Path, storage: Box<dyn Storage>) -> Result<BackupManager> {
        info!("create backup manager"; "base" => base.display());

        let current = Path::new(CURRENT_DIR).to_owned();
        if let Err(e) = storage.make_dir(&current) {
            if e.kind() != ErrorKind::AlreadyExists {
                return Err(e);
            }
        }
        let auxiliary = base.join("auxiliary");
        maybe_create_dir(&auxiliary).unwrap();

        let meta_path = current.join(BACKUP_META_NAME);
        let mut content = vec![];
        if let Err(e) = storage.read_file(meta_path.as_path(), &mut content) {
            if e.kind() != ErrorKind::NotFound {
                error!("fail to start backup"; "error" => ?e);
                return Err(e);
            }
            info!("new backup");
        } else {
            info!("continue backup");
        }

        let mut backup_meta = BackupMeta::new();
        backup_meta
            .merge_from_bytes(&content)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("{:?}", e)))
            .unwrap();
        let events = backup_meta.mut_events();
        events.sort_by(|l, r| l.get_dependency().cmp(&r.get_dependency()));
        let last_number = events.last().map_or(0, |e| e.get_dependency());
        let dependency = Box::new(AtomicU64::new(last_number + 1));
        let buf = vec![];
        info!("backup last number"; "last_number" => last_number);
        debug!("backup meta"; "meta" => ?backup_meta);
        Ok(BackupManager {
            dependency,
            storage,
            auxiliary,
            current,
            meta: Mutex::new(Meta { backup_meta, buf }),
        })
    }

    pub fn check_meta(&self) -> Result<()> {
        check_meta(self.backup_meta())
    }

    pub fn backup_meta(&self) -> BackupMeta {
        self.meta.lock().unwrap().backup_meta.clone()
    }

    pub fn tmp_dir(&self, prefix: &str) -> Result<TempDir> {
        TempDir::new_in(&self.auxiliary, prefix)
    }

    pub fn current_dir(&self) -> &Path {
        &self.current
    }

    pub fn region_path(&self, region_id: u64) -> PathBuf {
        self.current.join(format!("{}", region_id))
    }

    pub fn start_backup_region(&self, region_id: u64) -> Result<()> {
        if let Err(e) = self
            .storage
            .make_dir(&self.current.join(&format!("{}", region_id)))
        {
            if e.kind() != ErrorKind::AlreadyExists {
                return Err(e);
            }
        }
        Ok(())
    }

    pub fn log_path(&self, region_id: u64, first: u64, last: u64) -> PathBuf {
        self.current
            .join(format!("{}/{}_{}", region_id, first, last))
    }

    pub fn save_logs(&self, region_id: u64, first: u64, last: u64, content: &[u8]) -> Result<()> {
        let dst = self.log_path(region_id, first, last);
        self.storage.save_file(&dst, content)
    }

    pub fn snapshot_dir(&self, region_id: u64, term: u64, index: u64, dependency: u64) -> PathBuf {
        self.current
            .join(format!("{}/{}@{}_{}", region_id, index, term, dependency))
    }

    pub fn save_snapshot(&self, region_id: u64, term: u64, index: u64, src: &Path) -> Result<()> {
        let dep = self.dependency.alloc_number();
        let dst = self.snapshot_dir(region_id, term, index, dep);
        self.storage.save_dir(&dst, src).unwrap();
        let mut event = BackupEvent::new();
        event.set_region_id(region_id);
        event.set_index(index);
        event.set_dependency(dep);
        event.set_event(BackupEvent_Event::Snapshot);
        self.save_events(vec![event])
    }

    pub fn save_events<I>(&self, events: I) -> Result<()>
    where
        I: IntoIterator<Item = BackupEvent>,
    {
        let mut meta = self.meta.lock().unwrap();
        for event in events {
            meta.backup_meta.mut_events().push(event);
        }
        let meta: &mut Meta = &mut *meta;
        let backup_meta = &mut meta.backup_meta;
        let buf = &mut meta.buf;
        buf.clear();
        backup_meta.write_to_vec(buf).unwrap();
        let meta_path = self.current.join(BACKUP_META_NAME);
        self.storage.save_file(&meta_path, &meta.buf).unwrap();
        Ok(())
    }
}

fn check_meta(mut meta: BackupMeta) -> Result<()> {
    use std::collections::hash_map::{Entry, HashMap};

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
    //   3. TODO: consider merge.
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
            if let BackupEvent_Event::Split = cur.get_event() {
                for id in cur.get_related_region_ids() {
                    if id == region_id {
                        // Do not check self.
                        // TODO: must not include self.
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
            // TODO: Check other events.
        }
    }
    if err.is_empty() {
        Ok(())
    } else {
        Err(Error::new(ErrorKind::Other, err))
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
    fn test_backup_mgr() {
        let temp_dir = TempDir::new("test_backup_mgr").unwrap();
        let path = temp_dir.path();
        // let path = Path::new("test_backup_mgr");
        // fs::create_dir(path).unwrap();

        let ls = LocalStorage::new(&path).unwrap();
        let bm = BackupManager::new(&path, Box::new(ls)).unwrap();
        assert_eq!(bm.dependency.get(), 1);
        assert_eq!(bm.dependency.alloc_number(), 1);
        assert_eq!(bm.dependency.alloc_number(), 2);

        let src = path.join("src");
        bm.start_backup_region(1).unwrap();
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

        drop(bm);
        let ls = LocalStorage::new(&path).unwrap();
        let bm = BackupManager::new(&path, Box::new(ls)).unwrap();
        check_meta(&bm, bm.dependency.get() - 1, magic_contents);

        bm.save_logs(1, 10, 100, magic_contents).unwrap();
        let mut buf = vec![];
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
    }
}
