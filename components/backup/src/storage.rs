// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::log_storage::{LogStorage, DEFAULT_FILE_CAPACITY};
use std::fs::{self, File};
use std::io::{Read, Result as IoResult, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rand::Rng;

use super::maybe_create_dir;
use kvproto::backup::EntryBatch;

const LOCAL_STORAGE_TMP_DIR: &str = "localtmp";
const LOCAL_STORAGE_TEP_FILE_SUFFIX: &str = "tmp";

// TODO(backup): Simplify the trait.
pub trait Storage: Sync + Send + 'static {
    fn rename_dir(&self, from: &Path, to: &Path) -> IoResult<()>;
    fn make_dir(&self, path: &Path) -> IoResult<()>;
    fn list_dir(&self, path: &Path) -> IoResult<Vec<PathBuf>>;
    fn save_dir(&self, path: &Path, src: &Path) -> IoResult<()>;
    fn save_file(&self, path: &Path, content: &[u8]) -> IoResult<()>;
    fn read_file(&self, path: &Path, buf: &mut Vec<u8>) -> IoResult<()>;
    fn put(&self, batch: &mut EntryBatch) -> bool;
    fn sync(&self) -> IoResult<()>;
}

impl Storage for Arc<dyn Storage> {
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
    fn put(&self, batch: &mut EntryBatch) -> bool {
        (**self).put(batch)
    }
    fn sync(&self) -> IoResult<()> {
        (**self).sync()
    }
}

#[derive(Clone)]
pub struct LocalStorage {
    base: PathBuf,
    tmp: PathBuf,
    log: Arc<LogStorage>,
}

impl LocalStorage {
    pub fn new(base: &Path) -> IoResult<LocalStorage> {
        info!("create local storage"; "base" => base.display());
        let tmp = base.join(LOCAL_STORAGE_TMP_DIR);
        let log = LogStorage::open(base.to_str().unwrap(), DEFAULT_FILE_CAPACITY)?;
        maybe_create_dir(&tmp)?;
        Ok(LocalStorage {
            base: base.to_owned(),
            log: Arc::new(log),
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

    fn put(&self, batch: &mut EntryBatch) -> bool {
        self.log.put(batch)
    }
    fn sync(&self) -> IoResult<()> {
        self.log.sync()
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use crate::tests::make_snap_dir;

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
}
