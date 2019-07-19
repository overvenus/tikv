use super::file_util::{close, deserialize, pread, serialize, sync, write, BatchEntryIterator};
use super::CURRENT_DIR;
use errno;
use kvproto::backup::{EntryBatch, FileMeta, RegionMeta};
use libc;
use protobuf::Message;
use raft::eraftpb::Entry as RaftEntry;
use std::cmp;
use std::collections::HashMap;
use std::ffi::CString;
use std::fs;
use std::io::{Error as IoError, ErrorKind, Result as IoResult, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use std::thread::sleep;
use std::time::Duration;
use std::u64;

#[cfg(target_os = "linux")]
const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

#[cfg(target_os = "linux")]
const NEW_FILE_MODE: libc::mode_t = libc::S_IRUSR | libc::S_IWUSR;

#[cfg(not(target_os = "linux"))]
const NEW_FILE_MODE: libc::c_uint = (libc::S_IRUSR | libc::S_IWUSR) as libc::c_uint;

const LOG_SUFFIX: &str = ".raftlog";
const LOG_SUFFIX_LEN: usize = 8;
const FILE_NUM_LEN: usize = 16;
const FILE_NAME_LEN: usize = FILE_NUM_LEN + LOG_SUFFIX_LEN;
// pub const FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";
// pub const VERSION: &[u8] = b"v1.0.0";
pub const DEFAULT_FILE_CAPACITY: usize = 512 * 1024 * 1024;
pub const MAX_WRITE_BUFFER_SIZE: usize = 256 * 1024 * 1024;

const MAGIC_STR: &[u8] = b"haha";

pub struct FileMetaIndex {
    pub file_name: String,
    pub fd: libc::c_int,
    pub meta: HashMap<u64, RegionMeta>,
    pub size: usize,
}

impl FileMetaIndex {
    pub fn to_file_meta(&self) -> FileMeta {
        let mut meta = FileMeta::new();
        meta.path = self.file_name.clone();
        meta.crc32 = 0;
        meta.content_size = self.size as u64;
        for (_, m) in self.meta.iter() {
            meta.meta.push(m.clone());
        }
        return meta;
    }

    pub fn create(fd: libc::c_int, active_prefix: u64) -> FileMetaIndex {
        FileMetaIndex {
            fd,
            file_name: generate_file_name(active_prefix),
            size: 0,
            meta: HashMap::new(),
        }
    }

    pub fn new(fd: libc::c_int, meta: FileMeta) -> FileMetaIndex {
        let mut regions = HashMap::default();
        for region_meta in meta.meta.iter() {
            regions.insert(region_meta.region_id, region_meta.clone());
        }
        FileMetaIndex {
            file_name: meta.path,
            fd,
            meta: regions,
            size: meta.content_size as usize,
        }
    }

    pub fn read_meta(
        fd: libc::c_int,
        path: &PathBuf,
        mode: libc::c_int,
    ) -> IoResult<FileMetaIndex> {
        if mode == libc::O_RDONLY {
            Self::get_meta(fd, path)
        } else {
            let meta_correct = Self::check_meta(fd, path);
            if meta_correct {
                Self::get_meta(fd, path)
            } else {
                Self::rewrite_meta(fd, path)
            }
        }
    }

    fn rewrite_meta(fd: libc::c_int, path: &PathBuf) -> IoResult<FileMetaIndex> {
        let file_meta = fs::metadata(path)?;
        let file_size = file_meta.len() as usize;
        let mut iter = BatchEntryIterator::new(vec![(fd, file_size)]);
        let mut meta = FileMeta::new();
        let mut region_meta: HashMap<u64, RegionMeta> = HashMap::new();
        meta.path = path.file_name().unwrap().to_str().unwrap().to_string();
        meta.content_size = 0;
        // TODO: calculate src value
        meta.crc32 = 0;
        iter.seek_to_first();
        while iter.valid() {
            let mut batch = iter.get().unwrap();
            update_raft_meta(&mut region_meta, &mut batch);
            iter.next();
            meta.content_size = iter.get_offset() as u64;
        }
        for m in region_meta.iter() {
            meta.meta.push(m.1.clone());
        }
        let file_meta_index = FileMetaIndex {
            meta: region_meta,
            size: meta.content_size as usize,
            file_name: meta.path.clone(),
            fd: fd,
        };
        //seek_to_end(fd);
        write_file_meta(fd, meta);
        Ok(file_meta_index)
    }

    fn get_meta(fd: libc::c_int, path: &PathBuf) -> IoResult<FileMetaIndex> {
        let file_meta = fs::metadata(path)?;
        let file_size = file_meta.len();
        let mut buf: Vec<u8> = Vec::with_capacity(4);
        if !pread(fd, &mut buf, (file_size - 8) as u64, 4) {
            return Err(IoError::new(ErrorKind::Other, "get meta size failed"));
        }
        let meta_size = deserialize(buf.as_slice()) as u64;
        if meta_size + 8 >= file_size {
            return Err(IoError::new(ErrorKind::Other, "get meta size incorrect"));
        }
        let mut result_buf = Vec::with_capacity(meta_size as usize);
        if !pread(
            fd,
            &mut result_buf,
            file_size - 8 - meta_size,
            meta_size as u64,
        ) {
            return Err(IoError::new(ErrorKind::Other, "read meta failed"));
        }
        let mut meta = FileMeta::new();
        if meta.merge_from_bytes(result_buf.as_slice()).is_err() {
            return Err(IoError::new(ErrorKind::Other, "deserialize meta failed"));
        }
        return Ok(FileMetaIndex::new(fd, meta));
    }

    fn check_meta(fd: libc::c_int, path: &PathBuf) -> bool {
        let file_size = match fs::metadata(path) {
            Ok(file_meta) => file_meta.len(),
            Err(_) => {
                return false;
            }
        };
        if file_size < 16 {
            return false;
        }
        let mut buf: Vec<u8> = Vec::with_capacity(4);
        if !pread(fd, &mut buf, (file_size - 4) as u64, 4) {
            return false;
        }
        if buf.as_slice() != MAGIC_STR {
            return false;
        }
        if pread(fd, &mut buf, file_size - 8, 4) {
            return false;
        }
        let meta_size = deserialize(buf.as_slice()) as u64;
        if meta_size + 8 >= file_size {
            return false;
        }
        if pread(fd, &mut buf, file_size - 12 - meta_size, 4) {
            return false;
        }
        buf.as_slice() == MAGIC_STR
    }

    fn has_region_entry(&self, region_id: u64, start_index: u64, end_index: u64) -> bool {
        if let Some(meta) = self.meta.get(&region_id) {
            if meta.start_index <= end_index && meta.end_index >= start_index {
                return true;
            }
        }
        return false;
    }

    pub fn merge_region_meta(
        region_indexes: &mut HashMap<u64, RegionMeta>,
        region_metas: &HashMap<u64, RegionMeta>,
    ) {
        for (region_id, meta) in region_metas.iter() {
            match region_indexes.get_mut(&region_id) {
                Some(all_meta) => {
                    if meta.start_index <= all_meta.end_index + 1
                        && all_meta.end_index < meta.end_index
                    {
                        all_meta.end_index = meta.end_index;
                    }
                }
                None => {
                    region_indexes.insert(region_id.clone(), meta.clone());
                }
            }
        }
    }
}

pub struct LogManager {
    pub active_log_capacity: usize,
    pub active_file: FileMetaIndex,
    pub all_files: Vec<FileMetaIndex>,
    pub region_indexes: HashMap<u64, RegionMeta>,
    dir: PathBuf,
    pub write_buffer: Option<Vec<u8>>,
    pub sync_buffer: Option<Vec<u8>>,
}

impl LogManager {
    pub fn new(
        active_file: FileMetaIndex,
        active_log_capacity: usize,
        dir: PathBuf,
        all_files: Vec<FileMetaIndex>,
    ) -> LogManager {
        let mut region_indexes = HashMap::new();
        for f in all_files.iter() {
            FileMetaIndex::merge_region_meta(&mut region_indexes, &f.meta);
        }
        LogManager {
            active_log_capacity,
            all_files,
            active_file,
            dir,
            region_indexes,
            write_buffer: Some(Vec::default()),
            sync_buffer: Some(Vec::default()),
        }
    }

    pub fn open(
        path: &Path,
        filenames: Vec<(u64, String)>,
        active_prefix: u64,
        active_log_capacity: usize,
    ) -> IoResult<LogManager> {
        let mut all_files = Vec::new();
        for (file_prefix, file_name) in filenames.iter() {
            let file_path = path.join(file_name);
            let c_path = CString::new(file_path.to_str().unwrap().as_bytes()).unwrap();
            let mode = if file_prefix + 1 == active_prefix {
                // The last log file maybe have not written meta into disk. So we must build it meta.
                libc::O_RDWR | libc::O_APPEND
            } else {
                libc::O_RDONLY
            };
            let fd = unsafe { libc::open(c_path.as_ptr(), mode) };
            if fd < 0 {
                panic!("open file failed, err {}", errno::errno().to_string());
            }
            let file_meta = FileMetaIndex::read_meta(fd, &file_path, mode)?;
            all_files.push(file_meta);
        }
        let new_fd = new_log_file(path.to_path_buf(), active_prefix);
        let active_file = FileMetaIndex::create(new_fd, active_prefix);
        Ok(LogManager::new(
            active_file,
            active_log_capacity,
            path.to_path_buf(),
            all_files,
        ))
    }

    pub fn dump(&mut self) {
        let write_buffer = self.write_buffer.as_mut().unwrap();
        if !write_buffer.is_empty() {
            write(self.active_file.fd, write_buffer);
            self.active_file.size += write_buffer.len();
            write_buffer.clear();
        }
        let prefix = extract_file_num(self.active_file.file_name.as_str()).unwrap() + 1;
        let mut meta = self.active_file.to_file_meta();
        meta.content_size = self.active_file.size as u64;
        write_file_meta(self.active_file.fd, meta);
        let new_fd = new_log_file(self.dir.clone(), prefix);
        let new_file = FileMetaIndex::create(new_fd, prefix);
        let old_file = std::mem::replace(&mut self.active_file, new_file);
        self.all_files.push(old_file);
    }

    pub fn write_into_buffer(&mut self, batch: &mut EntryBatch) -> bool {
        let mut write_buffer = self.write_buffer.as_mut().unwrap();
        if write_buffer.len() > MAX_WRITE_BUFFER_SIZE {
            return false;
        }
        let buf = serialize(batch.compute_size());
        write_buffer.write_all(buf.as_ref()).unwrap();
        batch.write_to_vec(&mut write_buffer).unwrap();
        return true;
    }
}

impl Drop for LogManager {
    fn drop(&mut self) {
        for f in self.all_files.iter() {
            close(f.fd);
        }
    }
}

pub struct LogStorage {
    log_manager: RwLock<LogManager>,
    // dir: String,
    lock: Mutex<()>,
}

impl LogStorage {
    pub fn new(log_manager: LogManager) -> LogStorage {
        LogStorage {
            log_manager: RwLock::new(log_manager),
            lock: Mutex::new(())
            // dir: dir.to_string(),
        }
    }

    pub fn open(dir: &str, active_log_capacity: usize) -> IoResult<LogStorage> {
        let root = Path::new(dir);
        let path = root.join(CURRENT_DIR);
        if !path.exists() {
            if !root.exists() {
                info!("Create raft log directory: {}", root.display());
                fs::create_dir(root)
                    .unwrap_or_else(|e| panic!("Create raft log directory failed, err: {:?}", e));
            }
            info!("Create raft log directory: {}", path.display());
            fs::create_dir(path.clone())
                .unwrap_or_else(|e| panic!("Create raft log directory failed, err: {:?}", e));
        }

        if !path.is_dir() {
            return Err(IoError::new(ErrorKind::Other, "Not directory."));
        }
        let mut min_file_num: u64 = u64::MAX;
        let mut max_file_num: u64 = 0;
        let mut log_files = vec![];

        for entry in fs::read_dir(path.clone())? {
            let entry = entry?;
            let file_path = entry.path();
            if !file_path.is_file() {
                continue;
            }

            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            if file_name.ends_with(LOG_SUFFIX) && file_name.len() == FILE_NAME_LEN {
                let file_num = match extract_file_num(file_name) {
                    Ok(num) => num,
                    Err(_) => {
                        continue;
                    }
                };
                min_file_num = cmp::min(min_file_num, file_num);
                max_file_num = cmp::max(max_file_num, file_num);
                log_files.push((file_num, file_name.to_string()));
            }
        }
        let active_prefix = if log_files.is_empty() {
            0 as u64
        } else {
            max_file_num + 1
        };
        log_files.sort_by_key(|a| a.0);
        let log_manager = LogManager::open(
            path.as_path(),
            log_files,
            active_prefix,
            active_log_capacity,
        )?;
        Ok(LogStorage::new(log_manager))
    }

    pub fn iter(&self) -> BatchEntryIterator {
        let mut fds = Vec::new();
        {
            let log = self.log_manager.read().unwrap();
            for f in &log.all_files {
                fds.push((f.fd, f.size));
            }
            fds.push((log.active_file.fd, log.active_file.size));
        }
        BatchEntryIterator::new(fds)
    }

    pub fn iterator_with_region(&self, region_id: u64) -> BatchEntryIterator {
        let mut fds = Vec::new();
        {
            let log = self.log_manager.read().unwrap();
            for f in &log.all_files {
                if f.meta.contains_key(&region_id) {
                    fds.push((f.fd, f.size));
                }
            }
            if log.active_file.meta.contains_key(&region_id) {
                fds.push((log.active_file.fd, log.active_file.size));
            }
        }
        BatchEntryIterator::new(fds)
    }

    pub fn read(&self, region_id: u64, start_index: u64, end_index: u64) -> Vec<RaftEntry> {
        let mut fds = Vec::new();
        let mut result = Vec::new();
        {
            let log = self.log_manager.read().unwrap();
            for f in &log.all_files {
                if f.has_region_entry(region_id, start_index, end_index) {
                    fds.push((f.fd, f.size));
                }
            }
            if log
                .active_file
                .has_region_entry(region_id, start_index, end_index)
            {
                fds.push((log.active_file.fd, log.active_file.size));
            }
        }
        let mut iter = BatchEntryIterator::new(fds);
        iter.seek_to_first();
        while iter.valid() {
            let record = iter.get_ref().unwrap();
            if record.region_id == region_id {
                for e in record.entries.iter() {
                    if e.index >= start_index && e.index <= end_index {
                        result.push(e.clone());
                    }
                }
            }
            iter.next();
        }
        result
    }

    pub fn put(&self, batch: &mut EntryBatch) -> bool {
        if batch.entries.is_empty() {
            return true;
        }
        let mut first = true;
        loop {
            {
                let mut log_manager = self.log_manager.write().unwrap();
                if first {
                    if !update_raft_meta(&mut log_manager.region_indexes, batch)
                        || !update_raft_meta(&mut log_manager.active_file.meta, batch)
                    {
                        info!("put batch failed"; "batch_region_id" => batch.region_id);
                        return false;
                    }
                    first = false;
                }
                if log_manager.write_into_buffer(batch) {
                    return true;
                }
            }
            info!("write into buffer failed, try sync");
            if !self.sync() {
                info!("sync failed");
                return false;
            }
        }
        return true;
    }

    pub fn sync(&self) -> bool {
        let _guard = self.lock.lock().unwrap();
        let (mut sync_task, fd) = {
            let mut log = self.log_manager.write().unwrap();
            assert!(log.write_buffer.is_some());
            if let Some(sync_task) = log.write_buffer.as_ref() {
                if sync_task.is_empty() {
                    return true;
                }
            }
            let task = log.write_buffer.take();
            log.write_buffer = log.sync_buffer.take();
            (task, log.active_file.fd)
        };
        let write_buffer = sync_task.as_mut().unwrap();
        if !write(fd, write_buffer) {
            error!("flush buffer to file failed");
            return false;
        }
        sync(fd);
        let mut log = self.log_manager.write().unwrap();
        log.active_file.size += write_buffer.len();
        write_buffer.clear();
        log.sync_buffer = sync_task;
        if log.active_file.size >= log.active_log_capacity {
            info!("close current active log and write into file meta"; "file" => log.active_file.file_name.clone());
            log.dump();
        }
        true
    }

    fn dump(&self) {
        let mut log = self.log_manager.write().unwrap();
        log.dump();
    }

    fn file_num(&self) -> usize {
        let log = self.log_manager.read().unwrap();
        return log.all_files.len();
    }

    fn size(&self) -> usize {
        let log = self.log_manager.read().unwrap();
        return log.active_file.size;
    }
}

fn extract_file_num(file_name: &str) -> IoResult<u64> {
    match file_name[..FILE_NUM_LEN].parse::<u64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(IoError::new(ErrorKind::Other, "error file prefix")),
    }
}

fn new_log_file(dir: PathBuf, file_num: u64) -> libc::c_int {
    let mut path = dir.clone();
    path.push(generate_file_name(file_num));
    let path_cstr = CString::new(path.as_path().to_str().unwrap().as_bytes()).unwrap();
    let fd = unsafe {
        libc::open(
            path_cstr.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_APPEND,
            NEW_FILE_MODE,
        )
    };
    if fd < 0 {
        panic!("Open file failed, err {}", errno::errno().to_string());
    }
    fd
}

fn generate_file_name(file_num: u64) -> String {
    format!("{:016}{}", file_num, LOG_SUFFIX)
}

fn write_file_meta(fd: libc::c_int, meta: FileMeta) {
    let meta_len = meta.compute_size();
    let mut meta_buf = Vec::with_capacity(meta_len as usize + 256);
    meta_buf.extend_from_slice(MAGIC_STR);
    meta.write_to_vec(&mut meta_buf).unwrap();
    let buf = serialize(meta_len);
    meta_buf.extend_from_slice(buf.as_ref());
    meta_buf.extend_from_slice(MAGIC_STR);
    write(fd, &mut meta_buf);
    sync(fd);
}

fn update_raft_meta(meta: &mut HashMap<u64, RegionMeta>, batch: &mut EntryBatch) -> bool {
    match meta.get_mut(&batch.region_id) {
        Some(region_meta) => {
            if region_meta.end_index + 1 < batch.entries.first().unwrap().index {
                batch.mut_entries().clear();
                return false;
            }
            while !batch.entries.is_empty()
                && batch.entries.first().unwrap().index <= region_meta.end_index
            {
                batch.entries.remove(0);
            }
            if batch.entries.is_empty() {
                return false;
            }
            region_meta.end_index = batch.entries.last().unwrap().index;
        }
        None => {
            let mut region_meta = RegionMeta::new();
            region_meta.region_id = batch.region_id;
            region_meta.start_index = batch.entries.first().unwrap().index;
            region_meta.end_index = batch.entries.last().unwrap().index;
            meta.insert(region_meta.region_id, region_meta);
        }
    }
    return true;
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use crate::tests::make_snap_dir;
    use std::sync::Arc;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_file_meta() {
        println!("write test file begin");
        let temp_dir = TempDir::new("test_file_meta").unwrap();
        let path = temp_dir.path();
        let file_path = path.join("log");
        let mut tmp_f = File::create(file_path.clone()).unwrap();
        // TODO: handle errors.
        let mut content = Vec::new();
        content.extend_from_slice("abcd".as_bytes());
        let mut meta = FileMeta::new();
        meta.content_size = 4;
        meta.path = "log".to_string();
        let meta_len = meta.compute_size();
        let buf = serialize(meta_len);
        content.extend_from_slice(MAGIC_STR);
        meta.write_to_vec(&mut content).unwrap();
        content.extend_from_slice(buf.as_ref());
        content.extend_from_slice(MAGIC_STR);

        tmp_f.write_all(&content).unwrap();
        tmp_f.sync_all().unwrap();

        let c_path = CString::new(file_path.to_str().unwrap().as_bytes()).unwrap();
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
        if fd < 0 {
            panic!("open file failed, err {}", errno::errno().to_string());
        }
        let file_meta = FileMetaIndex::read_meta(fd, &file_path, libc::O_RDONLY).unwrap();
        println!("read meta succeed");
        assert_eq!(meta.content_size, file_meta.size as u64);
        assert_eq!("log".to_string(), file_meta.file_name);
    }

    #[test]
    fn test_log_open_with_two_log_file() {
        let temp_dir = TempDir::new("test_two_log_file").unwrap();
        let path = temp_dir.path();
        {
            let pipe_log = LogStorage::open(path.to_str().unwrap(), 1024).unwrap();
            for index in 1..6 {
                let mut batch = EntryBatch::new();
                batch.region_id = 1;
                let mut entry = RaftEntry::new();
                entry.index = index;
                entry.term = 2;
                batch.entries.push(entry);
                assert!(pipe_log.put(&mut batch));
                if index % 2 == 0 {
                    pipe_log.sync();
                    pipe_log.dump();
                }
            }
            let mut batch = EntryBatch::new();
            batch.region_id = 2;
            let mut entry = RaftEntry::new();
            entry.index = 2;
            entry.term = 2;
            batch.entries.push(entry);
            pipe_log.put(&mut batch);
            pipe_log.sync();
        }
        let pipe_log = LogStorage::open(path.to_str().unwrap(), 1024).unwrap();
        assert_eq!(3, pipe_log.file_num());
        let result = pipe_log.read(1, 2, u64::MAX);
        assert_eq!(4, result.len());
        for i in 0..result.len() {
            assert_eq!(2 + i, result[i].index as usize);
            assert_eq!(2, result[i].term);
        }
    }

    #[test]
    fn test_log_write_repeat_entry() {
        let temp_dir = TempDir::new("write_repeat_entry").unwrap();
        let path = temp_dir.path();
        let pipe_log = LogStorage::open(path.to_str().unwrap(), 1024).unwrap();
        for index in 1..6 {
            let mut batch = EntryBatch::new();
            batch.region_id = 1;
            let mut e = RaftEntry::new();
            e.index = index;
            e.term = 2;
            batch.entries.push(e);
            let mut e2 = RaftEntry::new();
            e2.index = index + 1;
            e2.term = 2;
            batch.entries.push(e2);
            assert!(pipe_log.put(&mut batch));
            pipe_log.sync();
        }
        pipe_log.sync();
        let result = pipe_log.read(1, 2, u64::MAX);
        assert_eq!(5, result.len());
        for i in 0..result.len() {
            assert_eq!(2 + i, result[i].index as usize);
        }
    }

    #[test]
    fn test_log_dump_over_capacity() {
        let temp_dir = TempDir::new("dump_over_capacity").unwrap();
        let path = temp_dir.path();
        let pipe_log = LogStorage::open(path.to_str().unwrap(), 128).unwrap();
        assert_eq!(0, pipe_log.file_num());
        for index in 1..64 {
            let mut batch = EntryBatch::new();
            batch.region_id = 1;
            let mut e = RaftEntry::new();
            e.index = index;
            e.term = 2;
            batch.entries.push(e);
            assert!(pipe_log.put(&mut batch));
        }
        pipe_log.sync();
        assert_eq!(1, pipe_log.file_num());
    }

    #[test]
    fn test_multi_thread_pipe_write() {
        println!("pipe write test begin");
        let temp_dir = TempDir::new("dump_over_capacity").unwrap();
        let path = temp_dir.path();
        let log = LogStorage::open(path.to_str().unwrap(), 1024).unwrap();
        let storage = Arc::new(log);
        let storage2 = storage.clone();
        let handler = std::thread::spawn(move || {
            for index in 1..1025 {
                let mut batch = EntryBatch::new();
                batch.region_id = 1;
                let mut e = RaftEntry::new();
                e.index = index;
                e.term = 2;
                batch.entries.push(e);
                assert!(storage2.put(&mut batch));
                if index % 2 == 0 {
                    storage2.sync();
                }
            }
        });
        let mut file_num = storage.file_num();
        assert!(file_num < 1);
        for index in 1..1025 {
            let mut batch = EntryBatch::new();
            batch.region_id = 2;
            let mut e = RaftEntry::new();
            e.index = index;
            e.term = 2;
            batch.entries.push(e);
            assert!(storage.put(&mut batch));
            if index % 2 == 0 {
                storage.sync();
            }
        }
        let result2 = storage.read(2, 1, u64::MAX);
        assert_eq!(1024, result2.len());
        for i in 0..result2.len() {
            assert_eq!(1 + i, result2[i].index as usize);
        }
        handler.join().unwrap();
        file_num = storage.file_num();
        println!("total write log number: {}", file_num);

        let result = storage.read(1, 1, u64::MAX);
        assert_eq!(1024, result.len());
        for i in 0..result.len() {
            assert_eq!(1 + i, result[i].index as usize);
        }
    }
}
