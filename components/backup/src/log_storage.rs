use super::file_util::{deserialize, serialize, BatchEntryIterator, FileWrapper};
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
use std::fs::{OpenOptions, File};
use std::io::{Error as IoError, ErrorKind, Result as IoResult, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock, Arc};
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::u64;
use memmap::{Mmap, MmapMut, MmapOptions};

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
const MAX_META_SIZE: usize = 1024 * 1024 * 4;
const MAX_REGION_NUMBER: usize = 1024 * 16;
// pub const FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";
// pub const VERSION: &[u8] = b"v1.0.0";
pub const DEFAULT_FILE_CAPACITY: usize = 512 * 1024 * 1024;
// pub const MAX_WRITE_BUFFER_SIZE: usize = 256 * 1024 * 1024;

const MAGIC_STR: &[u8] = b"haha";


pub struct ActiveLog {
    pub file_name: String,
    pub data: Option<MmapMut>,
    pub file: Option<File>,
    pub meta: HashMap<u64, RegionMeta>,
    pub size: usize,
}

impl ActiveLog {
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

    fn reuse_log(data: MmapMut, file: Option<File>, path: &PathBuf) -> IoResult<ActiveLog> {
        let file_size = data.len();
        let mut region_meta: HashMap<u64, RegionMeta> = HashMap::new();
        let mut content_size = 0;
        let mut offset = 0;
        while offset + 4 < file_size {
            if data[offset..(offset+4)].eq(MAGIC_STR) {
                break;
            }
            let record_len = deserialize(&data[offset..(offset+4)]) as usize;
            if record_len + offset + 4 >= file_size || record_len == 0 {
                break;
            }
            offset += 4;
            let mut record = EntryBatch::new();
            if record.merge_from_bytes(&data[offset..(offset+record_len)]).is_err() {
                break;
            }
            update_raft_meta(&mut region_meta, &mut record);
            offset += record_len;
            content_size = offset;
        }
        Ok(ActiveLog {
            meta: region_meta,
            size: content_size,
            file_name: path.file_name().unwrap().to_str().unwrap().to_string(),
            data: Some(data),
            file,
        })
    }

    pub fn new(data: Option<MmapMut>, file: Option<File>, file_name: String) -> ActiveLog {
        ActiveLog {
            file,
            data,
            file_name,
            size: 0,
            meta: HashMap::new(),
        }
    }

}

pub struct FreezeLog {
    pub file_name: String,
    pub data: Arc<Mmap>,
    pub file: File,
    pub meta: HashMap<u64, RegionMeta>,
    pub size: usize,
}

impl FreezeLog {
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

    pub fn new(file: File, data: Mmap, meta: FileMeta) -> FreezeLog {
        let mut regions = HashMap::default();
        for region_meta in meta.meta.iter() {
            regions.insert(region_meta.region_id, region_meta.clone());
        }
        FreezeLog {
            file,
            data: Arc::new(data),
            file_name: meta.path,
            meta: regions,
            size: meta.content_size as usize,
        }
    }


    fn get_meta(data: &Mmap) -> IoResult<FileMeta> {
        let file_size = data.len();
        let meta_size = deserialize(&data[file_size - 8..file_size - 4]) as usize;
        if meta_size + 8 >= file_size {
            return Err(IoError::new(ErrorKind::Other, "get meta size incorrect"));
        }
        let mut meta = FileMeta::new();
        if meta.merge_from_bytes(&data[file_size - 8 - meta_size..(file_size - 8)]).is_err() {
            return Err(IoError::new(ErrorKind::Other, "deserialize meta failed"));
        }
        return Ok(meta);
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
    pub active_log: ActiveLog,
    pub all_files: Vec<FreezeLog>,
    pub region_indexes: HashMap<u64, RegionMeta>,
    dir: PathBuf,
    begin_write: std::time::Instant,
    pub write_buffer: Option<Vec<u8>>,
    pub sync_buffer: Option<Vec<u8>>,
}

impl LogManager {
    pub fn new(
        active_log: ActiveLog,
        active_log_capacity: usize,
        dir: PathBuf,
        all_files: Vec<FreezeLog>,
    ) -> LogManager {
        let mut region_indexes = HashMap::new();
        for f in all_files.iter() {
            FreezeLog::merge_region_meta(&mut region_indexes, &f.meta);
        }
        LogManager {
            active_log_capacity,
            all_files,
            active_log,
            dir,
            region_indexes,
            write_buffer: Some(Vec::default()),
            sync_buffer: Some(Vec::default()),
            begin_write: Instant::now(),
        }
    }

    fn open_write_file(file_path: PathBuf, capacity: usize) -> IoResult<File> {
        let mut f = OpenOptions::new().read(true).write(true).create(true).open(file_path)?;
        f.set_len(capacity as u64)?;
        Ok(f)
    }

    pub fn open(
        path: &Path,
        filenames: Vec<(u64, String)>,
        active_log_capacity: usize,
    ) -> IoResult<LogManager> {
        let mut all_files = Vec::new();
        for (file_prefix, file_name) in filenames.iter() {
            let file_path = path.join(file_name);
            if *file_prefix == filenames.last().unwrap().0 {
                // The last log file maybe have not written meta into disk. So we must build it meta.
                let mut f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(file_path.clone())?;
                let mut data = unsafe { memmap::MmapOptions::new().map_mut(&f)? };
                let mut active_log = ActiveLog::reuse_log(data, Some(f), &file_path)?;
                return Ok(LogManager::new(
                    active_log,
                    active_log_capacity,
                    path.to_path_buf(),
                    all_files,
                ));
            } else {
                let mut f = OpenOptions::new()
                    .read(true)
                    .open(file_path)?;
                let mut data = unsafe { memmap::MmapOptions::new().map(&f)? };
                let mut meta = FreezeLog::get_meta(&data)?;
                all_files.push(FreezeLog::new(f, data, meta));
            };
        }
        let file_name = generate_file_name(0);
        let file_path = path.join(file_name.clone());
        let mut f = Self::open_write_file(file_path, active_log_capacity + MAX_META_SIZE)?;
        let mut data = unsafe { memmap::MmapOptions::new().map_mut(&f)? };
        let active_log = ActiveLog::new(Some(data), Some(f), file_name);
        Ok(LogManager::new(
            active_log,
            active_log_capacity,
            path.to_path_buf(),
            all_files,
        ))
    }

    fn create_write_file(&self, file_name: String) -> IoResult<File> {
        let file_path = self.dir.join(file_name.clone());
        let mut file = OpenOptions::new().read(true).write(true).create(true).open(file_path)?;
        file.set_len((self.active_log_capacity + MAX_META_SIZE) as u64)?;
        Ok(file)
    }

    pub fn should_dump(&self) -> bool {
        if self.write_buffer.as_ref().unwrap().len() + self.active_log.size > self.active_log_capacity {
            return true;
        } else if self.active_log.meta.len() > MAX_REGION_NUMBER {
            let meta_size = self.active_log.to_file_meta().compute_size() as usize;
            if self.write_buffer.as_ref().unwrap().len() + self.active_log.size + meta_size > self.active_log_capacity {
                return true;
            }
        }
        false
    }


    pub fn dump_meta(&mut self) -> FileMeta {
        let mut write_buffer = self.write_buffer.as_mut().unwrap();
        self.active_log.size += write_buffer.len();
        let write_cost = self.begin_write.elapsed();
        info!("log file write cost time (second)"; "file" => self.active_log.file_name.clone(), "time" => write_cost.as_millis() as u64);
        let meta = self.active_log.to_file_meta();
        self.active_log.meta.clear();
        // We do not create one new file, until we finished dumping last file.
        self.begin_write = Instant::now();
        meta
    }

    fn switch_active_log(&mut self, data: Mmap, meta: FileMeta, size: usize) -> IoResult<()> {
        let freeze_log = FreezeLog::new(self.active_log.file.take().unwrap(), data, meta);
        let prefix = extract_file_num(self.active_log.file_name.as_str()).unwrap();
        let file_name = generate_file_name(prefix + 1);
        let new_file = self.create_write_file(file_name.clone())?;
        let data = unsafe { memmap::MmapOptions::new().map_mut(&new_file)? };
        self.active_log.file_name = file_name;
        self.active_log.file = Some(new_file);
        self.active_log.data = Some(data);
        self.active_log.size = 0;
        self.all_files.push(freeze_log);
        Ok(())
    }

    fn dump(&mut self) -> IoResult<()> {
        let mut data = self.active_log.data.take().unwrap();
        let offset = self.active_log.size;
        let meta = self.dump_meta();
        let mut write_buffer = self.write_buffer.take().unwrap();
        let size = offset + write_buffer.len();
        if size > offset {
            (&mut data[offset..size]).copy_from_slice(write_buffer.as_ref());
            data.flush_range(offset, write_buffer.len())?;
        }
        write_file_meta(&mut data, &meta)?;

        let rd_data = data.make_read_only()?;
        self.switch_active_log(rd_data, meta, size)?;
        write_buffer.clear();
        self.write_buffer = Some(write_buffer);
        Ok(())
    }

    pub fn write_into_buffer(&mut self, batch: &mut EntryBatch) -> bool {
        if batch.entries.is_empty() {
            return true;
        }
        let mut write_buffer = self.write_buffer.as_mut().unwrap();
        if self.active_log.size + write_buffer.len() > self.active_log_capacity {
            return false;
        }
        let buf = serialize(batch.compute_size());
        write_buffer.write_all(buf.as_ref()).unwrap();
        batch.write_to_vec(&mut write_buffer).unwrap();
        return true;
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
        log_files.sort_by_key(|a| a.0);
        let mut log_manager = LogManager::open(
            path.as_path(),
            log_files,
            active_log_capacity,
        )?;
        if log_manager.active_log.size > 0 {
            log_manager.dump()?;
        }
        Ok(LogStorage::new(log_manager))
    }

    pub fn iter(&self) -> BatchEntryIterator {
        let mut fds = Vec::new();
        {
            let log = self.log_manager.read().unwrap();
            for f in &log.all_files {
                fds.push(FileWrapper::new(f.data.clone(), f.size));
            }
            // fds.push((log.active_log.fd, log.active_log.size));
        }
        BatchEntryIterator::new(fds)
    }

    pub fn iterator_with_region(&self, region_id: u64) -> BatchEntryIterator {
        let mut fds = Vec::new();
        {
            let log = self.log_manager.read().unwrap();
            for f in &log.all_files {
                if f.meta.contains_key(&region_id) {
                    fds.push(FileWrapper::new(f.data.clone(), f.size));
                }
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
                    fds.push(FileWrapper::new(f.data.clone(), f.size));
                }
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
        {
            let mut log_manager = self.log_manager.write().unwrap();

            update_raft_meta(&mut log_manager.region_indexes, batch);
            update_raft_meta(&mut log_manager.active_file.meta, batch);
            if 0 == log_manager.active_file.size {
                log_manager.begin_write = Instant::now();
            }
            if log_manager.write_into_buffer(batch) {
                return true;
            }
        }
        while self.sync().is_ok() {
            let mut log_manager = self.log_manager.write().unwrap();
            if log_manager.write_into_buffer(batch) {
                return true;
            }
            info!("write into buffer failed, try sync");
        }
        info!("sync failed");
        return false;
    }

    pub fn sync(&self) -> IoResult<()> {
        let _guard = self.lock.lock().unwrap();
        let mut meta = None;
        let mut offset = 0;
        let mut size = 0;
        let (mut sync_task, mut file) = {
            let mut log = self.log_manager.write().unwrap();
            assert!(log.write_buffer.is_some());
            offset = log.active_log.size;
            if let Some(sync_task) = log.write_buffer.as_ref() {
                if sync_task.is_empty() {
                    return Ok(());
                } else if log.should_dump() {
                    size = log.active_log.size + sync_task.len();
                    meta = Some(log.dump_meta());
                }
            }
            let task = log.write_buffer.take();
            let file = log.active_log.data.take();
            log.write_buffer = log.sync_buffer.take();
            (task, file)
        };
        let begin_dump = Instant::now();
        let write_buffer = sync_task.as_mut().unwrap();
        let mut data = file.unwrap();
        (&mut data[offset..(offset+write_buffer.len())]).copy_from_slice(write_buffer.as_ref());
        if let Some(active_meta) = &meta {
            write_file_meta(&mut data, active_meta)?;
        }
        data.flush_range(offset, write_buffer.len())?;
        let mut log = self.log_manager.write().unwrap();
        if meta.is_some() {
            let rd_data = data.make_read_only()?;
            log.switch_active_log(rd_data, meta.unwrap(), size);
            assert_eq!(0, log.active_log.size);
            info!("log file write sync last buffer cost"; "time" => begin_dump.elapsed().as_millis() as u64,
            "last_buffer_size" => write_buffer.len());
        } else {
            log.active_log.size += write_buffer.len();
            log.active_log.data = Some(data);
        }
        write_buffer.clear();
        log.sync_buffer = sync_task;
        Ok(())
    }

    fn dump(&self) {
        let mut log = self.log_manager.write().unwrap();
        if log.active_log.size > 0 {
            log.dump().unwrap();
        }
    }

    fn file_num(&self) -> usize {
        let log = self.log_manager.read().unwrap();
        return log.all_files.len();
    }

    fn size(&self) -> usize {
        let log = self.log_manager.read().unwrap();
        return log.active_log.size;
    }
}

fn extract_file_num(file_name: &str) -> IoResult<u64> {
    match file_name[..FILE_NUM_LEN].parse::<u64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(IoError::new(ErrorKind::Other, "error file prefix")),
    }
}

fn generate_file_name(file_num: u64) -> String {
    format!("{:016}{}", file_num, LOG_SUFFIX)
}

fn write_file_meta(f: &mut MmapMut, meta: &FileMeta) -> IoResult<()> {
    let meta_len = meta.compute_size();
    let mut meta_buf = Vec::with_capacity(meta_len as usize + 256);
    meta_buf.extend_from_slice(MAGIC_STR);
    meta.write_to_vec(&mut meta_buf).unwrap();
    let buf = serialize(meta_len);
    meta_buf.extend_from_slice(buf.as_ref());
    meta_buf.extend_from_slice(MAGIC_STR);
    let offset = f.len() - meta_buf.len();
    (&mut f[offset..]).copy_from_slice(meta_buf.as_slice());
    f.flush_range(offset, meta_buf.len())
}

fn update_raft_meta(meta: &mut HashMap<u64, RegionMeta>, batch: &mut EntryBatch) {
    match meta.get_mut(&batch.region_id) {
        Some(region_meta) => {
            while !batch.entries.is_empty()
                && batch.entries.first().unwrap().index <= region_meta.end_index
            {
                batch.entries.remove(0);
            }
            if batch.entries.is_empty() {
                return;
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
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use crate::tests::make_snap_dir;
    use std::fs::File;
    use std::io::Write;
    use std::sync::Arc;

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

//        let c_path = CString::new(file_path.to_str().unwrap().as_bytes()).unwrap();
//        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
//        if fd < 0 {
//            panic!("open file failed, err {}", errno::errno().to_string());
//        }
//        let file_meta = FreezeLog::read_meta(fd, &file_path, libc::O_RDONLY).unwrap();
//        println!("read meta succeed");
//        assert_eq!(meta.content_size, file_meta.size as u64);
//        assert_eq!("log".to_string(), file_meta.file_name);
    }

    #[test]
    fn test_log_open_with_two_log_file() {
        let temp_dir = TempDir::new("test_two_log_file").unwrap();
        let path = temp_dir.path();
        println!(".....step1");
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
                    println!(".....sync");
                    pipe_log.sync().unwrap();
                    println!(".....dump ");
                    pipe_log.dump();
                }
            }
            println!(".....step2");
            let mut batch = EntryBatch::new();
            batch.region_id = 2;
            let mut entry = RaftEntry::new();
            entry.index = 2;
            entry.term = 2;
            batch.entries.push(entry);
            pipe_log.put(&mut batch);
            pipe_log.sync();
        }
        println!(".....step5");
        let pipe_log = LogStorage::open(path.to_str().unwrap(), 1024).unwrap();
        println!(".....step6");
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
        pipe_log.file_num();
        pipe_log.dump();
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
        let pipe_log = LogStorage::open(path.to_str().unwrap(), 512).unwrap();
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

    fn inner_test_write_multi_thread(path: &Path, thread_num: u64) {
        let log = LogStorage::open(path.to_str().unwrap(), 1024).unwrap();
        let storage = Arc::new(log);
        let mut handlers = Vec::new();
        for i in 0..thread_num {
            let storage2 = storage.clone();
            let handler = std::thread::spawn(move || {
                for index in 1..1025 {
                    let mut batch = EntryBatch::new();
                    batch.region_id = i + 1;
                    let mut e = RaftEntry::new();
                    e.index = index;
                    e.term = 2;
                    batch.entries.push(e);
                    assert!(storage2.put(&mut batch));
                    if index % 2 == 0 {
                        storage2.sync();
                    }
                }
                storage2.dump();
            });
            handlers.push(handler);
        }
        while !handlers.is_empty() {
            let h = handlers.pop().unwrap();
            h.join();
        }

        let mut file_num = storage.file_num();
        assert!(file_num > 1);
        file_num = storage.file_num();
        println!("total write log number: {}", file_num);
        for i in 0..thread_num {
            let result = storage.read(i + 1, 1, u64::MAX);
            assert_eq!(1024, result.len());
            for i in 0..result.len() {
                assert_eq!(1 + i, result[i].index as usize);
            }
        }
    }

    #[test]
    fn test_multi_thread_pipe_write() {
        println!("pipe write test begin");
        let temp_dir = TempDir::new("dump_over_capacity").unwrap();
        let path = temp_dir.path();
        inner_test_write_multi_thread(path, 6);
        let log = LogStorage::open(path.to_str().unwrap(), 1024).unwrap();
        let guard = log.log_manager.read().unwrap();
        let meta = &guard.region_indexes;
        assert_eq!(6, meta.len());
        for (_, region_meta) in meta.iter() {
            assert_eq!(1, region_meta.start_index);
            assert_eq!(1024, region_meta.end_index);
        }
    }
}
