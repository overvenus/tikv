use errno;
use kvproto::backup::EntryBatch;
use libc;
use protobuf::Message;
use std::u64;
use std::sync::Arc;
use std::fs::{OpenOptions, File};
use memmap::{Mmap, MmapMut};


pub struct FileWrapper {
    pub data: Arc<Mmap>,
    pub size: usize,
}

impl FileWrapper {
    pub fn new(data: Arc<Mmap>, size: usize) -> FileWrapper {
        FileWrapper {
            data,
            size
        }
    }
}

pub struct BatchEntryIterator {
    files: Vec<FileWrapper>,
    cursor_file: usize,
    cursor_offset: usize,
    current_record: Option<EntryBatch>,
}

impl BatchEntryIterator {
    pub fn new(files: Vec<FileWrapper>) -> BatchEntryIterator {
        BatchEntryIterator {
            files,
            cursor_file: 0,
            cursor_offset: 0,
            current_record: None,
        }
    }

    pub fn valid(&self) -> bool {
        // self.cursor_file + 1 < self.files.len() || (self.cursor_file < self.files.len() && self.cursor_offset < self.files[self.cursor_file].1)
        self.current_record.is_some()
    }
    pub fn next(&mut self) {
        // move 4 bytes for size storage
        self.cursor_offset += self.current_record.as_ref().unwrap().compute_size() as usize + 4;
        if self.cursor_offset >= self.files[self.cursor_file].size
            && self.cursor_file + 1 < self.files.len()
        {
            self.cursor_file += 1;
            self.cursor_offset = 0;
        }
        self.current_record = self.read_record();
    }
    pub fn get(&self) -> Option<EntryBatch> {
        self.current_record.clone()
    }

    pub fn get_ref(&self) -> Option<&EntryBatch> {
        self.current_record.as_ref()
    }

    pub fn get_offset(&self) -> usize {
        self.cursor_offset
    }

    pub fn seek_to_first(&mut self) {
        self.cursor_file = 0;
        self.cursor_offset = 0;
        self.current_record = self.read_record();
    }

    fn buffer(&self, f: usize, offset: usize, len: usize) -> &[u8] {
        return &self.files[f].data[offset..(offset+len)];
    }

    fn read_record(&mut self) -> Option<EntryBatch> {
        let mut record_len = 0;
        if self.cursor_file >= self.files.len()
            || self.cursor_offset + 4 > self.files[self.cursor_file].size
        {
            return None;
        }
        let record_len = deserialize(self.buffer(self.cursor_file, self.cursor_offset, 4));
        if record_len as usize + self.cursor_offset > self.files[self.cursor_file].size {
            return None;
        }
        let mut record = EntryBatch::new();
        if record.merge_from_bytes(self.buffer(self.cursor_file, self.cursor_offset + 4, record_len as usize)).is_err() {
            return None;
        }
        Some(record)
    }
}

pub fn serialize(x: u32) -> [u8; 4] {
    let b1: u8 = ((x >> 24) & 0xff) as u8;
    let b2: u8 = ((x >> 16) & 0xff) as u8;
    let b3: u8 = ((x >> 8) & 0xff) as u8;
    let b4: u8 = (x & 0xff) as u8;
    return [b1, b2, b3, b4];
}

pub fn deserialize(buf: &[u8]) -> u32 {
    let mut x = 0;
    for i in 0..4 {
        x = (x << 8) | buf[i] as u32;
    }
    return x;
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use crate::tests::make_snap_dir;
    use raft::eraftpb::Entry as RaftEntry;
    use std::ffi::CString;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_serialize() {
        let buf = serialize(20190101);
        let ret = deserialize(buf.as_ref());
        assert_eq!(20190101, ret);
    }

    #[test]
    fn test_iterator() {
        println!("write test file begin");
        let temp_dir = TempDir::new("test_iterator").unwrap();
        let path = temp_dir.path();
        let file_path = path.join("log");
        let mut tmp_f = File::create(file_path.clone()).unwrap();
        // TODO: handle errors.
        let mut file_size = 0;
        for region_id in 1..3 {
            let mut content = Vec::new();
            let mut batch = EntryBatch::new();
            batch.region_id = region_id;
            let mut entry = RaftEntry::new();
            entry.index = 2;
            entry.term = 2;
            batch.entries.push(entry);
            batch.write_to_vec(&mut content).unwrap();
            assert_eq!(batch.compute_size() as usize, content.len());
            println!(
                "write content({} bytes) in offset: {}",
                content.len() + 4,
                file_size
            );
            file_size += content.len() + 4;
            let len_data = serialize(content.len() as u32);
            tmp_f.write_all(len_data.as_ref()).unwrap();
            tmp_f.write_all(&content).unwrap();
        }
        tmp_f.sync_all().unwrap();
        let c_path = CString::new(file_path.to_str().unwrap().as_bytes()).unwrap();
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
        assert!(fd > 0);
        let fs = tmp_f.metadata().unwrap().len() as usize;
        assert_eq!(file_size, fs);
        let mut vs = Vec::new();
        let mut f = OpenOptions::new()
                    .read(true)
                    .open(file_path).unwrap();
        let data = unsafe { memmap::MmapOptions::new().map(&f).unwrap() };
        vs.push(FileWrapper::new(Arc::new(data), file_size));

        let mut iter = BatchEntryIterator::new(vs);
        iter.seek_to_first();
        let mut region_id = 1;
        while iter.valid() {
            let batch = iter.get_ref().unwrap();
            assert_eq!(region_id, batch.region_id);
            assert_eq!(1, batch.entries.len());
            region_id += 1;
            iter.next();
        }
        assert_eq!(3, region_id);
    }
}
