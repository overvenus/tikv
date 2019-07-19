use errno;
use kvproto::backup::EntryBatch;
use libc;
use protobuf::Message;
use std::u64;

pub struct BatchEntryIterator {
    files: Vec<(libc::c_int, usize)>,
    cursor_file: usize,
    cursor_offset: usize,
    current_record: Option<EntryBatch>,
    // read_buffer: Vec<u8>,
}

impl BatchEntryIterator {
    pub fn new(files: Vec<(libc::c_int, usize)>) -> BatchEntryIterator {
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
        if self.cursor_offset >= self.files[self.cursor_file].1
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

    fn read_record(&mut self) -> Option<EntryBatch> {
        let mut record_len = 0;
        if self.cursor_file >= self.files.len()
            || self.cursor_offset + 4 > self.files[self.cursor_file].1
        {
            return None;
        }
        //        println!("read record in file[{}]={}, {}, file size: {}", self.cursor_file, self.files[self.cursor_file].0,
        //                 self.cursor_offset, self.files[self.cursor_file].1);
        if !pread_int(
            self.files[self.cursor_file].0,
            self.cursor_offset as u64,
            &mut record_len,
        ) {
            return None;
        }
        if record_len as usize + self.cursor_offset > self.files[self.cursor_file].1 {
            return None;
        }
        let mut buf = Vec::with_capacity(record_len as usize);
        if !pread(
            self.files[self.cursor_file].0,
            &mut buf,
            self.cursor_offset as u64 + 4,
            record_len as u64,
        ) {
            return None;
        }
        let mut record = EntryBatch::new();
        if record.merge_from_bytes(buf.as_ref()).is_err() {
            return None;
        }
        Some(record)
    }
}

pub fn pread(fd: libc::c_int, result: &mut Vec<u8>, offset: u64, len: u64) -> bool {
    let buf = result.as_mut_ptr();
    unsafe {
        loop {
            let ret_size = libc::pread(
                fd,
                buf as *mut libc::c_void,
                len as libc::size_t,
                offset as libc::off_t,
            );
            if ret_size < 0 {
                let err = errno::errno();
                if err.0 == libc::EAGAIN {
                    continue;
                }
                panic!(
                    "pread in {}, len: {}, failed, err {}",
                    offset,
                    len,
                    err.to_string()
                );
            }
            if ret_size as u64 != len {
                error!(
                    "Pread failed, expected return size {}, actual return size {}",
                    len, ret_size
                );
                return false;
            }
            result.set_len(len as usize);
            break;
        }
        return true;
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

pub fn write(fd: libc::c_int, content: &mut Vec<u8>) -> bool {
    unsafe {
        let ret = libc::write(fd, content.as_ptr() as *const libc::c_void, content.len());
        return ret == content.len() as libc::ssize_t;
    }
}

pub fn sync(fd: libc::c_int) {
    unsafe {
        if libc::fsync(fd) != 0 {
            panic!("fsync failed, err {}", errno::errno().to_string());
        }
    }
}

pub fn close(fd: libc::c_int) {
    unsafe {
        libc::close(fd);
    }
}

pub fn pread_int(fd: libc::c_int, offset: u64, result: &mut u32) -> bool {
    // let x = std::mem::size_of::<T>();
    let mut buf = Vec::with_capacity(4);
    if !pread(fd, &mut buf, offset, 4) {
        return false;
    }
    *result = deserialize(buf.as_slice());
    true
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
        vs.push((fd, fs));
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
