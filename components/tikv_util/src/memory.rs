// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use collections::HashMap;
use kvproto::{
    encryptionpb::EncryptionMeta,
    kvrpcpb::LockInfo,
    metapb::{Peer, Region, RegionEpoch},
    raft_cmdpb::{self, RaftCmdRequest, ReadIndexRequest},
};

/// Transmute vec from one type to the other type.
///
/// # Safety
///
/// The two types should be with same memory layout.
#[inline]
pub unsafe fn vec_transmute<F, T>(from: Vec<F>) -> Vec<T> {
    debug_assert!(mem::size_of::<F>() == mem::size_of::<T>());
    debug_assert!(mem::align_of::<F>() == mem::align_of::<T>());
    let (ptr, len, cap) = from.into_raw_parts();
    Vec::from_raw_parts(ptr as _, len, cap)
}

pub trait HeapSize {
    fn heap_size(&self) -> usize {
        0
    }
}

macro_rules! impl_heap_size{
    (
        $($typ: ty,)+
    ) => {
        $(
            impl HeapSize for $typ {
                fn heap_size(&self) -> usize {
                    std::mem::size_of::<Self>()
                }
            }
        )+
    }
}

impl_heap_size! {
    // These types are not necessary stored in heap as they are be inlined
    // in structures.
    // TODO: We may need to add a new method to distinguish the difference.
    u8, bool, u64,
}

impl<T: HeapSize> HeapSize for [T] {
    fn heap_size(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            self.len() * self[0].heap_size()
        }
    }
}

impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_size(&self) -> usize {
        self.as_slice().heap_size()
    }
}

impl<A: HeapSize, B: HeapSize> HeapSize for (A, B) {
    fn heap_size(&self) -> usize {
        self.0.heap_size() + self.1.heap_size()
    }
}

impl<T: HeapSize> HeapSize for Option<T> {
    fn heap_size(&self) -> usize {
        match self {
            Some(t) => t.heap_size(),
            None => 0,
        }
    }
}

impl<K: HeapSize, V: HeapSize> HeapSize for HashMap<K, V> {
    fn heap_size(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            let kv = self.iter().next().unwrap();
            self.len() * (kv.0.heap_size() + kv.1.heap_size())
        }
    }
}

impl HeapSize for Region {
    fn heap_size(&self) -> usize {
        let mut size = self.start_key.capacity() + self.end_key.capacity();
        size += mem::size_of::<RegionEpoch>();
        size += self.peers.capacity() * mem::size_of::<Peer>();
        // There is still a `bytes` in `EncryptionMeta`. Ignore it because it could be
        // shared.
        size += mem::size_of::<EncryptionMeta>();
        size
    }
}

impl HeapSize for ReadIndexRequest {
    fn heap_size(&self) -> usize {
        self.key_ranges
            .iter()
            .map(|r| r.start_key.capacity() + r.end_key.capacity())
            .sum()
    }
}

impl HeapSize for LockInfo {
    fn heap_size(&self) -> usize {
        self.primary_lock.capacity()
            + self.key.capacity()
            + self.secondaries.iter().map(|k| k.len()).sum::<usize>()
    }
}

impl HeapSize for RaftCmdRequest {
    fn heap_size(&self) -> usize {
        mem::size_of::<raft_cmdpb::RaftRequestHeader>()
            + self.requests.capacity() * mem::size_of::<raft_cmdpb::Request>()
            + mem::size_of_val(&self.admin_request)
            + mem::size_of_val(&self.status_request)
    }
}

#[derive(Debug)]
pub struct MemoryQuotaExceeded;

impl std::error::Error for MemoryQuotaExceeded {}

impl_display_as_debug!(MemoryQuotaExceeded);

pub struct MemoryQuota {
    in_use: AtomicUsize,
    capacity: AtomicUsize,
}

pub struct OwnedAllocated {
    allocated: usize,
    from: Arc<MemoryQuota>,
}

impl OwnedAllocated {
    pub fn new(target: Arc<MemoryQuota>) -> Self {
        Self {
            allocated: 0,
            from: target,
        }
    }

    pub fn alloc(&mut self, bytes: usize) -> Result<(), MemoryQuotaExceeded> {
        self.from.alloc(bytes)?;
        self.allocated += bytes;
        Ok(())
    }
}

impl Drop for OwnedAllocated {
    fn drop(&mut self) {
        self.from.free(self.allocated)
    }
}

impl MemoryQuota {
    pub fn new(capacity: usize) -> MemoryQuota {
        MemoryQuota {
            in_use: AtomicUsize::new(0),
            capacity: AtomicUsize::new(capacity),
        }
    }

    pub fn in_use(&self) -> usize {
        self.in_use.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    pub fn set_capacity(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::Relaxed);
    }

    pub fn alloc_force(&self, bytes: usize) {
        let mut in_use_bytes = self.in_use.load(Ordering::Relaxed);
        loop {
            let new_in_use_bytes = in_use_bytes + bytes;
            match self.in_use.compare_exchange_weak(
                in_use_bytes,
                new_in_use_bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => in_use_bytes = current,
            }
        }
    }

    pub fn alloc(&self, bytes: usize) -> Result<(), MemoryQuotaExceeded> {
        let capacity = self.capacity.load(Ordering::Relaxed);
        let mut in_use_bytes = self.in_use.load(Ordering::Relaxed);
        loop {
            if in_use_bytes + bytes > capacity {
                return Err(MemoryQuotaExceeded);
            }
            let new_in_use_bytes = in_use_bytes + bytes;
            match self.in_use.compare_exchange_weak(
                in_use_bytes,
                new_in_use_bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(current) => in_use_bytes = current,
            }
        }
    }

    pub fn free(&self, bytes: usize) {
        let mut in_use_bytes = self.in_use.load(Ordering::Relaxed);
        loop {
            // Saturating at the numeric bounds instead of overflowing.
            let new_in_use_bytes = in_use_bytes - std::cmp::min(bytes, in_use_bytes);
            match self.in_use.compare_exchange_weak(
                in_use_bytes,
                new_in_use_bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => in_use_bytes = current,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_quota() {
        let quota = MemoryQuota::new(100);
        quota.alloc(10).unwrap();
        assert_eq!(quota.in_use(), 10);
        quota.alloc(100).unwrap_err();
        assert_eq!(quota.in_use(), 10);
        quota.free(5);
        assert_eq!(quota.in_use(), 5);
        quota.alloc(95).unwrap();
        assert_eq!(quota.in_use(), 100);
        quota.free(95);
        assert_eq!(quota.in_use(), 5);
    }

    #[test]
    fn test_resize_memory_quota() {
        let quota = MemoryQuota::new(100);
        quota.alloc(10).unwrap();
        assert_eq!(quota.in_use(), 10);
        quota.alloc(100).unwrap_err();
        assert_eq!(quota.in_use(), 10);
        quota.set_capacity(200);
        quota.alloc(100).unwrap();
        assert_eq!(quota.in_use(), 110);
        quota.set_capacity(50);
        quota.alloc(100).unwrap_err();
        assert_eq!(quota.in_use(), 110);
        quota.free(100);
        assert_eq!(quota.in_use(), 10);
        quota.alloc(40).unwrap();
        assert_eq!(quota.in_use(), 50);
    }

    #[test]
    fn test_allocated() {
        let quota = Arc::new(MemoryQuota::new(100));
        let mut allocated = OwnedAllocated::new(Arc::clone(&quota));
        allocated.alloc(42).unwrap();
        assert_eq!(quota.in_use(), 42);
        quota.alloc(59).unwrap_err();
        allocated.alloc(16).unwrap();
        assert_eq!(quota.in_use(), 58);
        let mut allocated2 = OwnedAllocated::new(Arc::clone(&quota));
        allocated2.alloc(8).unwrap();
        allocated2.alloc(40).unwrap_err();
        assert_eq!(quota.in_use(), 66);
        quota.alloc(4).unwrap();
        assert_eq!(quota.in_use(), 70);
        drop(allocated);
        assert_eq!(quota.in_use(), 12);
        drop(allocated2);
        assert_eq!(quota.in_use(), 4);
    }

    #[test]
    fn test_alloc_force() {
        let quota = MemoryQuota::new(100);
        quota.alloc(10).unwrap();
        assert_eq!(quota.in_use(), 10);
        quota.alloc_force(100);
        assert_eq!(quota.in_use(), 110);

        quota.free(10);
        assert_eq!(quota.in_use(), 100);
        quota.alloc(10).unwrap_err();
        assert_eq!(quota.in_use(), 100);

        quota.alloc_force(20);
        assert_eq!(quota.in_use(), 120);
        quota.free(110);
        assert_eq!(quota.in_use(), 10);

        quota.alloc(10).unwrap();
        assert_eq!(quota.in_use(), 20);
        quota.free(10);
        assert_eq!(quota.in_use(), 10);

        // Resize to a smaller capacity
        quota.set_capacity(10);
        quota.alloc(100).unwrap_err();
        assert_eq!(quota.in_use(), 10);
        quota.alloc_force(100);
        assert_eq!(quota.in_use(), 110);
        // Resize to a larger capacity
        quota.set_capacity(120);
        quota.alloc(10).unwrap();
        assert_eq!(quota.in_use(), 120);
        quota.alloc_force(100);
        assert_eq!(quota.in_use(), 220);
        // Free more then it has.
        quota.free(230);
        assert_eq!(quota.in_use(), 0);
    }
}
