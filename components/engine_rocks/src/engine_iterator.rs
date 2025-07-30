// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use engine_traits::{self, Result};
use rocksdb::{DBIterator, DB};

use crate::r2e;

pub struct RocksEngineDroppedIterator(DBIterator<Arc<DB>>);

pub fn register_iter_destructor(
    destructor: Box<dyn Fn(RocksEngineDroppedIterator) + Send + Sync + 'static>,
) {
    let mut guard = ITERATOR_DESTRUCTOR.write().unwrap();
    assert!(
        guard.is_none(),
        "Only one iterator destructor can be registered at a time"
    );
    *guard = Some(destructor);
}

static ITERATOR_DESTRUCTOR: RwLock<
    Option<Box<dyn Fn(RocksEngineDroppedIterator) + Send + Sync + 'static>>,
> = RwLock::new(None);

// FIXME: Would prefer using &DB instead of Arc<DB>.  As elsewhere in
// this crate, it would require generic associated types.
pub struct RocksEngineIterator(Option<DBIterator<Arc<DB>>>);

impl RocksEngineIterator {
    pub fn from_raw(iter: DBIterator<Arc<DB>>) -> RocksEngineIterator {
        RocksEngineIterator(Some(iter))
    }

    pub fn sequence(&self) -> Option<u64> {
        self.0.as_ref().unwrap().sequence()
    }
}

impl engine_traits::Iterator for RocksEngineIterator {
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.0
            .as_mut()
            .unwrap()
            .seek(rocksdb::SeekKey::Key(key))
            .map_err(r2e)
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.0
            .as_mut()
            .unwrap()
            .seek_for_prev(rocksdb::SeekKey::Key(key))
            .map_err(r2e)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        self.0
            .as_mut()
            .unwrap()
            .seek(rocksdb::SeekKey::Start)
            .map_err(r2e)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        self.0
            .as_mut()
            .unwrap()
            .seek(rocksdb::SeekKey::End)
            .map_err(r2e)
    }

    fn prev(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(r2e("Iterator invalid"));
        }
        self.0.as_mut().unwrap().prev().map_err(r2e)
    }

    fn next(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(r2e("Iterator invalid"));
        }
        self.0.as_mut().unwrap().next().map_err(r2e)
    }

    fn key(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.0.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.0.as_ref().unwrap().value()
    }

    fn valid(&self) -> Result<bool> {
        self.0.as_ref().unwrap().valid().map_err(r2e)
    }
}

impl Drop for RocksEngineIterator {
    fn drop(&mut self) {
        if let Some(destructor) = ITERATOR_DESTRUCTOR.read().unwrap().as_ref() {
            destructor(RocksEngineDroppedIterator(self.0.take().unwrap()));
        }
    }
}
