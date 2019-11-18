// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::atomic::*;
// use std::sync::mpsc::Sender as StdSender;
use std::sync::*;

use engine::rocks::util::io_limiter::IOLimiter;
use engine::DB;
use external_storage::*;
use futures::lazy;
use futures::sync::mpsc::*;
use kvproto::backup::*;
use kvproto::metapb::*;
use raft::StateRole;
use tidb_query::codec::table::decode_table_id;
use tikv::raftstore::store::util::find_peer;
use tikv::storage::kv::{Engine, RegionInfoProvider};
use tikv::storage::{Key, Statistics};
use tokio_threadpool::ThreadPool;

use crate::backup_range::BackupRange;
use crate::errors::{Error as BError, Result};
use crate::writer::BackupWriter;

const WORKER_TAKE_RANGE: usize = 6;

type BackupRes = (Vec<File>, Statistics);

#[derive(Clone)]
pub struct LimitedStorage {
    pub limiter: Option<Arc<IOLimiter>>,
    pub storage: Arc<dyn ExternalStorage>,
}

static ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct TaskID(usize);

/// The progress of a backup task
pub struct Progress<R: RegionInfoProvider> {
    store_id: u64,
    next_start: Option<Key>,
    end_key: Option<Key>,
    region_info: R,
    finished: bool,
    // complete_tx: StdSender<()>,
}

impl<R: RegionInfoProvider> Drop for Progress<R> {
    fn drop(&mut self) {
        // TODO notify completion.
        // if let Err(_) = self.complete_tx.send(()) {
        //     warn!("fail to send completeness"; "id" => ?self.id);
        // }
    }
}

impl<R: RegionInfoProvider> Progress<R> {
    pub fn new(
        store_id: u64,
        next_start: Option<Key>,
        end_key: Option<Key>,
        region_info: R,
    ) -> Self {
        Progress {
            store_id,
            next_start,
            end_key,
            region_info,
            finished: false,
        }
    }

    /// Forward the progress by `ranges` BackupRanges
    ///
    /// The size of the returned BackupRanges should <= `ranges`
    pub fn forward(&mut self, limit: usize) -> Vec<BackupRange> {
        if self.finished {
            return Vec::new();
        }
        let store_id = self.store_id;
        let (tx, rx) = mpsc::channel();
        let start_key = self.next_start.clone();
        let end_key = self.end_key.clone();
        let res = self.region_info.seek_region(
            &self
                .next_start
                .clone()
                .map_or_else(Vec::new, |k| k.into_encoded()),
            Box::new(move |iter| {
                let mut sended = 0;
                for info in iter {
                    let region = &info.region;
                    if end_key.is_some() {
                        let end_slice = end_key.as_ref().unwrap().as_encoded().as_slice();
                        if end_slice <= region.get_start_key() {
                            // We have reached the end.
                            // The range is defined as [start, end) so break if
                            // region start key is greater or equal to end key.
                            break;
                        }
                    }
                    if info.role == StateRole::Leader {
                        let ekey = get_min_end_key(end_key.as_ref(), &region);
                        let skey = get_max_start_key(start_key.as_ref(), &region);
                        assert!(!(skey == ekey && ekey.is_some()), "{:?} {:?}", skey, ekey);
                        let leader = find_peer(region, store_id).unwrap().to_owned();
                        let backup_range = BackupRange {
                            start_key: skey,
                            end_key: ekey,
                            region: region.clone(),
                            leader,
                        };
                        tx.send(backup_range).unwrap();
                        sended += 1;
                        if sended >= limit {
                            break;
                        }
                    }
                }
            }),
        );
        if let Err(e) = res {
            // TODO: handle error.
            error!("backup seek region failed"; "error" => ?e);
        }

        let branges: Vec<_> = rx.iter().collect();
        if let Some(b) = branges.last() {
            // The region's end key is empty means it is the last
            // region, we need to set the `finished` flag here in case
            // we run with `next_start` set to None
            if b.region.get_end_key().is_empty() || b.end_key == self.end_key {
                self.finished = true;
            }
            self.next_start = b.end_key.clone();
        } else {
            self.finished = true;
        }
        branges
    }
}

/// Backup task.
pub struct Task {
    id: TaskID,
    start_key: Option<Key>,
    end_key: Option<Key>,
    pub(crate) start_ts: u64,
    pub(crate) end_ts: u64,
    pub(crate) rate_limit: u64,
    pub(crate) concurrency: u32,
    pub(crate) path: String,

    cancel: Arc<AtomicBool>,
    pub(crate) resp: UnboundedSender<BackupResponse>,
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackupTask")
            .field("start_ts", &self.start_ts)
            .field("end_ts", &self.end_ts)
            .field("start_key", &self.start_key)
            .field("end_key", &self.end_key)
            .finish()
    }
}

impl Task {
    /// Create a backup task based on the given backup request.
    pub fn new(
        mut req: BackupRequest,
        resp: UnboundedSender<BackupResponse>,
    ) -> Result<(Task, Arc<AtomicBool>)> {
        let id = TaskID(ID_ALLOC.fetch_add(1, Ordering::Relaxed));
        let cancel = Arc::new(AtomicBool::new(false));

        let start_key = if req.start_key.is_empty() {
            None
        } else {
            Some(Key::from_raw(&req.start_key))
        };
        let end_key = if req.end_key.is_empty() {
            None
        } else {
            Some(Key::from_raw(&req.end_key))
        };

        if req.get_end_version() == 0 || req.get_path().is_empty() || req.get_concurrency() == 0 {
            return Err(BError::Other(format!("invalid request {:?}", req).into()));
        }

        Ok((
            Task {
                id,
                start_key,
                end_key,
                start_ts: req.get_start_version(),
                end_ts: req.get_end_version(),
                path: req.take_path(),
                rate_limit: req.get_rate_limit(),
                concurrency: req.get_concurrency(),
                cancel: cancel.clone(),
                resp,
            },
            cancel,
        ))
    }

    /// Check whether the task is canceled.
    pub fn has_canceled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }

    pub fn spawn_backup_worker<E, R>(
        &self,
        pool: &ThreadPool,
        store_id: u64,
        db: Arc<DB>,
        engine: &E,
        region_info: &R,
        tx: mpsc::Sender<(BackupRange, Result<BackupRes>)>,
    ) -> Result<()>
    where
        E: Engine,
        R: RegionInfoProvider,
    {
        let progress = Arc::new(Mutex::new(Progress::new(
            store_id,
            self.start_key.clone(),
            self.end_key.clone(),
            region_info.clone(),
        )));

        let limiter = if self.rate_limit != 0 {
            Some(Arc::new(IOLimiter::new(self.rate_limit as _)))
        } else {
            None
        };
        let storage = LimitedStorage {
            storage: create_storage(&self.path)?,
            limiter,
        };

        // TODO: support incremental backup
        let _ = self.start_ts;

        let backup_ts = self.end_ts;
        let store_id = store_id;
        for _ in 0..self.concurrency {
            let prs = progress.clone();
            let cancel = self.cancel.clone();
            let engine = engine.clone();
            let db = db.clone();
            let storage = storage.clone();
            let tx = tx.clone();
            // TODO: make it async.
            pool.spawn(lazy(move || loop {
                let branges = prs.lock().unwrap().forward(WORKER_TAKE_RANGE);
                debug!("backup forward ranges"; "ranges" => ?branges);
                if branges.is_empty() {
                    return Ok(());
                }
                for brange in branges {
                    if cancel.load(Ordering::SeqCst) {
                        warn!("backup task has canceled"; "range" => ?brange);
                        return Ok(());
                    }
                    let table_id = brange
                        .start_key
                        .clone()
                        .and_then(|k| decode_table_id(&k.into_raw().unwrap()).ok());
                    let name = backup_file_name(store_id, &brange.region, table_id);
                    let mut writer =
                        match BackupWriter::new(db.clone(), &name, storage.limiter.clone()) {
                            Ok(w) => w,
                            Err(e) => {
                                error!("backup writer failed"; "error" => ?e);
                                return tx.send((brange, Err(e))).map_err(|_| ());
                            }
                        };
                    let stat = match brange.backup(&mut writer, &engine, backup_ts) {
                        Ok(s) => s,
                        Err(e) => return tx.send((brange, Err(e))).map_err(|_| ()),
                    };
                    // Save sst files to storage.
                    let files = match writer.save(&storage.storage) {
                        Ok(files) => files,
                        Err(e) => {
                            error!("backup save file failed"; "error" => ?e);
                            return tx.send((brange, Err(e))).map_err(|_| ());
                        }
                    };
                    let _ = tx.send((brange, Ok((files, stat)))).map_err(|_| ());
                }
            }));
        }
        Ok(())
    }
}

/// Get the min end key from the given `end_key` and `Region`'s end key.
fn get_min_end_key(end_key: Option<&Key>, region: &Region) -> Option<Key> {
    let region_end = if region.get_end_key().is_empty() {
        None
    } else {
        Some(Key::from_encoded_slice(region.get_end_key()))
    };
    if region.get_end_key().is_empty() {
        end_key.cloned()
    } else if end_key.is_none() {
        region_end
    } else {
        let end_slice = end_key.as_ref().unwrap().as_encoded().as_slice();
        if end_slice < region.get_end_key() {
            end_key.cloned()
        } else {
            region_end
        }
    }
}

/// Get the max start key from the given `start_key` and `Region`'s start key.
fn get_max_start_key(start_key: Option<&Key>, region: &Region) -> Option<Key> {
    let region_start = if region.get_start_key().is_empty() {
        None
    } else {
        Some(Key::from_encoded_slice(region.get_start_key()))
    };
    if start_key.is_none() {
        region_start
    } else {
        let start_slice = start_key.as_ref().unwrap().as_encoded().as_slice();
        if start_slice < region.get_start_key() {
            region_start
        } else {
            start_key.cloned()
        }
    }
}

/// Construct an backup file name based on the given store id and region.
/// A name consists with three parts: store id, region_id and a epoch version.
fn backup_file_name(store_id: u64, region: &Region, table_id: Option<i64>) -> String {
    match table_id {
        Some(t_id) => format!(
            "{}_{}_{}_{}",
            store_id,
            region.get_id(),
            region.get_region_epoch().get_version(),
            t_id
        ),
        None => format!(
            "{}_{}_{}",
            store_id,
            region.get_id(),
            region.get_region_epoch().get_version()
        ),
    }
}
