// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::mem;
use std::result;
use std::thread;
use std::time::Duration;
use std::u64;

use futures::{Async, Future, Poll};
use kvproto::kvrpcpb::{CommandPri, Context, LockInfo};

use storage::engine::{CbContext, Modify, Result as EngineResult};
use storage::mvcc::{
    Error as MvccError, Lock as MvccLock, MvccReader, MvccTxn, Write, MAX_TXN_WRITE_SIZE,
};
use storage::{
    Command, Engine, Error as StorageError, Result as StorageResult, ScanMode, Snapshot,
    Statistics, StorageCb,
};
use storage::{Key, MvccInfo, Value};
use util::future::paired_future_callback;
use util::time::SlowTimer;

use super::super::metrics::*;
use super::latch::Lock;
use super::scheduler::Msg;
use super::{Error, Result};

// To resolve a key, the write size is about 100~150 bytes, depending on key and value length.
// The write batch will be around 32KB if we scan 256 keys each time.
pub const RESOLVE_LOCK_BATCH_SIZE: usize = 256;

/// Process result of a command.
pub enum ProcessResult {
    Res,
    MultiRes { results: Vec<StorageResult<()>> },
    MvccKey { mvcc: MvccInfo },
    MvccStartTs { mvcc: Option<(Key, MvccInfo)> },
    Locks { locks: Vec<LockInfo> },
    NextCommand { cmd: Command },
    Failed { err: StorageError },
}

/// Delivers the process result of a command to the storage callback.
pub fn execute_callback(callback: StorageCb, pr: ProcessResult) {
    match callback {
        StorageCb::Boolean(cb) => match pr {
            ProcessResult::Res => cb(Ok(())),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
        StorageCb::Booleans(cb) => match pr {
            ProcessResult::MultiRes { results } => cb(Ok(results)),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
        StorageCb::MvccInfoByKey(cb) => match pr {
            ProcessResult::MvccKey { mvcc } => cb(Ok(mvcc)),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
        StorageCb::MvccInfoByStartTs(cb) => match pr {
            ProcessResult::MvccStartTs { mvcc } => cb(Ok(mvcc)),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
        StorageCb::Locks(cb) => match pr {
            ProcessResult::Locks { locks } => cb(Ok(locks)),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
    }
}

/// Task is a running command.
pub struct Task {
    pub cid: u64,
    pub lock: Lock,
    pub tag: &'static str,

    pub cb: Option<StorageCb>,
    pub cmd: Option<Command>,
    ts: u64,
    region_id: u64,
}

impl Task {
    /// Creates a task for a running command.
    pub fn new(cid: u64, cmd: Command, lock: Lock, callback: StorageCb) -> Task {
        Task {
            cid,
            tag: cmd.tag(),
            region_id: cmd.get_context().get_region_id(),
            ts: cmd.ts(),
            cb: Some(callback),
            cmd: Some(cmd),
            lock,
        }
    }

    pub fn cmd(&self) -> &Command {
        &self.cmd.as_ref().unwrap()
    }

    pub fn priority(&self) -> CommandPri {
        self.cmd().priority()
    }

    pub fn context(&self) -> &Context {
        self.cmd().get_context()
    }

    pub fn execute<E: Engine>(
        self,
        cb_ctx: CbContext,
        snapshot: EngineResult<E::Snap>,
        engine: Option<E>,
    ) -> Execution<E> {
        Execution {
            task: self,
            engine,
            state: Some(ExecState::ProcessSnapshot { cb_ctx, snapshot }),
        }
    }

    pub fn fail<E: Engine>(self, err: Error) -> Execution<E> {
        let err = err.into();
        Execution {
            task: self,
            state: Some(ExecState::Done {
                pr: ProcessResult::Failed { err },
                statistics: None,
            }),
            engine: None,
        }
    }
}

enum ExecState<E: Engine> {
    ProcessSnapshot {
        cb_ctx: CbContext,
        snapshot: EngineResult<E::Snap>,
    },
    ProcessRead {
        snapshot: E::Snap,
    },
    ProcessWrite {
        snapshot: E::Snap,
    },
    AsyncWrite {
        fut: Box<Future<Item = ProcessResult, Error = ()> + Send + 'static>,
        statistics: Option<Statistics>,
    },
    Next {
        cmd: Command,
        statistics: Option<Statistics>,
    },
    Done {
        pr: ProcessResult,
        statistics: Option<Statistics>,
    },
}

pub struct Execution<E: Engine> {
    task: Task,
    engine: Option<E>,
    state: Option<ExecState<E>>,
}

impl<E: Engine> Future for Execution<E> {
    type Item = Msg;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let state = self
                .state
                .take()
                .expect("empty exection state, poll after ready");

            match state {
                ExecState::ProcessSnapshot { cb_ctx, snapshot } => {
                    self.process_snapshot(cb_ctx, snapshot);
                }
                ExecState::ProcessRead { snapshot } => {
                    self.process_read(snapshot);
                }
                ExecState::ProcessWrite { snapshot } => {
                    self.process_write(snapshot);
                }
                ExecState::AsyncWrite {
                    mut fut,
                    statistics,
                } => {
                    match fut.poll() {
                        Ok(Async::Ready(ProcessResult::NextCommand { cmd })) => {
                            self.state = Some(ExecState::Next { cmd, statistics });
                        }
                        Ok(Async::Ready(pr)) => {
                            self.state = Some(ExecState::Done { pr, statistics });
                        }
                        Ok(Async::NotReady) => {
                            self.state = Some(ExecState::AsyncWrite { fut, statistics });
                            return Ok(Async::NotReady);
                        }
                        Err(_) => {
                            unimplemented!()
                            // let pr = ProcessResult::Failed { err: e.into() };
                            // return Ok(Async::Ready((pr, None)));
                        }
                    };
                }
                ExecState::Next { cmd, statistics } => {
                    SCHED_STAGE_COUNTER_VEC
                        .with_label_values(&[self.task.tag, "next_cmd"])
                        .inc();
                    return Ok(Async::Ready(Msg::Next {
                        cid: self.task.cid,
                        lock: self.task.lock.clone(),
                        cmd,
                        cb: self.task.cb.take().unwrap(),
                        statistics,
                    }));
                }
                ExecState::Done { pr, statistics } => {
                    execute_callback(self.task.cb.take().unwrap(), pr);
                    return Ok(Async::Ready(Msg::Done {
                        lock: self.task.lock.clone(),
                        cid: self.task.cid,
                        statistics,
                    }));
                }
            }
        }
    }
}

impl<E: Engine> Execution<E> {
    fn process_snapshot(&mut self, cb_ctx: CbContext, snapshot: EngineResult<E::Snap>) {
        match snapshot {
            Ok(snapshot) => {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[self.task.tag, "snapshot_ok"])
                    .inc();
                debug!(
                    "receive snapshot and process for cid={}, cb_ctx={:?}",
                    self.task.cid, cb_ctx
                );

                if let Some(term) = cb_ctx.term {
                    self.task.cmd.as_mut().unwrap().mut_context().set_term(term);
                }

                if self.task.cmd().readonly() {
                    self.state = Some(ExecState::ProcessRead { snapshot });
                } else {
                    self.state = Some(ExecState::ProcessWrite { snapshot });
                }
            }
            Err(err) => {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[self.task.tag, "snapshot_err"])
                    .inc();
                error!(
                    "get snapshot failed for cid={}, error {:?}",
                    self.task.cid, err
                );

                let pr = ProcessResult::Failed { err: err.into() };
                self.state = Some(ExecState::Done {
                    pr,
                    statistics: None,
                });
            }
        }
    }

    /// Processes a read command within a worker thread, then posts `ReadFinished` message back to the
    /// `Scheduler`.
    fn process_read(&mut self, snapshot: E::Snap) {
        fail_point!("txn_before_process_read");
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[self.task.tag, "read_finish"])
            .inc();
        debug!("read command(cid={}) finished", self.task.cid);

        let _timer = SCHED_PROCESSING_READ_HISTOGRAM_VEC
            .with_label_values(&[self.task.tag])
            .start_coarse_timer();
        let region_id = self.task.region_id;
        let ts = self.task.ts;
        let timer = SlowTimer::new();

        debug!("process read cmd(cid={}) in worker pool", self.task.cid);
        let mut statistics = Some(Statistics::default());
        match process_read_impl(
            self.task.cmd.take().unwrap(),
            snapshot,
            statistics.as_mut().unwrap(),
        ) {
            Ok(ProcessResult::NextCommand { cmd }) => {
                self.state = Some(ExecState::Next { cmd, statistics });
            }
            Ok(pr) => self.state = Some(ExecState::Done { pr, statistics }),
            Err(e) => {
                self.state = Some(ExecState::Done {
                    pr: ProcessResult::Failed { err: e.into() },
                    statistics,
                })
            }
        }
        slow_log!(
            timer,
            "[region {}] scheduler handle command: {}, ts: {}",
            region_id,
            self.task.tag,
            ts
        );
    }

    /// Processes a write command within a worker thread, then posts either a `WriteFinished`
    /// message if successful or a `FinishedWithErr` message back to the `Scheduler`.
    fn process_write(&mut self, snapshot: E::Snap) {
        fail_point!("txn_before_process_write");
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[self.task.tag, "write_finish"])
            .inc();
        debug!("write finished for command, cid={}", self.task.cid);

        let _timer = SCHED_PROCESSING_WRITE_HISTOGRAM_VEC
            .with_label_values(&[self.task.tag])
            .start_coarse_timer();
        let timer = SlowTimer::new();

        let region_id = self.task.region_id;
        let ts = self.task.ts;
        let tag = self.task.tag;
        let cid = self.task.cid;

        let mut statistics = Statistics::default();
        match process_write_impl(self.task.cmd.take().unwrap(), snapshot, &mut statistics) {
            // Initiates an async write operation on the storage engine, there'll be a `WriteFinished`
            // message when it finishes.
            Ok((ctx, pr, to_be_write, rows)) => {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[tag, "write"])
                    .inc();
                if to_be_write.is_empty() {
                    self.state = Some(ExecState::Done {
                        pr,
                        statistics: Some(statistics),
                    })
                } else {
                    let (cb, fut) = paired_future_callback();
                    // The callback to receive async results of write prepare from the storage engine.
                    let fut = fut.then(
                        move |res: result::Result<(CbContext, EngineResult<()>), _>| {
                            KV_COMMAND_KEYWRITE_HISTOGRAM_VEC
                                .with_label_values(&[tag])
                                .observe(rows as f64);

                            let pr = match res {
                                Ok((_, Ok(()))) => pr,
                                Ok((_, Err(e))) => ProcessResult::Failed { err: e.into() },
                                Err(e) => ProcessResult::Failed { err: box_err!(e) },
                            };
                            Ok(pr)
                        },
                    );

                    if let Err(e) = self
                        .engine
                        .as_ref()
                        .unwrap()
                        .async_write(&ctx, to_be_write, cb)
                    {
                        SCHED_STAGE_COUNTER_VEC
                            .with_label_values(&[tag, "async_write_err"])
                            .inc();
                        error!("engine async_write failed, cid={}, err={:?}", cid, e);
                        self.state = Some(ExecState::Done {
                            pr: ProcessResult::Failed { err: e.into() },
                            statistics: Some(statistics),
                        });
                    } else {
                        self.state = Some(ExecState::AsyncWrite {
                            fut: Box::new(fut),
                            statistics: Some(statistics),
                        });
                    }
                }
            }
            // Write prepare failure typically means conflicting transactions are detected. Delivers the
            // error to the callback, and releases the latches.
            Err(err) => {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[tag, "prepare_write_err"])
                    .inc();

                debug!("write command(cid={}) failed at prewrite.", cid);
                self.state = Some(ExecState::Done {
                    pr: ProcessResult::Failed { err: err.into() },
                    statistics: Some(statistics),
                })
            }
        };

        slow_log!(
            timer,
            "[region {}] scheduler handle command: {}, ts: {}",
            region_id,
            self.task.tag,
            ts
        );
    }
}

fn process_read_impl<S: Snapshot>(
    mut cmd: Command,
    snapshot: S,
    statistics: &mut Statistics,
) -> Result<ProcessResult> {
    let tag = cmd.tag();
    match cmd {
        Command::MvccByKey { ref ctx, ref key } => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                None,
                None,
                ctx.get_isolation_level(),
            );
            let result = find_mvcc_infos_by_key(&mut reader, key, u64::MAX);
            statistics.add(reader.get_statistics());
            let (lock, writes, values) = result?;
            Ok(ProcessResult::MvccKey {
                mvcc: MvccInfo {
                    lock,
                    writes,
                    values,
                },
            })
        }
        Command::MvccByStartTs { ref ctx, start_ts } => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                None,
                None,
                ctx.get_isolation_level(),
            );
            match reader.seek_ts(start_ts)? {
                Some(key) => {
                    let result = find_mvcc_infos_by_key(&mut reader, &key, u64::MAX);
                    statistics.add(reader.get_statistics());
                    let (lock, writes, values) = result?;
                    Ok(ProcessResult::MvccStartTs {
                        mvcc: Some((
                            key,
                            MvccInfo {
                                lock,
                                writes,
                                values,
                            },
                        )),
                    })
                }
                None => Ok(ProcessResult::MvccStartTs { mvcc: None }),
            }
        }
        // Scans locks with timestamp <= `max_ts`
        Command::ScanLock {
            ref ctx,
            max_ts,
            ref start_key,
            limit,
            ..
        } => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                None,
                None,
                ctx.get_isolation_level(),
            );
            let result = reader.scan_locks(start_key.as_ref(), |lock| lock.ts <= max_ts, limit);
            statistics.add(reader.get_statistics());
            let (kv_pairs, _) = result?;
            let mut locks = Vec::with_capacity(kv_pairs.len());
            for (key, lock) in kv_pairs {
                let mut lock_info = LockInfo::new();
                lock_info.set_primary_lock(lock.primary);
                lock_info.set_lock_version(lock.ts);
                lock_info.set_key(key.into_raw()?);
                locks.push(lock_info);
            }
            KV_COMMAND_KEYREAD_HISTOGRAM_VEC
                .with_label_values(&[tag])
                .observe(locks.len() as f64);
            Ok(ProcessResult::Locks { locks })
        }
        Command::ResolveLock {
            ref ctx,
            ref mut txn_status,
            ref scan_key,
            ..
        } => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                None,
                None,
                ctx.get_isolation_level(),
            );
            let result = reader.scan_locks(
                scan_key.as_ref(),
                |lock| txn_status.contains_key(&lock.ts),
                RESOLVE_LOCK_BATCH_SIZE,
            );
            statistics.add(reader.get_statistics());
            let (kv_pairs, has_remain) = result?;
            KV_COMMAND_KEYREAD_HISTOGRAM_VEC
                .with_label_values(&[tag])
                .observe(kv_pairs.len() as f64);
            if kv_pairs.is_empty() {
                Ok(ProcessResult::Res)
            } else {
                let next_scan_key = if has_remain {
                    // There might be more locks.
                    kv_pairs.last().map(|(k, _lock)| k.clone())
                } else {
                    // All locks are scanned
                    None
                };
                Ok(ProcessResult::NextCommand {
                    cmd: Command::ResolveLock {
                        ctx: ctx.clone(),
                        txn_status: mem::replace(txn_status, Default::default()),
                        scan_key: next_scan_key,
                        key_locks: kv_pairs,
                    },
                })
            }
        }
        _ => panic!("unsupported read command"),
    }
}

fn process_write_impl<S: Snapshot>(
    cmd: Command,
    snapshot: S,
    statistics: &mut Statistics,
) -> Result<(Context, ProcessResult, Vec<Modify>, usize)> {
    let (pr, modifies, rows, ctx) = match cmd {
        Command::Prewrite {
            ctx,
            mutations,
            primary,
            start_ts,
            options,
            ..
        } => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let mut locks = vec![];
            let rows = mutations.len();
            for m in mutations {
                match txn.prewrite(m, &primary, &options) {
                    Ok(_) => {}
                    e @ Err(MvccError::KeyIsLocked { .. }) => {
                        locks.push(e.map_err(Error::from).map_err(StorageError::from));
                    }
                    Err(e) => return Err(Error::from(e)),
                }
            }

            statistics.add(&txn.take_statistics());
            if locks.is_empty() {
                let pr = ProcessResult::MultiRes { results: vec![] };
                let modifies = txn.into_modifies();
                (pr, modifies, rows, ctx)
            } else {
                // Skip write stage if some keys are locked.
                let pr = ProcessResult::MultiRes { results: locks };
                (pr, vec![], 0, ctx)
            }
        }
        Command::Commit {
            ctx,
            keys,
            lock_ts,
            commit_ts,
            ..
        } => {
            if commit_ts <= lock_ts {
                return Err(Error::InvalidTxnTso {
                    start_ts: lock_ts,
                    commit_ts,
                });
            }
            let mut txn = MvccTxn::new(snapshot, lock_ts, !ctx.get_not_fill_cache())?;
            let rows = keys.len();
            for k in keys {
                txn.commit(k, commit_ts)?;
            }

            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), rows, ctx)
        }
        Command::Cleanup {
            ctx, key, start_ts, ..
        } => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            txn.rollback(key)?;

            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), 1, ctx)
        }
        Command::Rollback {
            ctx,
            keys,
            start_ts,
            ..
        } => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let rows = keys.len();
            for k in keys {
                txn.rollback(k)?;
            }

            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), rows, ctx)
        }
        Command::ResolveLock {
            ctx,
            mut txn_status,
            mut scan_key,
            key_locks,
        } => {
            let mut scan_key = scan_key.take();
            let mut modifies: Vec<Modify> = vec![];
            let mut write_size = 0;
            let rows = key_locks.len();
            for (current_key, current_lock) in key_locks {
                let mut txn =
                    MvccTxn::new(snapshot.clone(), current_lock.ts, !ctx.get_not_fill_cache())?;
                let status = txn_status.get(&current_lock.ts);
                let commit_ts = match status {
                    Some(ts) => *ts,
                    None => panic!("txn status {} not found.", current_lock.ts),
                };
                if commit_ts > 0 {
                    if current_lock.ts >= commit_ts {
                        return Err(Error::InvalidTxnTso {
                            start_ts: current_lock.ts,
                            commit_ts,
                        });
                    }
                    txn.commit(current_key.clone(), commit_ts)?;
                } else {
                    txn.rollback(current_key.clone())?;
                }
                write_size += txn.write_size();

                statistics.add(&txn.take_statistics());
                modifies.append(&mut txn.into_modifies());

                if write_size >= MAX_TXN_WRITE_SIZE {
                    scan_key = Some(current_key);
                    break;
                }
            }
            let pr = if scan_key.is_none() {
                ProcessResult::Res
            } else {
                ProcessResult::NextCommand {
                    cmd: Command::ResolveLock {
                        ctx: ctx.clone(),
                        txn_status,
                        scan_key: scan_key.take(),
                        key_locks: vec![],
                    },
                }
            };
            (pr, modifies, rows, ctx)
        }
        Command::Pause { ctx, duration, .. } => {
            thread::sleep(Duration::from_millis(duration));
            (ProcessResult::Res, vec![], 0, ctx)
        }
        _ => panic!("unsupported write command"),
    };

    Ok((ctx, pr, modifies, rows))
}

// Make clippy happy.
type MultipleReturnValue = (Option<MvccLock>, Vec<(u64, Write)>, Vec<(u64, Value)>);

fn find_mvcc_infos_by_key<S: Snapshot>(
    reader: &mut MvccReader<S>,
    key: &Key,
    mut ts: u64,
) -> Result<MultipleReturnValue> {
    let mut writes = vec![];
    let mut values = vec![];
    let lock = reader.load_lock(key)?;
    loop {
        let opt = reader.seek_write(key, ts)?;
        match opt {
            Some((commit_ts, write)) => {
                ts = commit_ts - 1;
                writes.push((commit_ts, write));
            }
            None => break,
        };
    }
    for (ts, v) in reader.scan_values_in_default(key)? {
        values.push((ts, v));
    }
    Ok((lock, writes, values))
}
