// Copyright 2016 PingCAP, Inc.
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

//! Scheduler which schedules the execution of `storage::Command`s.
//!
//! There is one scheduler for each store. It receives commands from clients, executes them against
//! the MVCC layer storage engine.
//!
//! Logically, the data organization hierarchy from bottom to top is row -> region -> store ->
//! database. But each region is replicated onto N stores for reliability, the replicas form a Raft
//! group, one of which acts as the leader. When the client read or write a row, the command is
//! sent to the scheduler which is on the region leader's store.
//!
//! Scheduler runs in a single-thread event loop, but command executions are delegated to a pool of
//! worker thread.
//!
//! Scheduler keeps track of all the running commands and uses latches to ensure serialized access
//! to the overlapping rows involved in concurrent commands. But note that scheduler only ensures
//! serialized access to the overlapping rows at command level, but a transaction may consist of
//! multiple commands, therefore conflicts may happen at transaction level. Transaction semantics
//! is ensured by the transaction protocol implemented in the client library, which is transparent
//! to the scheduler.

use std::fmt::{self, Debug, Display, Formatter};
use std::time::Duration;
use std::u64;

use futures::Future;
use kvproto::kvrpcpb::CommandPri;
use prometheus::HistogramTimer;

use storage::engine::Statistics;
use storage::Key;
use storage::{Command, Engine, Error as StorageError, StorageCb};
use util::collections::HashMap;
use util::config::ReadableSize;
use util::future::paired_future_callback;
use util::futurepool::FuturePool;
use util::worker::{self, Runnable};

use super::super::metrics::*;
use super::latch::{Latches, Lock};
use super::process::{execute_callback, ProcessResult, Task};
use super::Error;

pub const CMD_BATCH_SIZE: usize = 256;

/// Message types for the scheduler event loop.
pub enum Msg {
    Quit,
    RawCmd {
        cmd: Command,
        cb: StorageCb,
    },
    Done {
        cid: u64,
        lock: Lock,
        statistics: Option<Statistics>,
    },
    Next {
        cid: u64,
        lock: Lock,
        cmd: Command,
        cb: StorageCb,
        statistics: Option<Statistics>,
    },
}

/// Debug for messages.
impl Debug for Msg {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// Display for messages.
impl Display for Msg {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Msg::Quit => write!(f, "Quit"),
            Msg::RawCmd { ref cmd, .. } => write!(f, "RawCmd {:?}", cmd),
            Msg::Done { cid, .. } => write!(f, "Done [cid={}]", cid),
            Msg::Next { cid, .. } => write!(f, "WriteFinished [cid={}]", cid),
        }
    }
}

// It stores context of a task.
struct TaskContext {
    write_bytes: usize,
    // How long it waits on latches.
    latch_timer: Option<HistogramTimer>,
    // Total duration of a command.
    _cmd_timer: HistogramTimer,
}

impl TaskContext {
    fn new(is_write_lock: bool, cmd: &Command) -> TaskContext {
        let write_bytes =
            if is_write_lock { cmd.write_bytes() } else { 0 };

        TaskContext {
            write_bytes,
            latch_timer: Some(
                SCHED_LATCH_HISTOGRAM_VEC
                    .with_label_values(&[cmd.tag()])
                    .start_coarse_timer(),
            ),
            _cmd_timer: SCHED_HISTOGRAM_VEC
                .with_label_values(&[cmd.tag()])
                .start_coarse_timer(),
        }
    }

    fn on_schedule(&mut self) {
        self.latch_timer.take();
    }
}

/// Scheduler which schedules the execution of `storage::Command`s.
pub struct Scheduler<E: Engine> {
    engine: E,

    // cid -> Task
    pending_tasks: HashMap<u64, Task>,

    // cid -> TaskContext
    task_contexts: HashMap<u64, TaskContext>,

    // actual scheduler to schedule the execution of commands
    scheduler: worker::Scheduler<Msg>,

    // cmd id generator
    id_alloc: u64,

    // write concurrency control
    latches: Latches,

    // TODO: Dynamically calculate this value according to processing
    // speed of recent write requests.
    sched_pending_write_threshold: usize,

    // worker pool
    worker_pool: FuturePool<()>,

    // high priority commands will be delivered to this pool
    high_priority_pool: FuturePool<()>,

    // used to control write flow
    running_write_bytes: usize,
}

impl<E: Engine> Scheduler<E> {
    /// Creates a scheduler.
    pub fn new(
        engine: E,
        scheduler: worker::Scheduler<Msg>,
        concurrency: usize,
        worker_pool_size: usize,
        sched_pending_write_threshold: usize,
    ) -> Self {
        Scheduler {
            engine,
            // TODO: GC these two maps.
            pending_tasks: Default::default(),
            task_contexts: Default::default(),
            scheduler,
            id_alloc: 0,
            latches: Latches::new(concurrency),
            sched_pending_write_threshold,
            worker_pool: FuturePool::new(
                worker_pool_size,
                ReadableSize::mb(10).0 as _,
                &thd_name!("sched-worker-pool"),
                Duration::from_secs(15),
                || {},
            ),
            high_priority_pool: FuturePool::new(
                worker_pool_size,
                ReadableSize::mb(10).0 as _,
                &thd_name!("sched-high-pri-pool"),
                Duration::from_secs(15),
                || {},
            ),
            running_write_bytes: 0,
        }
    }

    /// Generates the next command ID.
    fn gen_id(&mut self) -> u64 {
        self.id_alloc += 1;
        self.id_alloc
    }

    fn dequeue_task(&mut self, cid: u64) -> Task {
        let task = self.pending_tasks.remove(&cid).unwrap();
        assert_eq!(task.cid, cid);
        task
    }

    fn enqueue_task(&mut self, task: Task) {
        let cid = task.cid;

        let tctx = {
            let cmd = task.cmd();
            let is_write_lock = task.lock.is_write_lock();
            TaskContext::new(is_write_lock, cmd)
        };

        self.running_write_bytes += tctx.write_bytes;
        SCHED_WRITING_BYTES_GAUGE.set(self.running_write_bytes as i64);

        if self.pending_tasks.insert(cid, task).is_some() {
            panic!("command cid={} shouldn't exist", cid);
        }
        SCHED_CONTEX_GAUGE.set(self.pending_tasks.len() as i64);
        if self.task_contexts.insert(cid, tctx).is_some() {
            panic!("TaskContext cid={} shouldn't exist", cid);
        }
    }

    fn dequeue_task_context(&mut self, cid: u64) -> TaskContext {
        let tctx = self.task_contexts.remove(&cid).unwrap();

        self.running_write_bytes -= tctx.write_bytes;
        SCHED_WRITING_BYTES_GAUGE.set(self.running_write_bytes as i64);
        SCHED_CONTEX_GAUGE.set(self.pending_tasks.len() as i64);

        tctx
    }

    pub fn fetch_pool(&self, priority: CommandPri) -> &FuturePool<()> {
        match priority {
            CommandPri::Low | CommandPri::Normal => &self.worker_pool,
            CommandPri::High => &self.high_priority_pool,
        }
    }

    /// Event handler for new command.
    ///
    /// This method will try to acquire all the necessary latches. If all the necessary latches are
    /// acquired, the method initiates a get snapshot operation for furthur processing; otherwise,
    /// the method adds the command to the waiting queue(s). The command will be handled later in
    /// `try_to_wake_up` when its turn comes.
    ///
    /// Note that once a command is ready to execute, the snapshot is always up-to-date during the
    /// execution because 1) all the conflicting commands (if any) must be in the waiting queues;
    /// 2) there may be non-conflicitng commands running concurrently, but it doesn't matter.
    fn schedule_command(&mut self, cmd: Command, callback: StorageCb) {
        let cid = self.gen_id();
        debug!("received new command, cid={}, cmd={}", cid, cmd);

        let tag = cmd.tag();
        let priority_tag = cmd.priority_tag();
        let lock = self.gen_lock(&cmd);
        let task = Task::new(cid, cmd, lock, callback);
        // TODO: enqueue_task should return an reference of the tctx.
        self.enqueue_task(task);
        self.try_to_wake_up(cid);

        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[tag, "new"])
            .inc();
        SCHED_COMMANDS_PRI_COUNTER_VEC
            .with_label_values(&[priority_tag])
            .inc();
    }

    /// Tries to acquire all the necessary latches. If all the necessary latches are acquired,
    /// the method initiates a get snapshot operation for furthur processing.
    fn try_to_wake_up(&mut self, cid: u64) {
        let wake = if let Some(tctx) = self.acquire_lock(cid) {
            tctx.on_schedule();
            true
        } else {
            false
        };
        if wake {
            self.get_snapshot(cid);
        }
    }

    fn too_busy(&self) -> bool {
        fail_point!("txn_scheduler_busy", |_| true);
        self.running_write_bytes >= self.sched_pending_write_threshold
    }

    fn on_receive_new_cmd(&mut self, cmd: Command, callback: StorageCb) {
        // write flow control
        if cmd.need_flow_control() && self.too_busy() {
            SCHED_TOO_BUSY_COUNTER_VEC
                .with_label_values(&[cmd.tag()])
                .inc();
            execute_callback(
                callback,
                ProcessResult::Failed {
                    err: StorageError::SchedTooBusy,
                },
            );
            return;
        }
        self.schedule_command(cmd, callback);
    }

    /// Initiates an async operation to get a snapshot from the storage engine, then posts a
    /// `SnapshotFinished` message back to the event loop when it finishes.
    fn get_snapshot(&mut self, cid: u64) {
        let mut task = self.dequeue_task(cid);
        let tag = task.tag;
        let ctx = task.context().clone();

        let (cb, fut) = paired_future_callback();
        if let Err(e) = self.engine.async_snapshot(&ctx, cb) {
            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[tag, "async_snapshot_err"])
                .inc();

            error!(
                "command cid={}, engine async_snapshot failed, err: {:?}",
                cid, e
            );

            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[tag, "error"])
                .inc();

            let pr = ProcessResult::Failed {
                err: StorageError::from(e),
            };
            execute_callback(task.cb.take().unwrap(), pr);

            self.dequeue_task_context(cid);
            self.release_lock(&task.lock, cid);
        } else {
            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[tag, "snapshot"])
                .inc();

            let pool = self.fetch_pool(task.priority());
            let scheduler = self.scheduler.clone();
            let engine = Some(self.engine.clone());
            pool.spawn(move |_| {
                fut.then(move |res| {
                    fail_point!("scheduler_async_snapshot_finish");
                    match res {
                        Ok((cb_ctx, snapshot)) => task.execute(cb_ctx, snapshot, engine),
                        Err(err) => task.fail(Error::Other(box_err!(err))),
                    }
                }).map(move |msg| notify_scheduler(&scheduler, msg))
            }).forget()
        }
    }

    /// Generates the lock for a command.
    ///
    /// Basically, read-only commands require no latches, write commands require latches hashed
    /// by the referenced keys.
    fn gen_lock(&self, cmd: &Command) -> Lock {
        gen_command_lock(&self.latches, cmd)
    }

    /// Tries to acquire all the required latches for a command.
    ///
    /// Returns `Some(TaskContext)` if successful; returns `None` otherwise.
    fn acquire_lock(&mut self, cid: u64) -> Option<&mut TaskContext> {
        let task = self.pending_tasks.get_mut(&cid).unwrap();
        if self.latches.acquire(&mut task.lock, cid) {
            Some(self.task_contexts.get_mut(&cid).unwrap())
        } else {
            None
        }
    }

    /// Releases all the latches held by a command.
    fn release_lock(&mut self, lock: &Lock, cid: u64) {
        let wakeup_list = self.latches.release(lock, cid);
        for wcid in wakeup_list {
            self.try_to_wake_up(wcid);
        }
    }
}

impl<E: Engine> Runnable<Msg> for Scheduler<E> {
    fn run_batch(&mut self, msgs: &mut Vec<Msg>) {
        for msg in msgs.drain(..) {
            match msg {
                Msg::Quit => {
                    // pools are closed by dropping.
                    info!("scheduler stopped");
                    return;
                }
                Msg::RawCmd { cmd, cb } => self.on_receive_new_cmd(cmd, cb),
                Msg::Done { cid, lock, .. } => {
                    self.dequeue_task_context(cid);
                    self.release_lock(&lock, cid);
                }
                Msg::Next {
                    cid, lock, cmd, cb, ..
                } => {
                    self.release_lock(&lock, cid);
                    self.schedule_command(cmd, cb);
                }
            }
        }
    }
}

fn gen_command_lock(latches: &Latches, cmd: &Command) -> Lock {
    match *cmd {
        Command::Prewrite { ref mutations, .. } => {
            let keys: Vec<&Key> = mutations.iter().map(|x| x.key()).collect();
            latches.gen_lock(&keys)
        }
        Command::ResolveLock { ref key_locks, .. } => {
            let keys: Vec<&Key> = key_locks.iter().map(|x| &x.0).collect();
            latches.gen_lock(&keys)
        }
        Command::Commit { ref keys, .. } | Command::Rollback { ref keys, .. } => {
            latches.gen_lock(keys)
        }
        Command::Cleanup { ref key, .. } => latches.gen_lock(&[key]),
        Command::Pause { ref keys, .. } => latches.gen_lock(keys),
        _ => Lock::new(vec![]),
    }
}

fn notify_scheduler(scheduler: &worker::Scheduler<Msg>, msg: Msg) {
    match scheduler.schedule(msg) {
        Ok(_) => (),
        e @ Err(worker::ScheduleError::Stopped(_)) => {
            info!("scheduler stopped, {:?}", e);
        }
        Err(e) => {
            panic!("schedule msg failed, err:{:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvproto::kvrpcpb::Context;
    use storage::mvcc;
    use storage::txn::latch::*;
    use storage::{Command, Key, Mutation, Options};
    use util::collections::HashMap;

    #[test]
    fn test_command_latches() {
        let mut temp_map = HashMap::default();
        temp_map.insert(10, 20);
        let readonly_cmds = vec![
            Command::ScanLock {
                ctx: Context::new(),
                max_ts: 5,
                start_key: None,
                limit: 0,
            },
            Command::ResolveLock {
                ctx: Context::new(),
                txn_status: temp_map.clone(),
                scan_key: None,
                key_locks: vec![],
            },
            Command::MvccByKey {
                ctx: Context::new(),
                key: Key::from_raw(b"k"),
            },
            Command::MvccByStartTs {
                ctx: Context::new(),
                start_ts: 25,
            },
        ];
        let write_cmds = vec![
            Command::Prewrite {
                ctx: Context::new(),
                mutations: vec![Mutation::Put((Key::from_raw(b"k"), b"v".to_vec()))],
                primary: b"k".to_vec(),
                start_ts: 10,
                options: Options::default(),
            },
            Command::Commit {
                ctx: Context::new(),
                keys: vec![Key::from_raw(b"k")],
                lock_ts: 10,
                commit_ts: 20,
            },
            Command::Cleanup {
                ctx: Context::new(),
                key: Key::from_raw(b"k"),
                start_ts: 10,
            },
            Command::Rollback {
                ctx: Context::new(),
                keys: vec![Key::from_raw(b"k")],
                start_ts: 10,
            },
            Command::ResolveLock {
                ctx: Context::new(),
                txn_status: temp_map.clone(),
                scan_key: None,
                key_locks: vec![(
                    Key::from_raw(b"k"),
                    mvcc::Lock::new(mvcc::LockType::Put, b"k".to_vec(), 10, 20, None),
                )],
            },
        ];

        let mut latches = Latches::new(1024);

        let write_locks: Vec<Lock> = write_cmds
            .into_iter()
            .enumerate()
            .map(|(id, cmd)| {
                let mut lock = gen_command_lock(&latches, &cmd);
                assert_eq!(latches.acquire(&mut lock, id as u64), id == 0);
                lock
            })
            .collect();

        for (id, cmd) in readonly_cmds.iter().enumerate() {
            let mut lock = gen_command_lock(&latches, cmd);
            assert!(latches.acquire(&mut lock, id as u64));
        }

        // acquire/release locks one by one.
        let max_id = write_locks.len() as u64 - 1;
        for (id, mut lock) in write_locks.into_iter().enumerate() {
            let id = id as u64;
            if id != 0 {
                assert!(latches.acquire(&mut lock, id));
            }
            let unlocked = latches.release(&lock, id);
            if id as u64 == max_id {
                assert!(unlocked.is_empty());
            } else {
                assert_eq!(unlocked, vec![id + 1]);
            }
        }
    }
}
