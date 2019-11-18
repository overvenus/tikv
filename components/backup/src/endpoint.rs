// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::cmp;
use std::sync::*;
use std::time::*;

use engine::DB;
use tikv::storage::kv::{Engine, RegionInfoProvider};
use tikv_util::timer::Timer;
use tikv_util::worker::{Runnable, RunnableWithTimer};
use tokio_threadpool::{Builder as ThreadPoolBuilder, ThreadPool};

use crate::metrics::*;
use crate::task::Task;

// if thread pool has been idle for such long time, we will shutdown it.
const IDLE_THREADPOOL_DURATION: u64 = 30 * 60 * 1000; // 30 mins

struct ControlThreadPool {
    size: usize,
    workers: Option<ThreadPool>,
    last_active: Instant,
}

impl ControlThreadPool {
    fn new() -> Self {
        ControlThreadPool {
            size: 0,
            workers: None,
            last_active: Instant::now(),
        }
    }

    fn workers(&self) -> &ThreadPool {
        self.workers.as_ref().unwrap()
    }

    /// Lazily adjust the thread pool's size
    ///
    /// Resizing if the thread pool need to expend or there
    /// are too many idle threads. Otherwise do nothing.
    fn adjust_with(&mut self, new_size: usize) {
        if self.size >= new_size && self.size - new_size <= 10 {
            return;
        }
        let workers = ThreadPoolBuilder::new()
            .name_prefix("backup-worker")
            .pool_size(new_size)
            .build();
        let _ = self.workers.replace(workers);
        self.size = new_size;
        BACKUP_THREAD_POOL_SIZE_GAUGE.set(new_size as i64);
    }

    fn heartbeat(&mut self) {
        self.last_active = Instant::now();
    }

    /// Shutdown the thread pool if it has been idle for a long time.
    fn check_active(&mut self, idle_threshold: Duration) {
        if self.last_active.elapsed() >= idle_threshold {
            self.size = 0;
            if let Some(w) = self.workers.take() {
                w.shutdown();
            }
        }
    }
}

/// The endpoint of backup.
///
/// It coordinates backup tasks and dispatches them to different workers.
pub struct Endpoint<E: Engine, R: RegionInfoProvider> {
    store_id: u64,
    pool: RefCell<ControlThreadPool>,
    pool_idle_threshold: u64,
    db: Arc<DB>,

    pub(crate) engine: E,
    pub(crate) region_info: R,
}

impl<E: Engine, R: RegionInfoProvider> Endpoint<E, R> {
    pub fn new(store_id: u64, engine: E, region_info: R, db: Arc<DB>) -> Endpoint<E, R> {
        Endpoint {
            store_id,
            engine,
            region_info,
            pool: RefCell::new(ControlThreadPool::new()),
            pool_idle_threshold: IDLE_THREADPOOL_DURATION,
            db,
        }
    }

    pub fn new_timer(&self) -> Timer<()> {
        let mut timer = Timer::new(1);
        timer.add_task(Duration::from_millis(self.pool_idle_threshold), ());
        timer
    }

    pub fn handle_backup_task(&self, task: Task) {
        let task_id = task.id;
        let concurrency = cmp::max(1, task.concurrency) as usize;
        self.pool.borrow_mut().adjust_with(concurrency);
        if let Err(e) = task.spawn_backup_worker(
            self.pool.borrow().workers(),
            self.store_id,
            self.db.clone(),
            &self.engine,
            &self.region_info,
        ) {
            error!("handle backup task failed"; "error" => ?e, "task" => ?task_id);
        }
    }
}

impl<E: Engine, R: RegionInfoProvider> Runnable<Task> for Endpoint<E, R> {
    fn run(&mut self, task: Task) {
        if task.has_canceled() {
            warn!("backup task has canceled"; "task" => %task);
            return;
        }
        info!("run backup task"; "task" => %task);
        if task.start_ts == task.end_ts {
            self.handle_backup_task(task);
            self.pool.borrow_mut().heartbeat();
        } else {
            // TODO: support incremental backup
            BACKUP_RANGE_ERROR_VEC
                .with_label_values(&["incremental"])
                .inc();
            error!("incremental backup is not supported yet");
        }
    }
}

impl<E: Engine, R: RegionInfoProvider> RunnableWithTimer<Task, ()> for Endpoint<E, R> {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        let pool_idle_duration = Duration::from_millis(self.pool_idle_threshold);
        self.pool
            .borrow_mut()
            .check_active(pool_idle_duration.clone());
        timer.add_task(pool_idle_duration, ());
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::atomic::*;
    use std::thread;

    use futures::sync::mpsc::*;
    use futures::{self, Future, Stream};
    use kvproto::backup::*;
    use kvproto::metapb;
    use raft::StateRole;
    use rand;
    use tempfile::TempDir;
    use tikv::raftstore::coprocessor::RegionCollector;
    use tikv::raftstore::coprocessor::SeekRegionCallback;
    use tikv::raftstore::store::util::new_peer;
    use tikv::storage::kv::Result as EngineResult;
    use tikv::storage::mvcc::tests::*;
    use tikv::storage::Key;
    use tikv::storage::SHORT_VALUE_MAX_LEN;
    use tikv::storage::{RocksEngine, TestEngineBuilder};
    use tikv_util::time::Instant;

    use crate::task::{Progress, TaskID};

    use super::*;

    #[derive(Clone)]
    pub struct MockRegionInfoProvider {
        regions: Arc<Mutex<RegionCollector>>,
        cancel: Option<Arc<AtomicBool>>,
    }
    impl MockRegionInfoProvider {
        pub fn new() -> Self {
            MockRegionInfoProvider {
                regions: Arc::new(Mutex::new(RegionCollector::new())),
                cancel: None,
            }
        }
        pub fn set_regions(&self, regions: Vec<(Vec<u8>, Vec<u8>, u64)>) {
            let mut map = self.regions.lock().unwrap();
            for (mut start_key, mut end_key, id) in regions {
                if !start_key.is_empty() {
                    start_key = Key::from_raw(&start_key).into_encoded();
                }
                if !end_key.is_empty() {
                    end_key = Key::from_raw(&end_key).into_encoded();
                }
                let mut r = metapb::Region::default();
                r.set_id(id);
                r.set_start_key(start_key.clone());
                r.set_end_key(end_key);
                r.mut_peers().push(new_peer(1, 1));
                map.create_region(r, StateRole::Leader);
            }
        }
        fn canecl_on_seek(&mut self, cancel: Arc<AtomicBool>) {
            self.cancel = Some(cancel);
        }
    }
    impl RegionInfoProvider for MockRegionInfoProvider {
        fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> EngineResult<()> {
            let from = from.to_vec();
            let regions = self.regions.lock().unwrap();
            if let Some(c) = self.cancel.as_ref() {
                c.store(true, Ordering::SeqCst);
            }
            regions.handle_seek_region(from, callback);
            Ok(())
        }
    }

    pub fn new_endpoint() -> (TempDir, Endpoint<RocksEngine, MockRegionInfoProvider>) {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(&[engine::CF_DEFAULT, engine::CF_LOCK, engine::CF_WRITE])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();
        (
            temp,
            Endpoint::new(1, rocks, MockRegionInfoProvider::new(), db),
        )
    }

    pub fn check_response<F>(rx: UnboundedReceiver<BackupResponse>, check: F)
    where
        F: FnOnce(Option<BackupResponse>),
    {
        let (resp, rx) = rx.into_future().wait().unwrap();
        check(resp);
        let (none, _rx) = rx.into_future().wait().unwrap();
        assert!(none.is_none(), "{:?}", none);
    }

    #[test]
    fn test_seek_range() {
        let (_tmp, endpoint) = new_endpoint();

        endpoint.region_info.set_regions(vec![
            (b"".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"9".to_vec(), 4),
            (b"9".to_vec(), b"".to_vec(), 5),
        ]);
        // Test seek backup range.
        let test_seek_backup_range =
            |start_key: &[u8], end_key: &[u8], expect: Vec<(&[u8], &[u8])>| {
                let start_key = if start_key.is_empty() {
                    None
                } else {
                    Some(Key::from_raw(start_key))
                };
                let end_key = if end_key.is_empty() {
                    None
                } else {
                    Some(Key::from_raw(end_key))
                };
                let mut prs = Progress::new(
                    TaskID::default(),
                    endpoint.store_id,
                    start_key,
                    end_key,
                    endpoint.region_info.clone(),
                );

                let mut ranges = Vec::with_capacity(expect.len());
                while ranges.len() != expect.len() {
                    let n = (rand::random::<usize>() % 3) + 1;
                    let mut r = prs.forward(n);
                    // The returned backup ranges should <= n
                    assert!(r.len() <= n);

                    if r.is_empty() {
                        // if return a empty vec then the progress is finished
                        assert_eq!(
                            ranges.len(),
                            expect.len(),
                            "got {:?}, expect {:?}",
                            ranges,
                            expect
                        );
                    }
                    ranges.append(&mut r);
                }

                for (a, b) in ranges.into_iter().zip(expect) {
                    assert_eq!(
                        a.start_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                        b.0
                    );
                    assert_eq!(
                        a.end_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                        b.1
                    );
                }
            };

        // Test whether responses contain correct range.
        #[allow(clippy::block_in_if_condition_stmt)]
        let test_handle_backup_task_range =
            |start_key: &[u8], end_key: &[u8], expect: Vec<(&[u8], &[u8])>| {
                let (tx, rx) = unbounded();
                let mut request = BackupRequest::default();
                request.set_start_key(start_key.to_vec());
                request.set_end_key(end_key.to_vec());
                request.set_concurrency(4);
                request.set_start_version(1);
                request.set_end_version(1);
                request.set_path("noop:///tmp/foo".to_owned());
                let (task, _) = Task::new(request, tx).unwrap();
                endpoint.handle_backup_task(task);
                let resps: Vec<_> = rx.collect().wait().unwrap();
                for a in &resps {
                    assert!(
                        expect
                            .iter()
                            .any(|b| { a.get_start_key() == b.0 && a.get_end_key() == b.1 }),
                        "{:?} {:?}",
                        resps,
                        expect
                    );
                }
                assert_eq!(resps.len(), expect.len());
            };

        // Backup range from case.0 to case.1,
        // the case.2 is the expected results.
        type Case<'a> = (&'a [u8], &'a [u8], Vec<(&'a [u8], &'a [u8])>);

        let case: Vec<Case> = vec![
            (b"", b"1", vec![(b"", b"1")]),
            (b"", b"2", vec![(b"", b"1"), (b"1", b"2")]),
            (b"1", b"2", vec![(b"1", b"2")]),
            (b"1", b"3", vec![(b"1", b"2")]),
            (b"1", b"4", vec![(b"1", b"2"), (b"3", b"4")]),
            (b"4", b"6", vec![]),
            (b"4", b"5", vec![]),
            (b"2", b"7", vec![(b"3", b"4")]),
            (b"7", b"8", vec![(b"7", b"8")]),
            (b"3", b"", vec![(b"3", b"4"), (b"7", b"9"), (b"9", b"")]),
            (b"5", b"", vec![(b"7", b"9"), (b"9", b"")]),
            (b"7", b"", vec![(b"7", b"9"), (b"9", b"")]),
            (b"8", b"91", vec![(b"8", b"9"), (b"9", b"91")]),
            (b"8", b"", vec![(b"8", b"9"), (b"9", b"")]),
            (
                b"",
                b"",
                vec![
                    (b"", b"1"),
                    (b"1", b"2"),
                    (b"3", b"4"),
                    (b"7", b"9"),
                    (b"9", b""),
                ],
            ),
        ];
        for (start_key, end_key, ranges) in case {
            test_seek_backup_range(start_key, end_key, ranges.clone());
            test_handle_backup_task_range(start_key, end_key, ranges);
        }
    }

    #[test]
    fn test_handle_backup_task() {
        let (tmp, endpoint) = new_endpoint();
        let engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts = 1;
        let mut alloc_ts = || {
            ts += 1;
            ts
        };
        let mut backup_tss = vec![];
        // Multi-versions for key 0..9.
        for len in &[SHORT_VALUE_MAX_LEN - 1, SHORT_VALUE_MAX_LEN * 2] {
            for i in 0..10u8 {
                let start = alloc_ts();
                let commit = alloc_ts();
                let key = format!("{}", i);
                must_prewrite_put(
                    &engine,
                    key.as_bytes(),
                    &vec![i; *len],
                    key.as_bytes(),
                    start,
                );
                must_commit(&engine, key.as_bytes(), start, commit);
                backup_tss.push((alloc_ts(), len));
            }
        }

        // TODO: check key number for each snapshot.
        for (ts, len) in backup_tss {
            let mut req = BackupRequest::new();
            req.set_start_key(vec![]);
            req.set_end_key(vec![b'5']);
            req.set_start_version(ts);
            req.set_end_version(ts);
            req.set_concurrency(4);
            let (tx, rx) = unbounded();
            // Empty path should return an error.
            Task::new(req.clone(), tx.clone()).unwrap_err();

            // Set an unique path to avoid AlreadyExists error.
            req.set_path(format!(
                "local://{}",
                tmp.path().join(format!("{}", ts)).display()
            ));
            if len % 2 == 0 {
                // Use rate limte
                req.set_rate_limit(10 * 1024 * 1024 /* 10 MB/s */);
            }
            let (task, _) = Task::new(req, tx).unwrap();
            endpoint.handle_backup_task(task);
            let (resp, rx) = rx.into_future().wait().unwrap();
            let resp = resp.unwrap();
            assert!(!resp.has_error(), "{:?}", resp);
            let file_len = if *len <= SHORT_VALUE_MAX_LEN { 1 } else { 2 };
            assert_eq!(
                resp.get_files().len(),
                file_len, /* default and write */
                "{:?}",
                resp
            );
            let (none, _rx) = rx.into_future().wait().unwrap();
            assert!(none.is_none(), "{:?}", none);
        }
    }

    #[test]
    fn test_scan_error() {
        let (tmp, endpoint) = new_endpoint();
        let engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts = 1;
        let mut alloc_ts = || {
            ts += 1;
            ts
        };
        let start = alloc_ts();
        let key = format!("{}", start);
        must_prewrite_put(
            &engine,
            key.as_bytes(),
            key.as_bytes(),
            key.as_bytes(),
            start,
        );

        let now = alloc_ts();
        let mut req = BackupRequest::new();
        req.set_start_key(vec![]);
        req.set_end_key(vec![b'5']);
        req.set_start_version(now);
        req.set_end_version(now);
        req.set_concurrency(4);
        // Set an unique path to avoid AlreadyExists error.
        req.set_path(format!(
            "local://{}",
            tmp.path().join(format!("{}", now)).display()
        ));
        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req.clone(), tx).unwrap();
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_kv_error(), "{:?}", resp);
            assert!(resp.get_error().get_kv_error().has_locked(), "{:?}", resp);
            assert_eq!(resp.get_files().len(), 0, "{:?}", resp);
        });

        // Commit the perwrite.
        let commit = alloc_ts();
        must_commit(&engine, key.as_bytes(), start, commit);

        // Test whether it can correctly convert not leader to region error.
        engine.trigger_not_leader();
        let now = alloc_ts();
        req.set_start_version(now);
        req.set_end_version(now);
        // Set an unique path to avoid AlreadyExists error.
        req.set_path(format!(
            "local://{}",
            tmp.path().join(format!("{}", now)).display()
        ));
        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req.clone(), tx).unwrap();
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_region_error(), "{:?}", resp);
            assert!(
                resp.get_error().get_region_error().has_not_leader(),
                "{:?}",
                resp
            );
        });
    }

    #[test]
    fn test_cancel() {
        let (temp, mut endpoint) = new_endpoint();
        let engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts = 1;
        let mut alloc_ts = || {
            ts += 1;
            ts
        };
        let start = alloc_ts();
        let key = format!("{}", start);
        must_prewrite_put(
            &engine,
            key.as_bytes(),
            key.as_bytes(),
            key.as_bytes(),
            start,
        );
        // Commit the perwrite.
        let commit = alloc_ts();
        must_commit(&engine, key.as_bytes(), start, commit);

        let now = alloc_ts();
        let mut req = BackupRequest::new();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(now);
        req.set_end_version(now);
        req.set_concurrency(4);
        req.set_path(format!("local://{}", temp.path().display()));

        // Cancel the task before starting the task.
        let (tx, rx) = unbounded();
        let (task, cancel) = Task::new(req.clone(), tx).unwrap();
        // Cancel the task.
        cancel.store(true, Ordering::SeqCst);
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            assert!(resp.is_none());
        });

        // Cancel the task during backup.
        let (tx, rx) = unbounded();
        let (task, cancel) = Task::new(req.clone(), tx).unwrap();
        endpoint.region_info.canecl_on_seek(cancel);
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            assert!(resp.is_none());
        });
    }

    #[test]
    fn test_busy() {
        let (_tmp, endpoint) = new_endpoint();
        let engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut req = BackupRequest::new();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(1);
        req.set_end_version(1);
        req.set_concurrency(4);
        req.set_path("noop://foo".to_owned());

        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req.clone(), tx).unwrap();
        // Pause the engine 6 seconds to trigger Timeout error.
        // The Timeout error is translated to server is busy.
        engine.pause(Duration::from_secs(6));
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_region_error(), "{:?}", resp);
            assert!(
                resp.get_error().get_region_error().has_server_is_busy(),
                "{:?}",
                resp
            );
        });
    }

    #[test]
    fn test_adjust_thread_pool_size() {
        let (_tmp, endpoint) = new_endpoint();
        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"".to_vec(), 1)]);

        let mut req = BackupRequest::new();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(1);
        req.set_end_version(1);
        req.set_path("noop://foo".to_owned());

        let (tx, _) = unbounded();

        // at lease spwan one thread
        req.set_concurrency(1);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size == 1);

        // expand thread pool is needed
        req.set_concurrency(15);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size == 15);

        // shrink thread pool only if there are too many idle threads
        req.set_concurrency(10);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size == 15);

        req.set_concurrency(3);
        let (task, _) = Task::new(req, tx).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size == 3);
    }

    #[test]
    fn test_thread_pool_shutdown_when_idle() {
        let (_, mut endpoint) = new_endpoint();

        // set the idle threshold to 100ms
        endpoint.pool_idle_threshold = 100;
        let mut backup_timer = endpoint.new_timer();
        let endpoint = Arc::new(Mutex::new(endpoint));
        let scheduler = {
            let endpoint = endpoint.clone();
            let (tx, rx) = tikv_util::mpsc::unbounded();
            thread::spawn(move || loop {
                let tick_time = backup_timer.next_timeout().unwrap();
                let timeout = tick_time.checked_sub(Instant::now()).unwrap_or_default();
                let task = match rx.recv_timeout(timeout) {
                    Ok(Some(task)) => Some(task),
                    _ => None,
                };
                if let Some(task) = task {
                    let mut endpoint = endpoint.lock().unwrap();
                    endpoint.run(task);
                }
                endpoint.lock().unwrap().on_timeout(&mut backup_timer, ());
            });
            tx
        };

        let mut req = BackupRequest::new();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(1);
        req.set_end_version(1);
        req.set_concurrency(10);
        req.set_path("noop://foo".to_owned());

        let (tx, _) = futures::sync::mpsc::unbounded();
        let (task, _) = Task::new(req, tx).unwrap();

        // if not task arrive after create the thread pool is empty
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 0);

        scheduler.send(Some(task)).unwrap();
        // wait the task send to worker
        thread::sleep(Duration::from_millis(10));
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 10);

        // thread pool not yet shutdown
        thread::sleep(Duration::from_millis(50));
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 10);

        // thread pool shutdown if not task arrive for 100ms
        thread::sleep(Duration::from_millis(50));
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 0);
    }
    // TODO: region err in txn(engine(request))
}
