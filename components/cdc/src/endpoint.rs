use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use engine_rocks::RocksEngine;
use futures::future::{lazy, Future};
use kvproto::cdcpb::*;
use kvproto::kvrpcpb::IsolationLevel;
use kvproto::metapb::Region;
use pd_client::PdClient;
use resolved_ts::Resolver;
use tikv::raftstore::coprocessor::CmdBatch;
use tikv::raftstore::store::fsm::{ApplyRouter, ApplyTask, ChangeCmd};
use tikv::raftstore::store::msg::{Callback, ReadResponse};
use tikv::raftstore::store::RegionSnapshot;
use tikv::storage::kv::Snapshot;
use tikv::storage::mvcc::reader::{DeltaScanner, ScannerBuilder};
use tikv::storage::txn::TxnEntry;
use tikv::storage::txn::TxnEntryScanner;
use tikv_util::collections::HashMap;
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::{Runnable, Scheduler};
use tokio_threadpool::{Builder, Sender as PoolSender, ThreadPool};
use txn_types::{Key, Lock, TimeStamp};

use crate::delegate::{Delegate, Downstream};
use crate::{CdcObserver, Error, Result};

pub enum Task {
    Register {
        request: ChangeDataRequest,
        downstream: Downstream,
    },
    Deregister {
        region_id: u64,
        id: Option<usize>,
        err: Option<Error>,
    },
    MultiBatch {
        multi: Vec<CmdBatch>,
    },
    MinTS {
        min_ts: TimeStamp,
    },
    ResolverReady {
        region_id: u64,
        region: Region,
        resolver: Resolver,
    },
    IncrementalScan {
        region_id: u64,
        downstream_id: usize,
        entries: Vec<Option<TxnEntry>>,
    },
    #[cfg(not(validate))]
    Validate(u64, Box<dyn FnOnce(Option<&Delegate>) + Send>),
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("CdcTask");
        match self {
            Task::Register {
                ref request,
                ref downstream,
                ..
            } => de
                .field("register request", request)
                .field("request", request)
                .field("id", &downstream.id)
                .finish(),
            Task::Deregister {
                ref region_id,
                ref id,
                ref err,
            } => de
                .field("deregister", &"")
                .field("region_id", region_id)
                .field("err", err)
                .field("id", id)
                .finish(),
            Task::MinTS { ref min_ts } => de.field("min_ts", min_ts).finish(),
            Task::ResolverReady { ref region_id, .. } => de.field("region_id", region_id).finish(),
            Task::MultiBatch { multi } => de.field("multibatch", &multi.len()).finish(),
            Task::IncrementalScan {
                ref region_id,
                ref downstream_id,
                ref entries,
            } => de
                .field("region_id", &region_id)
                .field("downstream", &downstream_id)
                .field("scan_entries", &entries.len())
                .finish(),
            #[cfg(not(validate))]
            Task::Validate(region_id, _) => de.field("region_id", &region_id).finish(),
        }
    }
}

pub struct Endpoint {
    capture_regions: HashMap<u64, Delegate>,
    scheduler: Scheduler<Task>,
    apply_router: ApplyRouter,
    observer: CdcObserver,

    pd_client: Arc<dyn PdClient>,
    timer: SteadyTimer,
    min_ts_interval: Duration,
    scan_batch_size: usize,

    workers: ThreadPool,
}

impl Endpoint {
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        apply_router: ApplyRouter,
        observer: CdcObserver,
    ) -> Endpoint {
        let workers = Builder::new().name_prefix("cdcwkr").pool_size(4).build();
        let ep = Endpoint {
            capture_regions: HashMap::default(),
            scheduler,
            observer,
            pd_client,
            timer: SteadyTimer::default(),
            workers,
            apply_router,
            scan_batch_size: 1024,
            min_ts_interval: Duration::from_secs(10),
        };
        ep.register_min_ts_event();
        ep
    }

    pub fn set_min_ts_interval(&mut self, dur: Duration) {
        self.min_ts_interval = dur;
    }

    pub fn set_scan_batch_size(&mut self, scan_batch_size: usize) {
        self.scan_batch_size = scan_batch_size;
    }

    fn on_deregister(&mut self, region_id: u64, id: Option<usize>, err: Option<Error>) {
        info!("cdc deregister region";
            "region_id" => region_id,
            "id" => ?id,
            "error" => ?err);
        let mut is_last = false;
        match (id, err) {
            (Some(id), err) => {
                // The peer wants to deregister
                if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                    is_last = delegate.unsubscribe(id, err);
                }
                if is_last {
                    self.capture_regions.remove(&region_id);
                }
            }
            (None, Some(err)) => {
                // Something went wrong, deregister all downstreams.
                if let Some(mut delegate) = self.capture_regions.remove(&region_id) {
                    delegate.fail(err);
                    is_last = true;
                }
            }
            (None, None) => panic!("none id none error?"),
        }
        if is_last {
            // Unsubscribe region role change events.
            self.observer.unsubscribe_region(region_id);
        }
    }

    pub fn on_register(&mut self, mut request: ChangeDataRequest, downstream: Downstream) {
        let region_id = request.region_id;
        info!("cdc register region"; "region_id" => region_id);
        let mut enabled = None;
        let delegate = self.capture_regions.entry(region_id).or_insert_with(|| {
            let d = Delegate::new(region_id);
            enabled = Some(d.enabled());
            d
        });

        let downstream_id = downstream.id;
        let checkpoint_ts = request.checkpoint_ts;
        let sched = self.scheduler.clone();
        let workers = self.workers.sender().clone();
        let batch_size = self.scan_batch_size;

        let init = Initializer {
            workers,
            sched,
            region_id,
            downstream_id,
            checkpoint_ts: checkpoint_ts.into(),
            batch_size,
            build_resolver: enabled.is_some(),
        };
        delegate.subscribe(downstream);
        let change_cmd = if let Some(enabled) = enabled {
            // The region has never been registered.
            // Subscribe region role change events.
            self.observer.subscribe_region(region_id);

            ApplyTask::Change(ChangeCmd::RegisterObserver {
                region_id,
                region_epoch: request.take_region_epoch(),
                enabled,
                cb: Callback::Read(Box::new(move |resp: ReadResponse<_>| {
                    init.on_change_cmd(resp);
                })),
            })
        } else {
            ApplyTask::Change(ChangeCmd::Snapshot {
                region_id,
                region_epoch: request.take_region_epoch(),
                cb: Callback::Read(Box::new(move |resp: ReadResponse<_>| {
                    init.on_change_cmd(resp);
                })),
            })
        };
        self.apply_router.schedule_task(region_id, change_cmd);
    }

    pub fn on_multi_batch(&mut self, multi: Vec<CmdBatch>) {
        for batch in multi {
            let mut has_failed = false;
            let region_id = batch.region_id;
            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                delegate.on_batch(batch);
                has_failed = delegate.has_failed();
            }
            if has_failed {
                self.capture_regions.remove(&region_id);
            }
        }
    }

    pub fn on_incremental_scan(
        &mut self,
        region_id: u64,
        downstream_id: usize,
        entries: Vec<Option<TxnEntry>>,
    ) {
        if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
            delegate.on_scan(downstream_id, entries);
        } else {
            warn!("region not found on incremental scan"; "region_id" => region_id);
        }
    }

    fn on_region_ready(&mut self, region_id: u64, resolver: Resolver, region: Region) {
        let delegate = self.capture_regions.get_mut(&region_id).unwrap();
        delegate.on_region_ready(resolver, region);
    }

    fn on_min_ts(&mut self, min_ts: TimeStamp) {
        for delegate in self.capture_regions.values_mut() {
            delegate.on_min_ts(min_ts);
        }
        self.register_min_ts_event();
    }

    fn register_min_ts_event(&self) {
        let timeout = self.timer.delay(self.min_ts_interval);
        let tso = self.pd_client.get_tso();
        let scheduler = self.scheduler.clone();
        let fut = tso.join(timeout.map_err(|_| unreachable!())).then(
            move |tso: pd_client::Result<(TimeStamp, ())>| {
                // Ignore get tso errors since we will retry every `min_ts_interval`.
                let (min_ts, _) = tso.unwrap_or((TimeStamp::default(), ()));
                if let Err(e) = scheduler.schedule(Task::MinTS { min_ts }) {
                    // Must schedule `MinTS` event otherwise resolved ts can not
                    // advance normally.
                    panic!(
                        "failed to schedule min_ts event, min_ts: {}, error: {:?}",
                        min_ts, e
                    );
                }
                Ok(())
            },
        );
        self.pd_client.spawn(Box::new(fut) as _);
    }
}

struct Initializer {
    workers: PoolSender,
    sched: Scheduler<Task>,

    region_id: u64,
    downstream_id: usize,
    checkpoint_ts: TimeStamp,
    batch_size: usize,

    build_resolver: bool,
}

impl Initializer {
    fn on_change_cmd(&self, mut resp: ReadResponse<RocksEngine>) {
        if let Some(region_snapshot) = resp.snapshot {
            assert_eq!(self.region_id, region_snapshot.get_region().get_id());
            self.async_incremental_scan(region_snapshot);
        } else {
            assert!(
                resp.response.get_header().has_error(),
                "no snapshot and no error? {:?}",
                resp.response
            );
            let err = resp.response.take_header().take_error();
            let deregister = Task::Deregister {
                region_id: self.region_id,
                id: Some(self.downstream_id),
                err: Some(Error::Request(err)),
            };
            self.sched.schedule(deregister).unwrap();
        }
    }

    fn async_incremental_scan(&self, snap: RegionSnapshot<RocksEngine>) {
        let downstream_id = self.downstream_id;
        let sched = self.sched.clone();
        let batch_size = self.batch_size;
        let checkpoint_ts = self.checkpoint_ts;
        let build_resolver = self.build_resolver;
        info!("async incremental scan";
            "region_id" => snap.get_region().get_id(),
            "downstream_id" => downstream_id);

        // spawn the task to a thread pool.
        let region_id = snap.get_region().get_id();
        let region = snap.get_region().clone();
        self.workers
            .spawn(lazy(move || {
                let mut resolver = if build_resolver {
                    Some(Resolver::new())
                } else {
                    None
                };

                // Time range: (checkpoint_ts, current]
                let current = TimeStamp::max();
                let mut scanner = ScannerBuilder::new(snap, current, false)
                    .range(None, None)
                    .isolation_level(IsolationLevel::Si)
                    .build_delta(checkpoint_ts, false)
                    .unwrap();
                let mut done = false;
                while !done {
                    let entries =
                        match Self::scan_batch(&mut scanner, batch_size, resolver.as_mut()) {
                            Ok(res) => res,
                            Err(e) => {
                                error!("cdc scan entries failed"; "error" => ?e);
                                // TODO: record in metrics.
                                if let Err(e) = sched.schedule(Task::Deregister {
                                    region_id,
                                    id: Some(downstream_id),
                                    err: Some(e),
                                }) {
                                    error!("schedule task failed"; "error" => ?e);
                                }
                                return Ok(());
                            }
                        };
                    // If the last element is None, it means scanning is finished.
                    if let Some(None) = entries.last() {
                        done = true;
                    }
                    debug!("cdc scan entries"; "len" => entries.len());
                    let scanned = Task::IncrementalScan {
                        region_id,
                        downstream_id,
                        entries,
                    };
                    if let Err(e) = sched.schedule(scanned) {
                        error!("schedule task failed"; "error" => ?e);
                        return Ok(());
                    }
                }

                if let Some(resolver) = resolver {
                    Self::finish_building_resolver(resolver, region, sched);
                }

                Ok(())
            }))
            .unwrap();
    }

    fn scan_batch<S: Snapshot>(
        scanner: &mut DeltaScanner<S>,
        batch_size: usize,
        resolver: Option<&mut Resolver>,
    ) -> Result<Vec<Option<TxnEntry>>> {
        let mut entries = Vec::with_capacity(batch_size);
        while entries.len() < entries.capacity() {
            match scanner.next_entry()? {
                Some(entry) => {
                    entries.push(Some(entry));
                }
                None => {
                    entries.push(None);
                    break;
                }
            }
        }

        if let Some(resolver) = resolver {
            // Track the locks.
            for entry in &entries {
                match entry {
                    Some(TxnEntry::Prewrite { lock, .. }) => {
                        let (encoded_key, value) = lock;
                        let key = Key::from_encoded_slice(encoded_key).into_raw().unwrap();
                        let lock = Lock::parse(value)?;
                        resolver.track_lock(lock.ts, key);
                    }
                    _ => {}
                }
            }
        }

        Ok(entries)
    }

    fn finish_building_resolver(mut resolver: Resolver, region: Region, sched: Scheduler<Task>) {
        resolver.init();
        if resolver.locks().is_empty() {
            info!(
                "no lock found";
                "region_id" => region.get_id()
            );
        } else {
            let rts = resolver.resolve(TimeStamp::zero());
            info!(
                "resolver initialized";
                "region_id" => region.get_id(),
                "resolved_ts" => rts,
                "lock_count" => resolver.locks().len()
            );
        }

        info!("schedule resolver ready"; "region_id" => region.get_id());
        if let Err(e) = sched.schedule(Task::ResolverReady {
            region_id: region.get_id(),
            resolver,
            region,
        }) {
            error!("schedule task failed"; "error" => ?e);
        }
    }
}

impl Runnable<Task> for Endpoint {
    fn run(&mut self, task: Task) {
        debug!("run cdc task"; "task" => %task);
        match task {
            Task::MinTS { min_ts } => self.on_min_ts(min_ts),
            Task::Register {
                request,
                downstream,
            } => self.on_register(request, downstream),
            Task::ResolverReady {
                region_id,
                resolver,
                region,
            } => self.on_region_ready(region_id, resolver, region),
            Task::Deregister { region_id, id, err } => self.on_deregister(region_id, id, err),
            Task::MultiBatch { multi } => self.on_multi_batch(multi),
            Task::IncrementalScan {
                region_id,
                downstream_id,
                entries,
            } => {
                self.on_incremental_scan(region_id, downstream_id, entries);
            }
            #[cfg(not(validate))]
            Task::Validate(region_id, validate) => {
                validate(self.capture_regions.get(&region_id));
            }
        }
    }
}
