use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures::future::{lazy, Future};
use kvproto::cdcpb::*;
use kvproto::metapb::Region;
use pd_client::PdClient;
use resolved_ts::Resolver;
use tikv::raftstore::coprocessor::*;
use tikv::raftstore::store::fsm::{ApplyRouter, ApplyTask};
use tikv::raftstore::store::msg::{Callback, ReadResponse};
use tikv::raftstore::store::RegionSnapshot;
use tikv_util::collections::HashMap;
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::{Runnable, Scheduler};
use tokio_threadpool::{Builder, ThreadPool};

use crate::delegate::{Delegate, Downstream};
use crate::lock_scanner::LockScanner;
use crate::{CdcObserver, Error};

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
        min_ts: u64,
    },
    ResolverReady {
        region_id: u64,
        region: Region,
        resolver: Resolver,
    },
    LoadLocks {
        region_snapshot: RegionSnapshot,
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
            Task::LoadLocks {
                ref region_snapshot,
            } => de
                .field(
                    "loadlocks region_id",
                    &region_snapshot.get_region().get_id(),
                )
                .finish(),
            Task::MultiBatch { multi } => de.field("multibatch", &multi.len()).finish(),
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

    lock_workers: ThreadPool,
}

impl Endpoint {
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        apply_router: ApplyRouter,
        observer: CdcObserver,
    ) -> Endpoint {
        let lock_workers = Builder::new().name_prefix("cdcwkr").pool_size(4).build();
        let ep = Endpoint {
            capture_regions: HashMap::default(),
            scheduler,
            observer,
            pd_client,
            timer: SteadyTimer::default(),
            lock_workers,
            apply_router,
            min_ts_interval: Duration::from_secs(10),
        };
        ep.register_min_ts_event();
        ep
    }

    pub fn set_min_ts_interval(&mut self, dur: Duration) {
        self.min_ts_interval = dur;
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

        let id = downstream.id;
        delegate.subscribe(downstream);
        if let Some(enabled) = enabled {
            // The region has never been registered.
            // Subscribe region role change events.
            self.observer.subscribe_region(region_id);

            let scheduler = self.scheduler.clone();
            self.apply_router.schedule_task(
                region_id,
                ApplyTask::RegisterCmdObserver {
                    region_id,
                    region_epoch: request.take_region_epoch(),
                    enabled,
                    cb: Callback::Read(Box::new(move |mut resp: ReadResponse| {
                        if let Some(region_snapshot) = resp.snapshot {
                            let load_locks = Task::LoadLocks { region_snapshot };
                            info!("schedule load locks"; "region_id" => region_id);
                            scheduler.schedule(load_locks).unwrap();
                        } else {
                            assert!(
                                resp.response.get_header().has_error(),
                                "no snashot and no error? {:?}",
                                resp.response
                            );
                            let err = resp.response.take_header().take_error();
                            let deregister = Task::Deregister {
                                region_id,
                                id: Some(id),
                                err: Some(Error::Request(err)),
                            };
                            scheduler.schedule(deregister).unwrap();
                        }
                    })),
                },
            );
        }
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

    fn on_region_load_locks(&mut self, region_snapshot: RegionSnapshot) {
        info!("load locks for resolver";
            "region_id" => region_snapshot.get_region().get_id());
        // spawn the task to a thread pool.
        let sched = self.scheduler.clone();
        let region_id = region_snapshot.get_region().get_id();
        let region = region_snapshot.get_region().clone();
        self.lock_workers.spawn(
            lazy(move || {
                let mut lock_scanner = LockScanner::new(region_snapshot)?;
                lock_scanner.build_resolver()
            })
            .then(move |res| match res {
                Ok(resolver) => {
                    info!("schedule resolver ready";
                        "region_id" => region_id);
                    if let Err(e) = sched.schedule(Task::ResolverReady {
                        region_id,
                        resolver,
                        region,
                    }) {
                        error!("schedule task failed"; "error" => ?e);
                    }
                    Ok(())
                }
                Err(e) => {
                    error!("builder resolver failed"; "error" => ?e);
                    // Unregister all downstreams.
                    // TODO: record in metrics.
                    if let Err(e) = sched.schedule(Task::Deregister {
                        region_id,
                        id: None,
                        err: Some(e),
                    }) {
                        error!("schedule task failed"; "error" => ?e);
                    }
                    Ok(())
                }
            }),
        );
    }

    fn on_region_ready(&mut self, region_id: u64, resolver: Resolver, region: Region) {
        let delegate = self.capture_regions.get_mut(&region_id).unwrap();
        delegate.on_region_ready(resolver, region);
    }

    fn on_min_ts(&mut self, min_ts: u64) {
        for delegate in self.capture_regions.values_mut() {
            delegate.on_min_ts(min_ts);
        }
        self.register_min_ts_event();
    }

    fn register_min_ts_event(&self) {
        let timeout = self.timer.delay(self.min_ts_interval);
        let tso = self.pd_client.get_tso();
        let scheduler = self.scheduler.clone();
        let fut = tso
            .join(timeout.map_err(|_| unreachable!()))
            .map(move |(min_ts, ())| {
                if let Err(e) = scheduler.schedule(Task::MinTS { min_ts }) {
                    error!("failed to schedule min_ts event";
                        "error" => ?e,
                        "min_ts" => min_ts);
                }
            })
            .map_err(|e| {
                error!("get tso failed"; "error" => ?e);
                e
            });
        self.pd_client.spawn(Box::new(fut) as _);
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
            Task::LoadLocks { region_snapshot } => self.on_region_load_locks(region_snapshot),
            Task::ResolverReady {
                region_id,
                resolver,
                region,
            } => self.on_region_ready(region_id, resolver, region),
            Task::Deregister { region_id, id, err } => self.on_deregister(region_id, id, err),
            Task::MultiBatch { multi } => self.on_multi_batch(multi),
            #[cfg(not(validate))]
            Task::Validate(region_id, validate) => {
                validate(self.capture_regions.get(&region_id));
            }
        }
    }
}
