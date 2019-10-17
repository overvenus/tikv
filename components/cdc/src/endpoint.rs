use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::{lazy, Future};
use futures::sync::mpsc::UnboundedSender;
use kvproto::cdcpb::*;
use pd_client::PdClient;
use resolved_ts::Resolver;
use tikv::raftstore::store::fsm::{ApplyRouter, ApplyTask};
use tikv::raftstore::store::RegionSnapshot;
use tikv_util::collections::HashMap;
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::{Runnable, Scheduler};
use tokio_threadpool::{Builder, ThreadPool};

use crate::delegate::Delegate;
use crate::lock_scanner::LockScanner;
use crate::CdcObserver;
use crate::RawEvent;

pub enum Task {
    Register {
        request: ChangeDataRequest,
        sink: UnboundedSender<ChangeDataEvent>,
    },
    Deregister {
        region_id: u64,
        // error: Option<Error>,
    },
    RawEvent(RawEvent),
    MinTS {
        min_ts: u64,
    },
    ResolverReady {
        region_id: u64,
        resolver: Resolver,
    },
    LoadLocks {
        region_snapshot: RegionSnapshot,
    },
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
            Task::Register { ref request, .. } => de.field("request", request).finish(),
            Task::Deregister { ref region_id, .. } => {
                de.field("deregister region_id", region_id).finish()
            }
            Task::RawEvent(_) => de.field("raw_event", &"...").finish(),
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
        }
    }
}

pub struct Endpoint {
    capture_regions: HashMap<u64, (Delegate, Arc<AtomicBool>)>,
    observer: CdcObserver,
    scheduler: Scheduler<Task>,
    apply_router: ApplyRouter,

    pd_client: Arc<dyn PdClient>,
    timer: SteadyTimer,

    lock_workers: ThreadPool,
}

impl Endpoint {
    pub fn new(
        observer: CdcObserver,
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        apply_router: ApplyRouter,
    ) -> Endpoint {
        let lock_workers = Builder::new().name_prefix("cdcwkr").pool_size(4).build();
        let ep = Endpoint {
            capture_regions: HashMap::default(),
            observer,
            scheduler,
            pd_client,
            timer: SteadyTimer::default(),
            lock_workers,
            apply_router,
        };
        ep.register_min_ts_event(Duration::from_secs(10));
        ep
    }

    fn on_deregister(&mut self, region_id: u64) {
        info!("cdc deregister region"; "region_id" => region_id);
        self.observer.deregister_region(region_id);
        if let Some((_, enabled)) = self.capture_regions.remove(&region_id) {
            enabled.store(false, Ordering::Relaxed);
        }
    }

    pub fn on_register(
        &mut self,
        request: ChangeDataRequest,
        sink: UnboundedSender<ChangeDataEvent>,
    ) {
        let region_id = request.region_id;
        info!("cdc register region"; "region_id" => region_id);
        let enabled = Arc::new(AtomicBool::new(true));
        let delegate = Delegate::new(region_id, sink);
        if self
            .capture_regions
            .insert(region_id, (delegate, enabled.clone()))
            .is_some()
        {
            // TODO: should we close the sink?
            warn!("replace region change data sink"; "region_id"=> region_id);
        }
        self.observer.register_region(region_id);
        self.apply_router.schedule_task(
            region_id,
            ApplyTask::RegisterCmdObserver { region_id, enabled },
        )
    }

    pub fn on_raw_event(&mut self, event: RawEvent) {
        match event {
            RawEvent::DataRequest {
                region_id,
                index,
                requests,
            } => {
                if let Some((delegate, _)) = self.capture_regions.get_mut(&region_id) {
                    delegate.on_data_requsts(index, requests)
                }
            }
            RawEvent::DataResponse {
                region_id,
                index,
                header,
            } => {
                if let Some((delegate, _)) = self.capture_regions.get_mut(&region_id) {
                    delegate.on_data_responses(index, header)
                }
            }
            RawEvent::AdminRequest {
                region_id,
                index,
                request,
            } => {
                if let Some((delegate, _)) = self.capture_regions.get_mut(&region_id) {
                    delegate.on_admin_requst(index, request)
                }
            }
            RawEvent::AdminResponse {
                region_id,
                index,
                header,
                response,
            } => {
                if let Some((delegate, _)) = self.capture_regions.get_mut(&region_id) {
                    delegate.on_admin_response(index, header, response)
                }
            }
        }
    }

    fn on_region_load_locks(&mut self, region_snapshot: RegionSnapshot) {
        info!("load locks for resolver";
            "region_id" => region_snapshot.get_region().get_id());
        // spawn the task to a thread pool.
        let sched = self.scheduler.clone();
        let region_id = region_snapshot.get_region().get_id();
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
                    }) {
                        error!("schedule task failed"; "error" => ?e);
                    }
                    Ok(())
                }
                Err(e) => {
                    error!("builder resolver failed"; "error" => ?e);
                    // TODO: record in metrics.
                    // TODO: attach error the the task.
                    if let Err(e) = sched.schedule(Task::Deregister { region_id }) {
                        error!("schedule task failed"; "error" => ?e);
                    }
                    Ok(())
                }
            }),
        );
    }

    fn on_region_ready(&mut self, region_id: u64, resolver: Resolver) {
        let (delegate, _) = self.capture_regions.get_mut(&region_id).unwrap();
        delegate.on_region_ready(resolver);
    }

    fn on_min_ts(&mut self, min_ts: u64) {
        for (delegate, _) in self.capture_regions.values_mut() {
            delegate.on_min_ts(min_ts);
        }
        self.register_min_ts_event(Duration::from_secs(10));
    }

    fn register_min_ts_event(&self, dur: Duration) {
        let timeout = self.timer.delay(dur);
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
            Task::RawEvent(event) => self.on_raw_event(event),
            Task::MinTS { min_ts } => self.on_min_ts(min_ts),
            Task::Register { request, sink } => self.on_register(request, sink),
            Task::LoadLocks { region_snapshot } => self.on_region_load_locks(region_snapshot),
            Task::ResolverReady {
                region_id,
                resolver,
            } => self.on_region_ready(region_id, resolver),
            Task::Deregister { region_id } => self.on_deregister(region_id),
        }
    }
}
