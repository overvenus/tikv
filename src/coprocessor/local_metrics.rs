// Copyright 2017 PingCAP, Inc.
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

use coprocessor::dag::executor::ExecutorMetrics;
use coprocessor::metrics::*;
use pd::PdTask;
use prometheus::local::{LocalHistogramVec, LocalIntCounterVec};
use storage::engine::{FlowStatistics, Statistics};
use util::collections::HashMap;
use util::worker::FutureScheduler;

/// `CopFlowStatistics` is for flow statistics, it would be reported to PD by flush.
pub struct CopFlowStatistics {
    data: HashMap<u64, FlowStatistics>,
    sender: FutureScheduler<PdTask>,
}

impl CopFlowStatistics {
    pub fn new(sender: FutureScheduler<PdTask>) -> CopFlowStatistics {
        CopFlowStatistics {
            sender,
            data: Default::default(),
        }
    }

    pub fn collect(&mut self, region_id: u64, stats: &Statistics) {
        let flow_stats = self.data.entry(region_id).or_default();
        flow_stats.add(&stats.write.flow_stats);
        flow_stats.add(&stats.data.flow_stats);
    }

    pub fn flush(&mut self) {
        if self.data.is_empty() {
            return;
        }
        let mut to_send_stats = HashMap::default();
        mem::swap(&mut to_send_stats, &mut self.data);
        if let Err(e) = self.sender.schedule(PdTask::ReadStats {
            read_stats: to_send_stats,
        }) {
            error!("send coprocessor statistics: {:?}", e);
        };
    }
}

/// `ExecLocalMetrics` collects metrics for request with executors.
pub struct ExecLocalMetrics {
    flow_stats: CopFlowStatistics,
    scan_details: LocalIntCounterVec,
    scan_counter: LocalIntCounterVec,
    exec_counter: LocalIntCounterVec,
}

impl ExecLocalMetrics {
    pub fn new(sender: FutureScheduler<PdTask>) -> ExecLocalMetrics {
        ExecLocalMetrics {
            flow_stats: CopFlowStatistics::new(sender),
            scan_details: COPR_SCAN_DETAILS.local(),
            scan_counter: COPR_GET_OR_SCAN_COUNT.local(),
            exec_counter: COPR_EXECUTOR_COUNT.local(),
        }
    }

    pub fn collect(&mut self, type_str: &str, region_id: u64, metrics: ExecutorMetrics) {
        let stats = &metrics.cf_stats;
        // cf statistics group by type
        for (cf, details) in stats.details() {
            for (tag, count) in details {
                self.scan_details
                    .with_label_values(&[type_str, cf, tag])
                    .inc_by(count as i64);
            }
        }
        // flow statistics group by region
        self.flow_stats.collect(region_id, stats);
        // scan count
        if metrics.scan_counter.point > 0 {
            self.scan_counter
                .with_label_values(&["point"])
                .inc_by(metrics.scan_counter.point as i64);
        }
        if metrics.scan_counter.range > 0 {
            self.scan_counter
                .with_label_values(&["range"])
                .inc_by(metrics.scan_counter.range as i64);
        }
        // exec count
        if metrics.executor_count.table_scan > 0 {
            self.exec_counter
                .with_label_values(&["tblscan"])
                .inc_by(metrics.executor_count.table_scan);
        }
        if metrics.executor_count.index_scan > 0 {
            self.exec_counter
                .with_label_values(&["idxscan"])
                .inc_by(metrics.executor_count.index_scan);
        }
        if metrics.executor_count.selection > 0 {
            self.exec_counter
                .with_label_values(&["selection"])
                .inc_by(metrics.executor_count.selection);
        }
        if metrics.executor_count.topn > 0 {
            self.exec_counter
                .with_label_values(&["topn"])
                .inc_by(metrics.executor_count.topn);
        }
        if metrics.executor_count.limit > 0 {
            self.exec_counter
                .with_label_values(&["limit"])
                .inc_by(metrics.executor_count.limit);
        }
        if metrics.executor_count.aggregation > 0 {
            self.exec_counter
                .with_label_values(&["aggregation"])
                .inc_by(metrics.executor_count.aggregation);
        }
    }

    pub fn flush(&mut self) {
        self.flow_stats.flush();
        self.scan_details.flush();
        self.scan_counter.flush();
        self.exec_counter.flush();
    }
}

/// `BasicLocalMetrics` is for the basic metrics for coprocessor requests.
pub struct BasicLocalMetrics {
    pub req_time: LocalHistogramVec,
    pub outdate_time: LocalHistogramVec,
    pub handle_time: LocalHistogramVec,
    pub wait_time: LocalHistogramVec,
    pub error_cnt: LocalIntCounterVec,
    pub scan_keys: LocalHistogramVec,
}

impl Default for BasicLocalMetrics {
    fn default() -> BasicLocalMetrics {
        BasicLocalMetrics {
            req_time: COPR_REQ_HISTOGRAM_VEC.local(),
            outdate_time: OUTDATED_REQ_WAIT_TIME.local(),
            handle_time: COPR_REQ_HANDLE_TIME.local(),
            wait_time: COPR_REQ_WAIT_TIME.local(),
            error_cnt: COPR_REQ_ERROR.local(),
            scan_keys: COPR_SCAN_KEYS.local(),
        }
    }
}

impl BasicLocalMetrics {
    pub fn flush(&mut self) {
        self.req_time.flush();
        self.outdate_time.flush();
        self.handle_time.flush();
        self.wait_time.flush();
        self.scan_keys.flush();
        self.error_cnt.flush();
    }
}

impl Drop for BasicLocalMetrics {
    fn drop(&mut self) {
        self.flush();
    }
}
