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

//! Module metric provides metrics for the coprocessor framework.

use prometheus::*;

lazy_static! {
    /// Records how long a request gets its response.
    pub static ref COPR_REQ_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_request_duration_seconds",
        "Bucketed histogram of coprocessor request duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    ).unwrap();
    /// Records how long a outdated request waits to be handle.
    pub static ref OUTDATED_REQ_WAIT_TIME: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_outdated_request_wait_seconds",
        "Bucketed histogram of outdated coprocessor request wait duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    ).unwrap();
    /// Records how long coprocessor handles a request.
    pub static ref COPR_REQ_HANDLE_TIME: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_request_handle_seconds",
        "Bucketed histogram of coprocessor handle request duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    ).unwrap();
    /// Records how long a request waits to be handled.
    pub static ref COPR_REQ_WAIT_TIME: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_request_wait_seconds",
        "Bucketed histogram of coprocessor request wait duration",
        &["req"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    ).unwrap();
    /// Records how many errors have occurred.
    pub static ref COPR_REQ_ERROR: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_request_error",
        "Total number of push down request error.",
        &["reason"]
    ).unwrap();
    /// Records how many requests coprocessor has received.
    pub static ref COPR_PENDING_REQS: IntGaugeVec = register_int_gauge_vec!(
        "tikv_coprocessor_pending_request",
        "Total number of pending push down request.",
        &["req", "priority"]
    ).unwrap();
    /// Records how many keys a request scans.
    pub static ref COPR_SCAN_KEYS: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_scan_keys",
        "Bucketed histogram of coprocessor per request scan keys",
        &["req"],
        exponential_buckets(1.0, 2.0, 20).unwrap()
    ).unwrap();
    /// Records details about a request's scan.
    pub static ref COPR_SCAN_DETAILS: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_scan_details",
        "Bucketed counter of coprocessor scan details for each CF",
        &["req", "cf", "tag"]
    ).unwrap();
    /// Records how many executors has been created.
    pub static ref COPR_EXECUTOR_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_executor_count",
        "Total number of each executor",
        &["type"]
    ).unwrap();
    /// Records total number of rocksdb query of get or scan count.
    pub static ref COPR_GET_OR_SCAN_COUNT: IntCounterVec = register_int_counter_vec!(
        "tikv_coprocessor_get_or_scan_count",
        "Total number of rocksdb query of get or scan count",
        &["type"]
    ).unwrap();
    /// Records how many tasks in a batch.
    pub static ref BATCH_REQUEST_TASKS: HistogramVec = register_histogram_vec!(
        "tikv_coprocessor_batch_request_tasks_total",
        "Bucketed histogram of total number of a batch request task",
        &["type"],
        vec![
            1.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0, 24.0, 28.0, 32.0, 48.0,
            64.0, 96.0, 128.0, 192.0, 256.0,
        ]
    ).unwrap();
}
