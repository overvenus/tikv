// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

const MUTEX_ACQUIRE_DURATION_BUCKETS: &[f64] = &[
    4.0,
    16.0,
    32.0,
    64.0, // L1: ideal uncontended cases
    128.0,
    256.0,
    512.0, // L2: slight contention
    1000.0,
    2000.0,
    4000.0,
    8000.0, // L3: microsecond level
    16000.0,
    32000.0,
    64000.0, // L4: unusual, possible queuing
    128000.0,
    256000.0,
    512000.0, // L5: rare contention spikes
    1000000.0,
    10000000.0,
    100000000.0,
    1000000000.0, // 1ms, 10ms, 100ms, 1 second, to cover extreme cases
];

const MUTEX_HOLD_DURATION_BUCKETS: &[f64] = &[
    16.0,
    32.0,
    64.0,
    128.0,
    256.0,
    512.0, // L1: short, well-behaved
    1000.0,
    2000.0,
    4000.0,
    8000.0, // L2: moderate
    16000.0,
    32000.0,
    64000.0, // L3: long holds
    128000.0,
    256000.0,
    512000.0, // L4: problematic
    1000000.0,
    2000000.0,
    4000000.0, // L5: major red flags
    8000000.0,
    16000000.0, // L6: severe outliers
    32000000.0,
    100000000.0,
    1000000000.0,
    4000000000.0,
    8000000000.0, // 32ms, 100ms, 1 second, 4 seconds, 8 seconds to cover extreme cases
];

lazy_static! {
    pub static ref MUTEX_ACQUIRE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_sync_instrumented_mutex_acquire_nanoseconds",
        "Bucketed histogram of instrumented mutex lock duration in nanoseconds.",
        &["name"],
        MUTEX_ACQUIRE_DURATION_BUCKETS.to_vec()
    )
    .unwrap();
    pub static ref MUTEX_HOLD_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "tikv_sync_instrumented_mutex_hold_nanoseconds",
        "Bucketed histogram of instrumented mutex hold duration in nanoseconds.",
        &["name"],
        MUTEX_HOLD_DURATION_BUCKETS.to_vec()
    )
    .unwrap();
}
