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

use std::error;
use std::result;
use std::time::Duration;

use kvproto::{errorpb, kvrpcpb};
use tipb;

use coprocessor;
use storage;
use util;

quick_error! {
    /// The error tpye of coprocessor framework.
    #[derive(Debug)]
    pub enum Error {
        /// A request meets errors related to region.
        Region(err: errorpb::Error) {
            description("region related failure")
            display("region {:?}", err)
        }
        /// A key is locked.
        Locked(l: kvrpcpb::LockInfo) {
            description("key is locked")
            display("locked {:?}", l)
        }
        /// A request is out of date.
        Outdated(elapsed: Duration, tag: &'static str) {
            description("request is outdated")
        }
        /// Coprocessor is full of task. It's busy.
        Full(allow: usize) {
            description("running queue is full")
        }
        /// An evaluation error when we handle a request.
        Eval(err: tipb::select::Error) {
            from()
            description("eval failed")
            display("eval error {:?}", err)
        }
        /// Other categorized errors.
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

/// A convenience wrapper for `Result<T, Error>`.
pub type Result<T> = result::Result<T, Error>;

impl From<storage::engine::Error> for Error {
    /// Converts `storage::engine::Error` to `Error`.
    fn from(e: storage::engine::Error) -> Error {
        match e {
            storage::engine::Error::Request(e) => Error::Region(e),
            _ => Error::Other(box e),
        }
    }
}

impl From<coprocessor::dag::expr::Error> for Error {
    /// Converts `coprocessor::dag::expr::Error` to `Error`.
    fn from(e: coprocessor::dag::expr::Error) -> Error {
        Error::Eval(e.into())
    }
}

impl From<util::codec::Error> for Error {
    /// Converts `util::codec::Error` to `Error`.
    fn from(e: util::codec::Error) -> Error {
        let mut err = tipb::select::Error::new();
        err.set_msg(format!("{}", e));
        Error::Eval(err)
    }
}

impl From<storage::txn::Error> for Error {
    /// Converts `storage::txn::Error` to `Error`.
    fn from(e: storage::txn::Error) -> Error {
        match e {
            storage::txn::Error::Mvcc(storage::mvcc::Error::KeyIsLocked {
                primary,
                ts,
                key,
                ttl,
            }) => {
                let mut info = kvrpcpb::LockInfo::new();
                info.set_primary_lock(primary);
                info.set_lock_version(ts);
                info.set_key(key);
                info.set_lock_ttl(ttl);
                Error::Locked(info)
            }
            _ => Error::Other(box e),
        }
    }
}
