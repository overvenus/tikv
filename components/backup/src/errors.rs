// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::{error, result};

use kvproto::backup::{BackupState, Error as ErrorPb};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{}", err)
        }
        Io(err: IoError) {
            from()
            cause(err)
            display("{}", err)
            description(err.description())
        }
        Step(current: BackupState, request: BackupState) {
            display("current {:?}, request {:?}", current, request)
            description("can not step backup state")
        }
        ClusterID(current: u64, request: u64) {
            display("current {:?}, request {:?}", current, request)
            description("cluster ID mismatch")
        }
    }
}

impl Into<ErrorPb> for Error {
    fn into(self) -> ErrorPb {
        let mut err = ErrorPb::new();
        match self {
            Error::Step(current, request) => {
                err.mut_state_step_error().set_current(current);
                err.mut_state_step_error().set_request(request);
            }
            Error::ClusterID(current, request) => {
                err.mut_cluster_id_error().set_current(current);
                err.mut_cluster_id_error().set_request(request);
            }
            other => {
                err.set_msg(format!("{:?}", other));
            }
        }
        err
    }
}

pub type Result<T> = result::Result<T, Error>;
