// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Error as IoError;
use std::{error, result};

use kvproto::errorpb::Error as ErrorHeader;
use tikv::storage::kv::Error as EngineError;
use tikv::storage::mvcc::Error as MvccError;
use tikv::storage::txn::Error as TxnError;

/// The error type for cdc.
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Other error {}", _0)]
    Other(Box<dyn error::Error + Sync + Send>),
    #[fail(display = "RocksDB error {}", _0)]
    Rocks(String),
    #[fail(display = "IO error {}", _0)]
    Io(IoError),
    #[fail(display = "Engine error {}", _0)]
    Engine(EngineError),
    #[fail(display = "Transaction error {}", _0)]
    Txn(TxnError),
    #[fail(display = "Mvcc error {}", _0)]
    Mvcc(MvccError),
    #[fail(display = "Request error {:?}", _0)]
    Request(ErrorHeader),
}

macro_rules! impl_from {
    ($($inner:ty => $container:ident,)+) => {
        $(
            impl From<$inner> for Error {
                fn from(inr: $inner) -> Error {
                    Error::$container(inr)
                }
            }
        )+
    };
}

impl_from! {
    Box<dyn error::Error + Sync + Send> => Other,
    String => Rocks,
    IoError => Io,
    EngineError => Engine,
    TxnError => Txn,
    MvccError => Mvcc,
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
    pub fn extract_error_header(self) -> ErrorHeader {
        match self {
            Error::Engine(EngineError::Request(e))
            | Error::Txn(TxnError::Engine(EngineError::Request(e)))
            | Error::Txn(TxnError::Mvcc(MvccError::Engine(EngineError::Request(e))))
            | Error::Request(e) => e,
            other => {
                let mut e = ErrorHeader::default();
                e.set_message(format!("{:?}", other));
                e
            }
        }
    }
}
