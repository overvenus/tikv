#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate failure;

mod delegate;
mod endpoint;
mod errors;
mod lock_scanner;
mod observer;
mod service;

pub use endpoint::Endpoint;
pub use errors::{Error, Result};
pub use observer::CdcObserver;
pub use service::Service;
