#![allow(unused)]
#![allow(unused_imports)]

#[macro_use]
extern crate failure;

mod errors;
mod file;
mod manager;
mod master_key;

pub use self::errors::{Error, Result};
pub use self::file::File;
