// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Importing RocksDB SST files into TiKV
#![feature(min_specialization)]
#![feature(let_chains)]
#![feature(assert_matches)]

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate tikv_util;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

mod config;
mod errors;
mod import_file;
mod sst_writer;
mod util;
#[macro_use]
pub mod import_mode;
mod caching;
pub mod import_mode2;
mod mediate;
pub mod metrics;
pub mod sst_importer;

pub use self::{
    config::{Config, ConfigManager},
    errors::{error_inc, Error, Result},
    import_file::{sst_meta_to_path, API_VERSION_2},
    import_mode2::range_overlaps,
    mediate::{periodic_gc_mediator, IngestMediator, IngestObserver, Mediator, Observer},
    sst_importer::SstImporter,
    sst_writer::{RawSstWriter, TxnSstWriter},
    util::{copy_sst_for_ingestion, prepare_sst_for_ingestion},
};
