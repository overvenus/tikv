// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(slice_patterns)]
#![feature(proc_macro_hygiene)]

#[macro_use(
    slog_kv,
    slog_error,
    slog_info,
    slog_crit,
    slog_log,
    slog_record,
    slog_b,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;

use clap::{crate_authors, crate_version, App, Arg};
use engine::rocks;
use engine::rocks::util::metrics_flusher::{MetricsFlusher, DEFAULT_FLUSHER_INTERVAL};
use engine::rocks::util::security::encrypted_env_from_cipher_file;
use engine::Engines;
use fs2::FileExt;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tikv::binutil::setup::initial_logger;
use tikv::binutil::setup::*;
use tikv::binutil::signal_handler;
use tikv::binutil::{self, server, setup};
use tikv::config::check_and_persist_critical_config;
use tikv::config::TiKvConfig;
use tikv::fatal;
use tikv::raftstore::store::fsm;
use tikv::raftstore::store::{new_compaction_listener, SnapManagerBuilder};
use tikv::server::status_server::StatusServer;
use tikv::storage::DEFAULT_ROCKSDB_SUB_DIR;
use tikv_util::time::Monitor;

use restore::RestoreSystem;

fn main() {
    let matches = App::new("TiKV Restore")
        .about("A tool to retore TiKV cluster")
        .author(crate_authors!())
        .version(crate_version!())
        .long_version(binutil::tikv_version_info().as_ref())
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log-level")
                .short("L")
                .long("log-level")
                .alias("log")
                .takes_value(true)
                .value_name("LEVEL")
                .possible_values(&[
                    "trace", "debug", "info", "warn", "warning", "error", "critical",
                ])
                .help("Set the log level"),
        )
        .arg(
            Arg::with_name("log-file")
                .short("f")
                .long("log-file")
                .takes_value(true)
                .value_name("FILE")
                .help("Sets log file")
                .long_help("Set the log file path. If not set, logs will output to stderr"),
        )
        .arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the listening address"),
        )
        .arg(
            Arg::with_name("advertise-addr")
                .long("advertise-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the advertise listening address for client communication"),
        )
        .arg(
            Arg::with_name("status-addr")
                .long("status-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the HTTP listening address for the status report service"),
        )
        .arg(
            Arg::with_name("data-dir")
                .long("data-dir")
                .short("s")
                .alias("store")
                .takes_value(true)
                .value_name("PATH")
                .help("Set the directory used to store data"),
        )
        .arg(
            Arg::with_name("capacity")
                .long("capacity")
                .takes_value(true)
                .value_name("CAPACITY")
                .help("Set the store capacity")
                .long_help("Set the store capacity to use. If not set, use entire partition"),
        )
        .arg(
            Arg::with_name("pd-endpoints")
                .long("pd-endpoints")
                .aliases(&["pd", "pd-endpoint"])
                .takes_value(true)
                .value_name("PD_URL")
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .help("Sets PD endpoints")
                .long_help("Set the PD endpoints to use. Use `,` to separate multiple PDs"),
        )
        .arg(
            Arg::with_name("labels")
                .long("labels")
                .alias("label")
                .takes_value(true)
                .value_name("KEY=VALUE")
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .help("Sets server labels")
                .long_help(
                    "Set the server labels. Uses `,` to separate kv pairs, like \
                     `zone=cn,disk=ssd`",
                ),
        )
        .arg(
            Arg::with_name("metrics-addr")
                .long("metrics-addr")
                .value_name("IP:PORT")
                .help("Sets Prometheus Pushgateway address")
                .long_help(
                    "Sets push address to the Prometheus Pushgateway, \
                     leaves it empty will disable Prometheus push",
                ),
        )
        .arg(
            Arg::with_name("cluster_id")
                .long("cluster_id")
                .value_name("ID")
                .help("Sets Restored Cluster ID"),
        )
        .arg(
            Arg::with_name("store_id")
                .long("store_id")
                .value_name("ID")
                .help("Sets Restored Store ID"),
        )
        .arg(
            Arg::with_name("backup_path")
                .long("backup_path")
                .value_name("PATH")
                .help("Sets Backup Files Path"),
        )
        .get_matches();

    let mut config = matches
        .value_of("config")
        .map_or_else(TiKvConfig::default, |path| TiKvConfig::from_file(&path));

    setup::overwrite_config_with_cmd_args(&mut config, &matches);
    config.server.cluster_id = matches
        .value_of("cluster_id")
        .unwrap_or_else(|| {
            fatal!("empty cluster id",);
        })
        .parse()
        .unwrap_or_else(|e| fatal!("invalid cluster id: {}", e));
    let store_id = matches
        .value_of("store_id")
        .unwrap_or_else(|| {
            fatal!("empty store id",);
        })
        .parse()
        .unwrap_or_else(|e| {
            fatal!("invalid store id: {}", e);
        });
    let backup_path = matches.value_of("backup_path").unwrap_or_else(|| {
        fatal!("empty backup path",);
    });

    run_tikv_restore(config, store_id, backup_path);
}

fn run_tikv_restore(mut config: TiKvConfig, store_id: u64, path: &str) {
    if let Err(e) = check_and_persist_critical_config(&config) {
        fatal!("critical config check failed: {}", e);
    }

    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);
    tikv_util::set_panic_hook(false, &config.storage.data_dir);

    // Print version information.
    tikv::binutil::log_tikv_info();

    config.compatible_adjust();
    if let Err(e) = config.validate() {
        fatal!("invalid configuration: {}", e.description());
    }
    info!(
        "using config";
        "config" => serde_json::to_string(&config).unwrap(),
    );

    config.write_into_metrics();
    // Do some prepare works before start.
    server::pre_start(&config);

    let _m = Monitor::default();
    run_raft_server(&config, store_id, path);
}

fn run_raft_server(cfg: &TiKvConfig, store_id: u64, path: &str) {
    let store_path = Path::new(&cfg.storage.data_dir);
    let lock_path = store_path.join(Path::new("LOCK"));
    let db_path = store_path.join(Path::new(DEFAULT_ROCKSDB_SUB_DIR));
    let snap_path = store_path.join(Path::new("snap"));
    let raft_db_path = Path::new(&cfg.raft_store.raftdb_path);

    let f = File::create(lock_path.as_path())
        .unwrap_or_else(|e| fatal!("failed to create lock at {}: {}", lock_path.display(), e));
    if f.try_lock_exclusive().is_err() {
        fatal!(
            "lock {} failed, maybe another instance is using this directory.",
            store_path.display()
        );
    }

    if tikv_util::panic_mark_file_exists(&cfg.storage.data_dir) {
        fatal!(
            "panic_mark_file {} exists, there must be something wrong with the db.",
            tikv_util::panic_mark_file_path(&cfg.storage.data_dir).display()
        );
    }

    // Initialize raftstore channels.
    let (router, system) = fsm::create_raft_batch_system(&cfg.raft_store);

    let compaction_listener = new_compaction_listener(router.clone());

    // Create encrypted env from cipher file
    let encrypted_env = if !cfg.security.cipher_file.is_empty() {
        match encrypted_env_from_cipher_file(&cfg.security.cipher_file, None) {
            Err(e) => fatal!(
                "failed to create encrypted env from cipher file, err {:?}",
                e
            ),
            Ok(env) => Some(env),
        }
    } else {
        None
    };

    // Create block cache.
    let cache = cfg.storage.block_cache.build_shared_cache();

    // Create raft engine.
    let mut raft_db_opts = cfg.raftdb.build_opt();
    if let Some(ref ec) = encrypted_env {
        raft_db_opts.set_env(ec.clone());
    }
    let raft_db_cf_opts = cfg.raftdb.build_cf_opts(&cache);
    let raft_engine = rocks::util::new_engine_opt(
        raft_db_path.to_str().unwrap(),
        raft_db_opts,
        raft_db_cf_opts,
    )
    .unwrap_or_else(|s| fatal!("failed to create raft engine: {}", s));

    // Create kv engine, storage.
    let mut kv_db_opts = cfg.rocksdb.build_opt();
    kv_db_opts.add_event_listener(compaction_listener);
    if let Some(ec) = encrypted_env {
        kv_db_opts.set_env(ec);
    }

    // Create kv engine, storage.
    let kv_cfs_opts = cfg.rocksdb.build_cf_opts(&cache);
    let kv_engine = rocks::util::new_engine_opt(db_path.to_str().unwrap(), kv_db_opts, kv_cfs_opts)
        .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));

    let engines = Engines::new(Arc::new(kv_engine), Arc::new(raft_engine), cache.is_some());

    // Create snapshot manager, server.
    let snap_mgr = SnapManagerBuilder::default()
        .max_write_bytes_per_sec(cfg.server.snap_max_write_bytes_per_sec.0)
        .max_total_size(cfg.server.snap_max_total_size.0)
        .build(
            snap_path.as_path().to_str().unwrap().to_owned(),
            Some(router.clone()),
            None,
        );

    info!("create restore system";
        "cluster_id" => cfg.server.cluster_id,
        "store_id" => store_id);

    // Create restore system.
    let mut restore_system = RestoreSystem::new(
        cfg.server.cluster_id,
        store_id,
        cfg.raft_store.clone(),
        engines.clone(),
        router.clone(),
        system,
        snap_mgr,
    );
    restore_system
        .bootstrap()
        .unwrap_or_else(|s| fatal!("failed to bootstrap restore system: {}", s));

    initial_metric(&cfg.metric, Some(store_id));

    let mut metrics_flusher = MetricsFlusher::new(
        engines.clone(),
        Duration::from_millis(DEFAULT_FLUSHER_INTERVAL),
    );

    // Start metrics flusher
    if let Err(e) = metrics_flusher.start() {
        error!(
            "failed to start metrics flusher";
            "err" => %e
        );
    }

    let server_cfg = cfg.server.clone();
    let mut status_enabled = cfg.metric.address.is_empty() && !server_cfg.status_addr.is_empty();

    // Create a status server.
    let mut status_server = StatusServer::new(server_cfg.status_thread_pool_size);
    if status_enabled {
        // Start the status server.
        if let Err(e) = status_server.start(server_cfg.status_addr) {
            error!(
                "failed to bind addr for status service";
                "err" => %e
            );
            status_enabled = false;
        }
    }

    let apply_notify = restore_system
        .start()
        .unwrap_or_else(|s| fatal!("failed to start restore system: {}", s));

    let p = Path::new(path);
    let runner = restore::Runner::new(router, apply_notify, store_id, &snap_path);
    let storage = backup::LocalStorage::new(p)
        .unwrap_or_else(|e| fatal!("failed to create local storage: {}", e));
    let manager = backup::RestoreManager::new(p.to_owned(), Arc::new(storage))
        .unwrap_or_else(|e| fatal!("failed to create restore manager: {}", e));
    let executor = manager
        .executor()
        .unwrap_or_else(|e| fatal!("failed to create restore exector: {}", e));
    executor.execute(runner);

    signal_handler::handle_signal(Some(engines));

    // Stop.
    restore_system
        .stop()
        .unwrap_or_else(|e| fatal!("failed to stop server: {}", e));

    if status_enabled {
        // Stop the status server.
        status_server.stop()
    }

    metrics_flusher.stop();
}
