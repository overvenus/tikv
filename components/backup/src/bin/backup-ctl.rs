// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::*;
use std::u64;

use clap::{crate_authors, crate_version, App, AppSettings, Arg, SubCommand};
use kvproto::raft_cmdpb::RaftCmdRequest;
use protobuf::Message;

use backup::*;

fn main() {
    let mut app = App::new("TiKV Control (tikv-ctl)")
        .about("A tool for interacting with TiKV deployments.")
        .author(crate_authors!())
        .version(crate_version!())
        .setting(AppSettings::AllowExternalSubcommands)
        .subcommand(
            SubCommand::with_name("backup")
                .about("Backup a TiKV cluster")
                .arg(
                    Arg::with_name("path")
                        .short("p")
                        .long("path")
                        .takes_value(true)
                        .required(true)
                        .help("the path of backup location"),
                )
                .arg(
                    Arg::with_name("base")
                        .short("b")
                        .long("base")
                        .takes_value(true)
                        .required(true)
                        .help("the base path of backup"),
                )
                .subcommand(SubCommand::with_name("check").about("Check backup meta information"))
                .subcommand(
                    SubCommand::with_name("meta")
                        .about("Display backup meta information")
                        .arg(
                            Arg::with_name("region")
                                .short("r")
                                .long("region")
                                .takes_value(true)
                                .help("Only display events of the region"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("data")
                        .about("Dump the content of backup data (snapshots and raft logs)")
                        .arg(
                            Arg::with_name("region")
                                .short("r")
                                .long("region")
                                .takes_value(true)
                                .help("The id of a region"),
                        )
                        .subcommand(
                            SubCommand::with_name("log")
                                .arg(
                                    Arg::with_name("first")
                                        .short("f")
                                        .long("first")
                                        .takes_value(true),
                                )
                                .arg(
                                    Arg::with_name("last")
                                        .short("l")
                                        .long("last")
                                        .takes_value(true),
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("graph")
                        .about("Display a graph of a backup")
                        .subcommand(
                            SubCommand::with_name("total").about("Display a total order graph"),
                        ),
                ),
        );

    let matches = app.clone().get_matches();
    if let Some(matches) = matches.subcommand_matches("backup") {
        use std::path::Path;
        let p = matches.value_of("path").unwrap();
        let path = Path::new(p);
        let b = matches.value_of("base").unwrap();
        let base = Path::new(b);
        let ls = LocalStorage::new(path).unwrap();
        // TODO: how to use base in backup manager?
        let bm = BackupManager::new(0, path, Box::new(ls.clone())).unwrap();
        if let Some(matches) = matches.subcommand_matches("meta") {
            let meta = bm.backup_meta();
            if let Some(v) = matches.value_of("region") {
                let id: u64 = v.parse().unwrap();
                println!("backup meta for region {}:", id);
                for e in meta.get_events() {
                    if e.get_region_id() == id {
                        println!("{:?}", e);
                    }
                }
            } else {
                println!("backup meta:\n{:#?}", meta);
            }
        } else if matches.subcommand_matches("check").is_some() {
            match bm.check_meta() {
                Ok(events) => {
                    println!("Backup meta is good!");
                    if let Err(e) = bm.check_data(events) {
                        println!("Bad backup data detected:\n{}", e);
                    } else {
                        println!("Backup data is good!");
                    }
                }
                Err(e) => {
                    println!("Bad backup meta detected:\n{}", e);
                }
            }
        } else if let Some(matches) = matches.subcommand_matches("data") {
            let id: u64 = matches.value_of("region").unwrap().parse().unwrap();
            if let Some(matches) = matches.subcommand_matches("log") {
                let first: u64 = matches.value_of("first").unwrap().parse().unwrap();
                let last: u64 = matches.value_of("last").unwrap().parse().unwrap();
                let mut buf = vec![];
                bm.storage
                    .read_file(&bm.log_path(id, first, last), &mut buf)
                    .unwrap();
                let mut entry_batch = kvproto::backup::EntryBatch::new();
                entry_batch.merge_from_bytes(&buf).unwrap();
                println!("Entries [{}, {}] for region {}", first, last, id);
                for e in entry_batch.get_entries() {
                    let mut cmd = RaftCmdRequest::new();
                    cmd.merge_from_bytes(e.get_data()).unwrap();
                    println!("entry {:?}\ndata: {:?}", e, cmd);
                }
            }
        } else if let Some(matches) = matches.subcommand_matches("graph") {
            let rm = backup::RestoreManager::new(base.to_owned(), Arc::new(ls)).unwrap();
            if matches.subcommand_matches("total").is_some() {
                let g = rm.total_order_eval().unwrap();
                println!("{}", backup::dot(&g.0));
                println!("{:?}", g.1);
            } else {
                let g = rm.eval_graph().unwrap();
                println!("{}", backup::dot(&g));
            }
        }
    } else {
        let _ = app.print_help();
    }
}
