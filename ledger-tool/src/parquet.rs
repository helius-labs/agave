use crate::output::output_slot_wrapper;
use solana_ledger::parquet_upload::upload_confirmed_blocks;
use std::collections::HashMap;
use std::path::PathBuf;
use {
    crate::{
        args::{load_genesis_arg, snapshot_args},
        ledger_path::canonicalize_ledger_path,
        load_and_process_ledger_or_exit, open_genesis_config_by,
        output::*,
        parse_process_options, LoadAndProcessLedgerOutput,
    },
    clap::{
        value_t, value_t_or_exit, values_t_or_exit, App, AppSettings, Arg, ArgMatches, SubCommand,
    },
    crossbeam_channel::unbounded,
    futures::stream::FuturesUnordered,
    log::{debug, error, info, warn},
    serde_json::json,
    solana_clap_utils::{
        input_parsers::pubkey_of,
        input_validators::{is_parsable, is_slot, is_valid_pubkey},
    },
    solana_cli_output::{
        display::println_transaction, CliBlock, CliTransaction, CliTransactionConfirmation,
        OutputFormat,
    },
    solana_entry::entry::{create_ticks, Entry},
    solana_ledger::{
        blockstore::Blockstore,
        blockstore_options::AccessType,
        parquet_upload::ConfirmedBlockUploadConfig,
        shred::{ProcessShredsStats, ReedSolomonCache, Shredder},
    },
    solana_sdk::{
        clock::Slot, hash::Hash, pubkey::Pubkey, shred_version::compute_shred_version,
        signature::Signature, signer::keypair::keypair_from_seed,
    },
    solana_storage_bigtable::CredentialType,
    solana_transaction_status::{ConfirmedBlock, UiTransactionEncoding, VersionedConfirmedBlock},
    std::{
        cmp::min,
        collections::HashSet,
        path::Path,
        process::exit,
        result::Result,
        str::FromStr,
        sync::{atomic::AtomicBool, Arc, Mutex},
    },
};
async fn upload(
    blockstore: Blockstore,
    starting_slot: Option<Slot>,
    ending_slot: Option<Slot>,
    force_reupload: bool,
    config: solana_storage_bigtable::LedgerStorageConfig,
    output_dir: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = ConfirmedBlockUploadConfig {
        force_reupload,
        ..ConfirmedBlockUploadConfig::default()
    };
    let blockstore = Arc::new(blockstore);

    let mut starting_slot = match starting_slot {
        Some(slot) => slot,
        // It is possible that the slot returned below could get purged by
        // LedgerCleanupService before upload_confirmed_blocks() receives the
        // value. This is ok because upload_confirmed_blocks() doesn't need
        // the exact slot to be in ledger, the slot is only used as a bound.
        None => blockstore.get_first_available_block()?,
    };

    let ending_slot = ending_slot.unwrap_or_else(|| blockstore.max_root());

    // while starting_slot <= ending_slot {
    //     let current_ending_slot = min(
    //         ending_slot,
    //         starting_slot.saturating_add(config.max_num_slots_to_check as u64 * 2),
    //     );
    //     let last_slot_checked = solana_ledger::parquet_upload::upload_confirmed_blocks(
    //         blockstore.clone(),
    //         starting_slot,
    //         current_ending_slot,
    //         config.clone(),
    //         Arc::new(AtomicBool::new(false)),
    //     )
    //     .await?;
    //     info!("last slot checked: {}", last_slot_checked);
    //     starting_slot = last_slot_checked.saturating_add(1);
    // }
    output_slot_wrapper(&blockstore, starting_slot, output_dir)?;

    info!("No more blocks to upload.");
    Ok(())
}

fn get_global_subcommand_arg<T: FromStr>(
    matches: &ArgMatches<'_>,
    sub_matches: Option<&clap::ArgMatches>,
    name: &str,
    default: &str,
) -> T {
    // this is kinda stupid, but there seems to be a bug in clap when a subcommand
    // arg is marked both `global(true)` and `default_value("default_value")`.
    // despite the "global", when the arg is specified on the subcommand, its value
    // is not propagated down to the (sub)subcommand args, resulting in the default
    // value when queried there. similarly, if the arg is specified on the
    // (sub)subcommand, the value is not propagated back up to the subcommand args,
    // again resulting in the default value. the arg having declared a
    // `default_value()` obviates `is_present(...)` tests since they will always
    // return true. so we consede and compare against the expected default. :/
    let on_command = matches
        .value_of(name)
        .map(|v| v != default)
        .unwrap_or(false);
    if on_command {
        value_t_or_exit!(matches, name, T)
    } else {
        let sub_matches = sub_matches.as_ref().unwrap();
        value_t_or_exit!(sub_matches, name, T)
    }
}

pub fn parquet_process_command(ledger_path: &Path, matches: &ArgMatches<'_>) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let verbose = matches.is_present("verbose");
    let output_format = OutputFormat::from_matches(matches, "output_format", verbose);

    let (subcommand, sub_matches) = matches.subcommand();

    let future = match (subcommand, sub_matches) {
        ("upload", Some(arg_matches)) => {
            let starting_slot = value_t!(arg_matches, "starting_slot", Slot).ok();
            let ending_slot = value_t!(arg_matches, "ending_slot", Slot).ok();
            let output_dir = value_t!(arg_matches, "output_dir", PathBuf).ok();
            let force_reupload = arg_matches.is_present("force_reupload");
            let blockstore = crate::open_blockstore(
                &canonicalize_ledger_path(ledger_path),
                arg_matches,
                AccessType::Secondary,
            );
            let config = solana_storage_bigtable::LedgerStorageConfig {
                read_only: false,
                instance_name: "".to_string(),
                app_profile_id: "".to_string(),
                ..solana_storage_bigtable::LedgerStorageConfig::default()
            };
            runtime.block_on(upload(
                blockstore,
                starting_slot,
                ending_slot,
                force_reupload,
                config,
                output_dir.unwrap(),
            ))
        }
        _ => unreachable!(),
    };

    future.unwrap_or_else(|err| {
        eprintln!("{err:?}");
        exit(1);
    });
}

pub trait ParquetSubCommand {
    fn parquet_subcommand(self) -> Self;
}

impl ParquetSubCommand for App<'_, '_> {
    fn parquet_subcommand(self) -> Self {
        self.subcommand(
            SubCommand::with_name("parquet")
                .about("Ledger data on a BigTable instance")
                .setting(AppSettings::InferSubcommands)
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommand(
                    SubCommand::with_name("upload")
                        .about("Upload the ledger to BigTable")
                        .arg(
                            Arg::with_name("starting_slot")
                                .long("starting-slot")
                                .validator(is_slot)
                                .value_name("START_SLOT")
                                .takes_value(true)
                                .index(1)
                                .help(
                                    "Start uploading at this slot [default: first available slot]",
                                ),
                        )
                        .arg(
                            Arg::with_name("ending_slot")
                                .long("ending-slot")
                                .validator(is_slot)
                                .value_name("END_SLOT")
                                .takes_value(true)
                                .index(2)
                                .help("Stop uploading at this slot [default: last available slot]"),
                        )
                        .arg(
                            Arg::with_name("output_dir")
                                .long("output-dir")
                                .value_name("OUTPUT_DIR")
                                .takes_value(true)
                                .help("Directory to output csvs to"),
                        )
                        .arg(
                            Arg::with_name("force_reupload")
                                .long("force")
                                .takes_value(false)
                                .help(
                                    "Force reupload of any blocks already present in BigTable \
                                     instance. Note: reupload will *not* delete any data from the \
                                     tx-by-addr table; Use with care.",
                                ),
                        ),
                ),
        )
    }
}
