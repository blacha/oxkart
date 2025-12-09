use clap::{CommandFactory, Parser, Subcommand};

mod commands;
mod source;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Convert Kart dataset to GeoParquet
    Export(commands::export::ExportArgs),
    /// Import Parquet file to Kart dataset
    Import(commands::import::ImportArgs),
    /// List Kart datasets in a directory or Git repository
    List(commands::list::ListArgs),
    /// Re-index a Kart repository
    Reindex(commands::reindex::ReindexArgs),
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Export(args)) => {
            if let Err(e) = commands::export::run(args.clone()) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Some(Commands::Import(args)) => {
            if let Err(e) = commands::import::run(args.clone()) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }

        Some(Commands::List(args)) => {
            if let Err(e) = commands::list::run(args.clone()) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Some(Commands::Reindex(args)) => {
            if let Err(e) = commands::reindex::run(args.clone()) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        None => {
            let mut cmd = Cli::command();
            cmd.print_help().unwrap();
        }
    }
}
