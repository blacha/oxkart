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
    ToParquet(commands::to_parquet::ToParquetArgs),
    /// List Kart datasets in a directory or Git repository
    List(commands::list::ListArgs),
    /// Show stats about Kart datasets
    Stats(commands::stats::StatsArgs),
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::ToParquet(args)) => {
            if let Err(e) = commands::to_parquet::run(args.clone()) {
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
        Some(Commands::Stats(args)) => {
            if let Err(e) = commands::stats::run(args.clone()) {
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
