// main.rs

// Import necessary crates for Git operations and command-line argument parsing.
use clap::{Parser, Subcommand};
use git2::{Repository, TreeWalkMode, TreeWalkResult};
use std::path::PathBuf;

mod commands;
mod source;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// A simple Rust CLI to open a bare Git repository and list all its files.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to the bare Git repository (e.g., /path/to/my_repo.git)
    /// (Optional if using a subcommand)
    #[clap(short, long, value_parser)]
    repo_path: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Convert Kart dataset to GeoParquet
    ToParquet(commands::to_parquet::ToParquetArgs),
    /// List Kart datasets in a directory or Git repository
    List(commands::list::ListArgs),
}

fn main() {
    // Parse command-line arguments.
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::ToParquet(args)) => {
            if let Err(e) = commands::to_parquet::run(args.clone()) {
                // Clone args as run takes ownership or update run signature?
                // run takes ownership in to_parquet.rs: pub fn run(args: ToParquetArgs)
                // We should probably clone or change run to take reference.
                // ToParquetArgs derives Clone? No, it derives Args which derives Clone usually?
                // clap::Args derives Clone.
                // Let's check if ToParquetArgs derives Clone. It derives Args, Debug.
                // We might need to add Clone to ToParquetArgs.
                // Or just move args if we can. But cli is borrowed? No, cli is owned here.
                // match &cli.command borrows cli.
                // We can match cli.command to move.
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Some(Commands::List(args)) => {
            if let Err(e) = commands::list::run(args.clone()) {
                // Need Clone for ListArgs too
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        None => {
            // Default behavior: List files in repo
            // ... (existing code)
            let repo_path = cli.repo_path.unwrap_or_else(|| PathBuf::from("."));
            if let Err(e) = list_files(&repo_path) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

// Placeholder for list_files function to make the code syntactically correct.
// This function would typically be defined in `commands/list.rs` or similar.
fn list_files(repo_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let repo = Repository::open_bare(repo_path).or_else(|_| Repository::open(repo_path))?;
    let head = repo.head()?;
    let commit = head.peel_to_commit()?;
    let tree = commit.tree()?;

    tree.walk(TreeWalkMode::PreOrder, |root, entry| {
        if let Some(name) = entry.name() {
            println!("{}{}", root, name);
        }
        TreeWalkResult::Ok
    })?;

    Ok(())
}
