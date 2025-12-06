use crate::commands;
use crate::source::KartSourceEnum;
use clap::Args;
use num_format::{Locale, ToFormattedString};
use rayon::prelude::*;
use serde::Serialize;
use std::path::Path;

#[derive(Args, Debug, Clone)]
pub struct StatsArgs {
    /// Path to search (default: current directory)
    #[arg(default_value = ".")]
    pub path: String,

    /// Git revision (branch, tag, sha) to read from (default: HEAD)
    #[arg(long)]
    pub git_rev: Option<String>,

    /// Output in JSON format
    #[arg(long)]
    pub json: bool,
}

#[derive(Serialize)]
struct DatasetStats {
    name: String,
    count: usize,
    size: u64,
}

pub fn run(args: StatsArgs) -> Result<(), Box<dyn std::error::Error>> {
    let (is_git, final_path) = commands::resolve_source_path(&args.path);

    let datasets = commands::find_datasets(&final_path, is_git, args.git_rev.as_deref())?;

    let mut stats = Vec::new();

    for (name, source) in datasets {
        let name_only = Path::new(&name)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        stats.push(calculate_stats(name_only, &source)?);
    }

    if args.json {
        for stat in stats {
            println!("{}", serde_json::to_string(&stat)?);
        }
    } else {
        for stat in stats {
            println!(
                "{: <50} {: >10} {: >15}",
                stat.name,
                stat.count.to_formatted_string(&Locale::en),
                humansize::format_size(stat.size, humansize::DECIMAL)
            );
        }
    }

    Ok(())
}

fn calculate_stats(
    name: String,
    source: &KartSourceEnum,
) -> Result<DatasetStats, Box<dyn std::error::Error>> {
    let identifiers: Vec<_> = source.feature_identifiers().collect();
    let count = identifiers.len();

    let size: u64 = identifiers
        .par_iter()
        .map(|id| source.get_feature_size(id).unwrap_or(0))
        .sum();

    Ok(DatasetStats { name, count, size })
}
