use crate::commands;
use crate::source::KartSourceEnum;
use clap::Args;
use num_format::{Locale, ToFormattedString};
use rayon::prelude::*;
use serde::Serialize;
use std::path::Path;

#[derive(Args, Debug, Clone)]
pub struct ListArgs {
    /// Path to search (default: current directory)
    #[arg(default_value = ".")]
    pub path: String,

    /// Git revision (branch, tag, sha) to read from (default: HEAD)
    #[arg(long)]
    pub git_rev: Option<String>,

    /// Output the dataset schema
    #[arg(long)]
    pub schema: bool,

    /// Output dataset statistics (count, size)
    #[arg(long)]
    pub stats: bool,

    /// Output in JSON format (NDJSON)
    #[arg(long)]
    pub json: bool,
}

#[derive(Serialize)]
struct DatasetStats {
    name: String,
    count: usize,
    size: u64,
}

#[derive(Serialize)]
struct DatasetOutput {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<serde_json::Value>,
}

pub fn run(args: ListArgs) -> Result<(), Box<dyn std::error::Error>> {
    let (is_git, final_path) = commands::resolve_source_path(&args.path);

    let datasets = commands::find_datasets(&final_path, is_git, args.git_rev.as_deref())?;

    for (name, source) in datasets {
        let name_only = Path::new(&name)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let mut count = None;
        let mut size = None;

        if args.stats {
            let stats = calculate_stats(name_only.clone(), &source)?;
            count = Some(stats.count);
            size = Some(stats.size);
        }

        let mut schema = None;
        if args.schema {
            schema = Some(serde_json::to_value(
                source
                    .get_schema()
                    .map_err(|e| e as Box<dyn std::error::Error>)?,
            )?);
        }

        if args.json {
            let output = DatasetOutput {
                name: name_only,
                count,
                size,
                schema,
            };
            println!("{}", serde_json::to_string(&output)?);
        } else {
            // Text output
            if let (Some(c), Some(s)) = (count, size) {
                println!(
                    "{: <50} {: >10} {: >15}",
                    name_only,
                    c.to_formatted_string(&Locale::en),
                    humansize::format_size(s, humansize::DECIMAL)
                );
            } else {
                println!("{}", name);
            }

            if let Some(sch) = schema {
                println!("{}", serde_json::to_string_pretty(&sch)?);
            }
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
        .map(|(_filename, id)| source.get_feature_size(id).unwrap_or(0))
        .sum();

    Ok(DatasetStats { name, count, size })
}
