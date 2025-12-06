use crate::commands;
use clap::Args;

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
}

pub fn run(args: ListArgs) -> Result<(), Box<dyn std::error::Error>> {
    let (is_git, final_path) = commands::resolve_source_path(&args.path);

    let datasets = commands::find_datasets(&final_path, is_git, args.git_rev.as_deref())?;

    for (name, source) in datasets {
        println!("{}", name);
        if args.schema {
            let schema = source.get_schema()?;
            println!("{}", serde_json::to_string_pretty(&schema)?);
        }
    }

    Ok(())
}
