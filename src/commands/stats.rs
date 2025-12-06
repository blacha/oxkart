use clap::Args;
use git2::{ObjectType, Repository};
use num_format::{Locale, ToFormattedString};
use rayon::prelude::*;
use serde::Serialize;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

use crate::source::{FsGit, FsSource, KartSourceEnum};

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
    let path = Path::new(&args.path);
    let kart_dir = path.join(".kart");
    let git_dir = path.join(".git");

    let (is_git, final_path) = if args.path.ends_with(".git")
        || args.path.ends_with(".kart")
        || args.path.ends_with(".git/")
        || args.path.ends_with(".kart/")
    {
        (true, args.path.clone())
    } else if kart_dir.exists() && kart_dir.is_dir() {
        (true, kart_dir.to_string_lossy().to_string())
    } else if git_dir.exists() && git_dir.is_dir() {
        (true, git_dir.to_string_lossy().to_string())
    } else {
        (false, args.path.clone())
    };

    let stats = if is_git {
        stats_git(&final_path, args.git_rev.as_deref())?
    } else {
        stats_fs(&final_path)?
    };

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

fn stats_fs(path: &str) -> Result<Vec<DatasetStats>, Box<dyn std::error::Error>> {
    let root = Path::new(path);
    let mut stats = Vec::new();

    let mut it = WalkDir::new(root).into_iter();
    loop {
        let entry = match it.next() {
            None => break,
            Some(Err(_)) => continue,
            Some(Ok(entry)) => entry,
        };

        if entry.file_type().is_dir() {
            let table_dataset = entry.path().join(".table-dataset");
            if table_dataset.exists() {
                let source = KartSourceEnum::Fs(FsSource::new(entry.path()));
                let name = entry
                    .path()
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                stats.push(calculate_stats(name, &source)?);
                it.skip_current_dir();
                continue;
            }
        }
    }

    Ok(stats)
}

fn stats_git(
    path: &str,
    rev: Option<&str>,
) -> Result<Vec<DatasetStats>, Box<dyn std::error::Error>> {
    let repo_path = PathBuf::from(path);
    let repo = Repository::open_bare(&repo_path).or_else(|_| Repository::open(&repo_path))?;

    let obj = if let Some(r) = rev {
        repo.revparse_single(r)?
    } else {
        let head = repo.head()?;
        head.peel_to_commit()?.into_object()
    };

    let tree = obj.peel_to_tree()?;
    let mut stats = Vec::new();

    walk_git_tree(&repo, &tree, Path::new(""), path, rev, &mut stats)?;

    Ok(stats)
}

fn walk_git_tree(
    repo: &Repository,
    tree: &git2::Tree,
    path: &Path,
    repo_path: &str,
    rev: Option<&str>,
    stats: &mut Vec<DatasetStats>,
) -> Result<(), Box<dyn std::error::Error>> {
    if tree.get_name(".table-dataset").is_some() {
        let name = if path.as_os_str().is_empty() {
            Path::new(repo_path)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string()
        } else {
            path.file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string()
        };
        let source = KartSourceEnum::Git(FsGit::new(repo_path, rev, path.to_str())?);
        stats.push(calculate_stats(name, &source)?);
        return Ok(()); // Stop recursing into this dataset
    }

    for entry in tree.iter() {
        if entry.kind() == Some(ObjectType::Tree) {
            if let Some(name) = entry.name() {
                let object = entry.to_object(repo)?;
                if let Some(sub_tree) = object.as_tree() {
                    walk_git_tree(repo, sub_tree, &path.join(name), repo_path, rev, stats)?;
                }
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
        .map(|id| source.get_feature_size(id).unwrap_or(0))
        .sum();

    Ok(DatasetStats { name, count, size })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_stats_fs_empty() {
        let temp_dir = TempDir::new().unwrap();
        let stats = stats_fs(temp_dir.path().to_str().unwrap()).unwrap();
        assert!(stats.is_empty());
    }

    #[test]
    fn test_stats_fs_simple() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = temp_dir.path().join("dataset1");
        fs::create_dir(&dataset_path).unwrap();
        fs::create_dir(dataset_path.join(".table-dataset")).unwrap();

        // Mock feature directory and files
        let feature_dir = dataset_path.join(".table-dataset/feature");
        fs::create_dir(&feature_dir).unwrap();

        let feature1 = feature_dir.join("f1");
        let feature2 = feature_dir.join("f2");

        // Create 2 files with known size (10 bytes each)
        let content = vec![0u8; 10];
        fs::write(&feature1, &content).unwrap();
        fs::write(&feature2, &content).unwrap();

        let stats = stats_fs(temp_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(stats.len(), 1);

        let stat = &stats[0];
        assert_eq!(stat.name, "dataset1");
        assert_eq!(stat.count, 2);
        assert_eq!(stat.size, 20);
    }
}
