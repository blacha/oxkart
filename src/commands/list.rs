use crate::source::{FsGit, FsSource, KartSchema, KartSourceEnum};
use clap::Args;
use git2::{ObjectType, Repository, Tree};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

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

    let datasets = if is_git {
        find_git_datasets(&final_path, args.git_rev.as_deref(), args.schema)?
    } else {
        find_fs_datasets(&final_path, args.schema)?
    };

    for (name, schema) in datasets {
        println!("{}", name);
        if let Some(schema) = schema {
            println!("{}", serde_json::to_string_pretty(&schema)?);
        }
    }

    Ok(())
}

fn find_fs_datasets(
    path: &str,
    show_schema: bool,
) -> Result<Vec<(String, Option<KartSchema>)>, Box<dyn std::error::Error>> {
    let root = Path::new(path);
    let mut datasets = Vec::new();

    // Walk directory and look for .table-dataset directories
    // If we find a directory that contains .table-dataset, we print it and do NOT descend into it.

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
                let name = entry.path().display().to_string();
                let schema = if show_schema {
                    let source = KartSourceEnum::Fs(FsSource::new(entry.path()));
                    Some(source.get_schema()?)
                } else {
                    None
                };
                datasets.push((name, schema));
                it.skip_current_dir();
                continue;
            }
        }
    }

    Ok(datasets)
}

fn find_git_datasets(
    path: &str,
    rev: Option<&str>,
    show_schema: bool,
) -> Result<Vec<(String, Option<KartSchema>)>, Box<dyn std::error::Error>> {
    let repo_path = PathBuf::from(path);
    let repo = Repository::open_bare(&repo_path).or_else(|_| Repository::open(&repo_path))?;

    let obj = if let Some(r) = rev {
        repo.revparse_single(r)?
    } else {
        let head = repo.head()?;
        head.peel_to_commit()?.into_object()
    };

    let tree = obj.peel_to_tree()?;
    let mut datasets = Vec::new();

    walk_git_tree(
        &repo,
        &tree,
        Path::new(""),
        path,
        rev,
        show_schema,
        &mut datasets,
    )?;

    Ok(datasets)
}

fn walk_git_tree(
    repo: &Repository,
    tree: &Tree,
    path: &Path,
    repo_path: &str,
    rev: Option<&str>,
    show_schema: bool,
    datasets: &mut Vec<(String, Option<KartSchema>)>,
) -> Result<(), Box<dyn std::error::Error>> {
    if tree.get_name(".table-dataset").is_some() {
        let display_path = if path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            path
        };
        let name = display_path.display().to_string();
        let schema = if show_schema {
            let source = KartSourceEnum::Git(FsGit::new(repo_path, rev, path.to_str())?);
            Some(source.get_schema()?)
        } else {
            None
        };
        datasets.push((name, schema));
        return Ok(()); // Stop recursing into this dataset
    }

    for entry in tree.iter() {
        if entry.kind() == Some(ObjectType::Tree) {
            if let Some(name) = entry.name() {
                let object = entry.to_object(repo)?;
                if let Some(sub_tree) = object.as_tree() {
                    walk_git_tree(
                        repo,
                        sub_tree,
                        &path.join(name),
                        repo_path,
                        rev,
                        show_schema,
                        datasets,
                    )?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_find_fs_datasets_empty() {
        let temp_dir = TempDir::new().unwrap();
        let datasets = find_fs_datasets(temp_dir.path().to_str().unwrap(), false).unwrap();
        assert!(datasets.is_empty());
    }

    #[test]
    fn test_find_fs_datasets_simple() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = temp_dir.path().join("dataset1");
        fs::create_dir(&dataset_path).unwrap();
        fs::create_dir(dataset_path.join(".table-dataset")).unwrap();

        let datasets = find_fs_datasets(temp_dir.path().to_str().unwrap(), false).unwrap();
        assert_eq!(datasets.len(), 1);
        assert!(datasets[0].0.ends_with("dataset1"));
        assert!(datasets[0].1.is_none());
    }

    #[test]
    fn test_find_fs_datasets_nested() {
        let temp_dir = TempDir::new().unwrap();
        let group_path = temp_dir.path().join("group");
        fs::create_dir(&group_path).unwrap();

        let dataset1_path = group_path.join("dataset1");
        fs::create_dir(&dataset1_path).unwrap();
        fs::create_dir(dataset1_path.join(".table-dataset")).unwrap();

        let dataset2_path = temp_dir.path().join("dataset2");
        fs::create_dir(&dataset2_path).unwrap();
        fs::create_dir(dataset2_path.join(".table-dataset")).unwrap();

        // Should be ignored because it is inside a dataset
        let nested_ignored_path = dataset2_path.join("ignored");
        fs::create_dir(&nested_ignored_path).unwrap();
        fs::create_dir(nested_ignored_path.join(".table-dataset")).unwrap();

        let datasets = find_fs_datasets(temp_dir.path().to_str().unwrap(), false).unwrap();
        assert_eq!(datasets.len(), 2);

        let names: Vec<_> = datasets.iter().map(|d| d.0.clone()).collect();
        assert!(names.iter().any(|n| n.ends_with("dataset1")));
        assert!(names.iter().any(|n| n.ends_with("dataset2")));
    }

    #[test]
    fn test_find_fs_datasets_with_schema() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = temp_dir.path().join("dataset1");
        fs::create_dir_all(dataset_path.join(".table-dataset/meta")).unwrap();

        let schema_json =
            r#"[{"id": "col1", "name": "Column 1", "dataType": "string", "geometryCRS": null}]"#;
        fs::write(
            dataset_path.join(".table-dataset/meta/schema.json"),
            schema_json,
        )
        .unwrap();

        let datasets = find_fs_datasets(temp_dir.path().to_str().unwrap(), true).unwrap();
        assert_eq!(datasets.len(), 1);
        assert!(datasets[0].1.is_some());

        let schema = datasets[0].1.as_ref().unwrap();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].name, "Column 1");
    }
}
