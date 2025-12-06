pub mod list;
pub mod stats;
pub mod to_parquet;

use crate::source::{FsGit, FsSource, KartSourceEnum};
use git2::{ObjectType, Repository, Tree};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

pub fn resolve_source_path(path_str: &str) -> (bool, String) {
    let path = Path::new(path_str);
    let kart_dir = path.join(".kart");
    let git_dir = path.join(".git");

    if path_str.ends_with(".git")
        || path_str.ends_with(".kart")
        || path_str.ends_with(".git/")
        || path_str.ends_with(".kart/")
    {
        (true, path_str.to_string())
    } else if kart_dir.exists() && kart_dir.is_dir() {
        (true, kart_dir.to_string_lossy().to_string())
    } else if git_dir.exists() && git_dir.is_dir() {
        (true, git_dir.to_string_lossy().to_string())
    } else {
        (false, path_str.to_string())
    }
}

pub fn find_datasets(
    path: &str,
    is_git: bool,
    rev: Option<&str>,
) -> Result<Vec<(String, KartSourceEnum)>, Box<dyn std::error::Error>> {
    if is_git {
        find_git_datasets(path, rev)
    } else {
        find_fs_datasets(path)
    }
}

fn find_fs_datasets(
    path: &str,
) -> Result<Vec<(String, KartSourceEnum)>, Box<dyn std::error::Error>> {
    let root = Path::new(path);
    let mut datasets = Vec::new();

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
                let source = KartSourceEnum::Fs(FsSource::new(entry.path()));
                datasets.push((name, source));
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
) -> Result<Vec<(String, KartSourceEnum)>, Box<dyn std::error::Error>> {
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

    walk_git_tree(&repo, &tree, Path::new(""), path, rev, &mut datasets)?;

    Ok(datasets)
}

fn walk_git_tree(
    repo: &Repository,
    tree: &Tree,
    path: &Path,
    repo_path: &str,
    rev: Option<&str>,
    datasets: &mut Vec<(String, KartSourceEnum)>,
) -> Result<(), Box<dyn std::error::Error>> {
    if tree.get_name(".table-dataset").is_some() {
        let display_path = if path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            path
        };
        let name = display_path.display().to_string();
        let source = KartSourceEnum::Git(FsGit::new(repo_path, rev, path.to_str())?);
        datasets.push((name, source));
        return Ok(());
    }

    for entry in tree.iter() {
        if entry.kind() == Some(ObjectType::Tree) {
            if let Some(name) = entry.name() {
                let object = entry.to_object(repo)?;
                if let Some(sub_tree) = object.as_tree() {
                    walk_git_tree(repo, sub_tree, &path.join(name), repo_path, rev, datasets)?;
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
        let datasets = find_fs_datasets(temp_dir.path().to_str().unwrap()).unwrap();
        assert!(datasets.is_empty());
    }

    #[test]
    fn test_find_fs_datasets_simple() {
        let temp_dir = TempDir::new().unwrap();
        let dataset_path = temp_dir.path().join("dataset1");
        fs::create_dir(&dataset_path).unwrap();
        fs::create_dir(dataset_path.join(".table-dataset")).unwrap();

        let datasets = find_fs_datasets(temp_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(datasets.len(), 1);
        assert!(datasets[0].0.ends_with("dataset1"));
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

        let datasets = find_fs_datasets(temp_dir.path().to_str().unwrap()).unwrap();
        assert_eq!(datasets.len(), 2);

        let names: Vec<_> = datasets.iter().map(|d| d.0.clone()).collect();
        assert!(names.iter().any(|n| n.ends_with("dataset1")));
        assert!(names.iter().any(|n| n.ends_with("dataset2")));
    }
}
