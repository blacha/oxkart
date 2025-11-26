use clap::Args;
use git2::{ObjectType, Repository};
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

    if is_git {
        list_git(&final_path, args.git_rev.as_deref())
    } else {
        list_fs(&final_path)
    }
}

fn list_fs(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(path);

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
                println!("{}", entry.path().display());
                it.skip_current_dir();
                continue;
            }
        }
    }

    Ok(())
}

fn list_git(path: &str, rev: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let repo_path = PathBuf::from(path);
    let repo = Repository::open_bare(&repo_path).or_else(|_| Repository::open(&repo_path))?;

    let obj = if let Some(r) = rev {
        repo.revparse_single(r)?
    } else {
        let head = repo.head()?;
        head.peel_to_commit()?.into_object()
    };

    let tree = obj.peel_to_tree()?;

    // Recursive walk
    walk_git_tree(&repo, &tree, Path::new(""))?;

    Ok(())
}

fn walk_git_tree(
    repo: &Repository,
    tree: &git2::Tree,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if .table-dataset exists in this tree
    // We can use tree.get_name to check for existence of .table-dataset
    if tree.get_name(".table-dataset").is_some() {
        let display_path = if path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            path
        };
        println!("{}", display_path.display());
        return Ok(()); // Stop recursing into this dataset
    }

    for entry in tree.iter() {
        if entry.kind() == Some(ObjectType::Tree) {
            if let Some(name) = entry.name() {
                let object = entry.to_object(repo)?;
                if let Some(sub_tree) = object.as_tree() {
                    walk_git_tree(repo, sub_tree, &path.join(name))?;
                }
            }
        }
    }
    Ok(())
}
