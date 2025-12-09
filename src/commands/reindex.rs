use clap::Args;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Args, Debug, Clone)]
pub struct ReindexArgs {
    /// Path to the Kart repository
    pub path: String,

    /// Encoding for the directory structure (hex or base64)
    #[arg(long, default_value = "hex")]
    pub encoding: String,

    /// Number of directory levels
    #[arg(long, default_value_t = 2)]
    pub levels: u32,

    /// Number of branches per directory level
    #[arg(long, default_value_t = 256)]
    pub branches: u32,
    /// Perform a dry run without changing any files
    #[arg(long)]
    pub dry_run: bool,

    /// Output every file change
    #[arg(long, short)]
    pub verbose: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct PathStructure {
    scheme: String,
    branches: u32,
    levels: u32,
    encoding: String,
}

pub fn run(args: ReindexArgs) -> Result<(), Box<dyn std::error::Error>> {
    let repo_path = Path::new(&args.path);
    let meta_dir = repo_path.join(".table-dataset/meta");
    let feature_dir = repo_path.join(".table-dataset/feature");

    if !feature_dir.exists() {
        return Err(format!("Feature directory not found at {}", feature_dir.display()).into());
    }

    // Load Legend to find Primary Keys
    let legend_dir = meta_dir.join("legend");
    let mut primary_keys: Option<Vec<String>> = None;

    // Use the first legend found (assuming single table structure for now)
    let mut legend_map: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();

    if legend_dir.exists() {
        for entry in WalkDir::new(&legend_dir).min_depth(1).max_depth(1) {
            let entry = entry?;
            if entry.file_type().is_file() {
                let legend_id = entry.file_name().to_string_lossy().to_string();
                let file = File::open(entry.path())?;
                let legend_data: serde_json::Value = match serde_json::from_reader(file) {
                    Ok(v) => v,
                    Err(_) => {
                        // Fallback to MsgPack
                        let mut file = File::open(entry.path())?;
                        let mut buffer = Vec::new();
                        file.read_to_end(&mut buffer)?;
                        match rmp_serde::from_slice(&buffer) {
                            Ok(v) => v,
                            Err(_) => continue,
                        }
                    }
                };

                // Parse generic legend format [ [pks], [cols] ] or object
                let (pks, cols) = if let Some(arr) = legend_data.as_array() {
                    if arr.len() >= 2 {
                        let p = arr[0]
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let c = arr[1]
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default();
                        (p, c)
                    } else {
                        (vec![], vec![])
                    }
                } else if let Some(obj) = legend_data.as_object() {
                    let p = obj
                        .get("primary_key")
                        .and_then(|v| v.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();
                    let c = obj
                        .get("columns")
                        .and_then(|v| v.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();
                    (p, c)
                } else {
                    (vec![], vec![])
                };

                if primary_keys.is_none() {
                    primary_keys = Some(pks.clone());
                }
                legend_map.insert(legend_id, (pks, cols));
            }
        }
    }

    // Collect all feature files first to avoid issues while moving
    let mut feature_files = Vec::new();
    for entry in WalkDir::new(&feature_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            feature_files.push(entry.path().to_path_buf());
        }
    }

    // Validate branches vs encoding
    let base = match args.encoding.as_str() {
        "base64" => 64u32,
        _ => 16u32,
    };

    // Calculate logs
    let branches_f = args.branches as f64;
    let base_f = base as f64;

    // Approximate check for power of base
    // log_base(branches) should be integer
    let log_val = branches_f.log(base_f);
    let rounded = log_val.round();
    let is_power = (log_val - rounded).abs() < 1e-10;

    if !is_power {
        eprintln!(
            "Error: 'branches' ({}) must be a power of the encoding base ({}) for '{}' encoding.",
            args.branches, base, args.encoding
        );
        let next_power = base.pow(rounded as u32);
        eprintln!("Did you mean {}?", next_power);
        std::process::exit(1);
    }

    println!("Found {} features to reindex", feature_files.len());

    let mut moved_count = 0;
    let mut source_dirs = HashSet::new();
    let mut dest_dirs = HashMap::new();

    for (i, file_path) in feature_files.into_iter().enumerate() {
        // Read feature
        let mut file = File::open(&file_path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        // Decode MsgPack
        // Feature structure: [legend_id, [values]]
        let value: rmpv::Value = rmp_serde::from_slice(&buf)?;

        let (new_id, dir_hash, _legend_id) = if let Some(arr) = value.as_array() {
            if arr.len() >= 2 {
                let legend_id = arr[0].as_str().unwrap_or_default();
                let row_values = arr[1].as_array().ok_or("Feature values not an array")?;

                let (pks, cols) = legend_map
                    .get(legend_id)
                    .ok_or_else(|| format!("Legend {} not found", legend_id))?;

                // Extract PK values
                let mut pk_values = Vec::new();
                for pk in pks {
                    if let Some(pos) = cols.iter().position(|c| c == pk) {
                        if let Some(val) = row_values.get(pos) {
                            pk_values.push(val.clone());
                        }
                    }
                }

                // Fallback: If PK extraction failed (e.g. PK not in cols), try to decode filename
                if pk_values.is_empty() {
                    // Try decoding filename as Base64 MsgPack
                    let filename = file_path.file_name().and_then(|n| n.to_str()).unwrap_or("");

                    // Try decoding as URL-safe Base64
                    use base64::{Engine as _, engine::general_purpose};
                    if let Ok(bytes) = general_purpose::URL_SAFE_NO_PAD.decode(filename) {
                        if let Ok(val) = rmp_serde::from_slice::<rmpv::Value>(&bytes) {
                            if let Some(arr) = val.as_array() {
                                pk_values = arr.clone();
                            }
                        }
                    }

                    // If still empty, try hex decoding (legacy support / different encoding)
                    if pk_values.is_empty() {
                        if let Ok(bytes) = hex::decode(filename) {
                            if let Ok(val) = rmp_serde::from_slice::<rmpv::Value>(&bytes) {
                                if let Some(arr) = val.as_array() {
                                    pk_values = arr.clone();
                                }
                            }
                        }
                    }
                }

                // If still empty, skip (cannot determine PK)
                if pk_values.is_empty() {
                    eprintln!(
                        "Skipping {}: Cannot extract PK from body or filename",
                        file_path.display()
                    );
                    continue;
                }

                // Encode PK values to MsgPack bytes (always as array)
                let mut pk_buf = Vec::new();
                rmp_serde::encode::write(&mut pk_buf, &pk_values)?;

                // Hash (for directory structure)
                let mut hasher = Sha256::new();
                hasher.update(&pk_buf);
                let result = hasher.finalize();

                // Directory hash depends on encoding
                let dir_hash = if args.encoding == "base64" {
                    use base64::{Engine as _, engine::general_purpose};
                    general_purpose::URL_SAFE_NO_PAD.encode(result)
                } else {
                    hex::encode(result)
                };

                // Filename is Base64 encoded PK (always)
                use base64::{Engine as _, engine::general_purpose};
                let new_id = general_purpose::URL_SAFE_NO_PAD.encode(&pk_buf);

                (new_id, dir_hash, legend_id.to_string())
            } else {
                continue;
            }
        } else {
            continue;
        };

        // Determine new path
        // Use dir_hash for directory structure
        let rel_path = compute_path(&dir_hash, &args.encoding, args.levels, args.branches);
        let target_dir = feature_dir.join(rel_path);
        let target_path = target_dir.join(&new_id);

        if let Some(parent) = file_path.parent() {
            source_dirs.insert(parent.to_path_buf());
        }
        *dest_dirs.entry(target_dir.clone()).or_insert(0) += 1;

        // Progress update every 10,000 items
        if !args.verbose && (i + 1) % 10_000 == 0 {
            println!("Processed {} features ({} moved)...", i + 1, moved_count);
        }

        if target_path != file_path {
            if args.verbose {
                if args.dry_run {
                    println!(
                        "Would rename {} to {}",
                        file_path
                            .strip_prefix(repo_path)
                            .unwrap_or(&file_path)
                            .display(),
                        target_path
                            .strip_prefix(repo_path)
                            .unwrap_or(&target_path)
                            .display()
                    );
                } else {
                    println!(
                        "Renaming {} to {}",
                        file_path
                            .strip_prefix(repo_path)
                            .unwrap_or(&file_path)
                            .display(),
                        target_path
                            .strip_prefix(repo_path)
                            .unwrap_or(&target_path)
                            .display()
                    );
                }
            }

            if !args.dry_run {
                fs::create_dir_all(&target_dir)?;
                fs::rename(&file_path, &target_path)?;
            }
            moved_count += 1;
        }
    }

    // Clean up empty directories
    if !args.dry_run {
        for entry in WalkDir::new(&feature_dir)
            .contents_first(true)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_dir() {
                // Try to remove, if not empty it will fail silently (which is what we want)
                let _ = fs::remove_dir(entry.path());
            }
        }
    } else {
        println!(
            "Would clean up empty directories in {}",
            feature_dir.display()
        );
    }

    // Update path-structure.json
    let structure = PathStructure {
        scheme: "msgpack/hash".to_string(),
        branches: args.branches,
        levels: args.levels,
        encoding: args.encoding.clone(),
    };

    let structure_path = meta_dir.join("path-structure.json");
    if args.dry_run {
        println!("Would update path-structure.json to: {:?}", structure);
    } else {
        let f = File::create(structure_path)?;
        serde_json::to_writer_pretty(f, &structure)?;
    }

    if args.dry_run {
        println!("Dry run complete. Found {} features to move.", moved_count);
    } else {
        println!(
            "Reindexed {} features. Updated path-structure.json",
            moved_count
        );
    }

    println!("Old Folders: {}", source_dirs.len());

    let dest_counts: Vec<usize> = dest_dirs.values().cloned().collect();
    let unique_dest_folders = dest_counts.len();
    let min_files = dest_counts.iter().min().unwrap_or(&0);
    let max_files = dest_counts.iter().max().unwrap_or(&0);
    let total_files: usize = dest_counts.iter().sum();
    let avg_files = if unique_dest_folders > 0 {
        total_files as f64 / unique_dest_folders as f64
    } else {
        0.0
    };

    println!("New Folders: {}", unique_dest_folders);
    println!(
        "Target Folder Stats: Min {}, Max {}, Avg {:.2}",
        min_files, max_files, avg_files
    );

    Ok(())
}

fn compute_path(id: &str, encoding: &str, levels: u32, branches: u32) -> PathBuf {
    let mut path = PathBuf::new();

    if levels == 0 {
        return path;
    }

    // Chunk size logic from kart_paths.py
    // group_length = int(math.log(max(branches, 1)) / math.log(max(base, 1)))
    let base: f64 = match encoding {
        "base64" => 64.0,
        _ => 16.0,
    };

    let branches_f = branches as f64;
    let group_length = (branches_f.max(1.0).ln() / base.max(1.0).ln()).floor() as usize;
    let group_length = group_length.max(1); // Ensure at least 1 char per level if calc yields 0

    let mut start = 0;
    for _ in 0..levels {
        if start + group_length > id.len() {
            break;
        }
        let segment = &id[start..start + group_length];
        path.push(segment);
        start += group_length;
    }

    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_reindex_hex() {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();
        let meta_dir = repo_path.join(".table-dataset/meta");
        let feature_dir = repo_path.join(".table-dataset/feature");

        fs::create_dir_all(&meta_dir.join("legend")).unwrap();
        fs::create_dir_all(&feature_dir).unwrap();

        // Create Legend
        let legend_id = "test_legend";
        let legend_content = serde_json::json!([["id"], ["id", "name"]]);
        let f = File::create(meta_dir.join("legend").join(legend_id)).unwrap();
        serde_json::to_writer(f, &legend_content).unwrap();

        // Create Feature
        // PK = 1
        // MsgPack([1]) -> [145, 1] -> SHA256 -> hex
        // Let's use a simpler PK "a" -> msgpack(["a"]) -> sha256 -> ...
        // Using "id"=1.
        let pk_val = rmpv::Value::Integer(1.into());
        let row = vec![pk_val, rmpv::Value::String("test".into())];
        let feature_content = rmpv::Value::Array(vec![
            rmpv::Value::String(legend_id.into()),
            rmpv::Value::Array(row),
        ]);

        let old_id = "old_uuid";
        let mut f = File::create(feature_dir.join(old_id)).unwrap();
        let mut buf = Vec::new();
        rmp_serde::encode::write(&mut buf, &feature_content).unwrap();
        f.write_all(&buf).unwrap();

        // Calculate expected hash
        // PK value is integer 1.
        // We encode `1` directly if single PK? No, code says:
        /*
        if pk_values.len() == 1 {
            rmp_serde::encode::write(&mut pk_buf, &pk_values[0])?;
        }
        */
        // So we encode integer 1 inside a vector (standard Kart behavior).
        let mut pk_buf = Vec::new();
        rmp_serde::encode::write(&mut pk_buf, &vec![1]).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(&pk_buf);
        let _ = hex::encode(hasher.finalize());
        // e.g. sha256(msgpack(1))

        let args = ReindexArgs {
            path: repo_path.to_string_lossy().to_string(),
            encoding: "hex".to_string(),
            levels: 2,
            branches: 256,
            dry_run: false,
            verbose: false,
        };

        run(args).unwrap();

        // precise hash for msgpack(1) (0x01) ?? No, msgpack(1) is 0x01 on wire?
        // rmp_serde::encode::write(&mut buf, &1) -> buf is [1].
        // sha256([0x91, 0x01]): cdca8b98b1dc607bbdc15ba076869dc8b8d9f26d207114ce1983bf74717f6401
        let expected_hash = "cdca8b98b1dc607bbdc15ba076869dc8b8d9f26d207114ce1983bf74717f6401";

        // Expected Filename: Base64UrlSafe([0x91, 0x01])
        // [145, 1] -> kQE
        // base64::URL_SAFE_NO_PAD([145, 1])
        let expected_filename = "kQE";

        let expected_path = feature_dir
            .join(&expected_hash[0..2])
            .join(&expected_hash[2..4])
            .join(expected_filename);

        assert!(
            expected_path.exists(),
            "Feature should be at {}",
            expected_path.display()
        );
        // Original file should be gone
        assert!(!feature_dir.join(old_id).exists());

        // Check structure file
        let struct_file = meta_dir.join("path-structure.json");
        assert!(struct_file.exists());
        let s: PathStructure = serde_json::from_reader(File::open(struct_file).unwrap()).unwrap();
        assert_eq!(s.scheme, "msgpack/hash");
        assert_eq!(s.encoding, "hex");
    }
}
