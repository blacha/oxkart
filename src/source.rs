use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::PathBuf;
use walkdir::WalkDir;

#[derive(Debug, Deserialize)]
pub struct SchemaField {
    pub id: String,
    pub name: String,
    #[serde(rename = "dataType")]
    pub data_type: String,
    #[serde(rename = "geometryCRS")]
    pub geometry_crs: Option<String>,
    // other fields ignored
}

pub type KartSchema = Vec<SchemaField>;
pub type Legend = (Vec<String>, Vec<String>);

use git2::{ObjectType, Oid, Repository};

#[derive(Debug, Clone)]
pub enum FeatureIdentifier {
    Oid(Oid),
    Path(PathBuf),
}

impl std::fmt::Display for FeatureIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeatureIdentifier::Oid(oid) => write!(f, "{}", oid),
            FeatureIdentifier::Path(path) => write!(f, "{}", path.display()),
        }
    }
}

/// Enum wrapping different KartSource implementations
pub enum KartSourceEnum {
    Fs(FsSource),
    Git(FsGit),
}

impl KartSourceEnum {
    pub fn get_schema(&self) -> Result<KartSchema, Box<dyn std::error::Error>> {
        match self {
            KartSourceEnum::Fs(s) => s.get_schema(),
            KartSourceEnum::Git(s) => s.get_schema(),
        }
    }

    pub fn get_legends(&self) -> Result<HashMap<String, Legend>, Box<dyn std::error::Error>> {
        match self {
            KartSourceEnum::Fs(s) => s.get_legends(),
            KartSourceEnum::Git(s) => s.get_legends(),
        }
    }

    pub fn get_crs_wkt(&self, crs_id: &str) -> Result<String, Box<dyn std::error::Error>> {
        match self {
            KartSourceEnum::Fs(s) => s.get_crs_wkt(crs_id),
            KartSourceEnum::Git(s) => s.get_crs_wkt(crs_id),
        }
    }

    pub fn feature_identifiers(&self) -> Box<dyn Iterator<Item = FeatureIdentifier> + Send> {
        match self {
            KartSourceEnum::Fs(s) => s.feature_identifiers(),
            KartSourceEnum::Git(s) => s.feature_identifiers(),
        }
    }

    pub fn read_feature<F, R>(
        &self,
        id: &FeatureIdentifier,
        f: F,
    ) -> Result<R, Box<dyn std::error::Error>>
    where
        F: FnOnce(&[u8]) -> R,
    {
        match self {
            KartSourceEnum::Fs(s) => s.read_feature(id, f),
            KartSourceEnum::Git(s) => s.read_feature(id, f),
        }
    }
}

/// Implementation of KartSource for the local filesystem
pub struct FsSource {
    base_path: PathBuf,
}

impl FsSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: path.into(),
        }
    }

    fn get_schema(&self) -> Result<KartSchema, Box<dyn std::error::Error>> {
        let schema_path = self.base_path.join(".table-dataset/meta/schema.json");
        let file = File::open(schema_path)?;
        let reader = BufReader::new(file);
        let kart_schema: KartSchema = serde_json::from_reader(reader)?;
        Ok(kart_schema)
    }

    fn get_legends(&self) -> Result<HashMap<String, Legend>, Box<dyn std::error::Error>> {
        let legend_dir = self.base_path.join(".table-dataset/meta/legend");
        let mut legend_cache = HashMap::new();

        if legend_dir.exists() {
            for entry in WalkDir::new(legend_dir).min_depth(1).max_depth(1) {
                let entry = entry?;
                if entry.file_type().is_file() {
                    let legend_id = entry
                        .path()
                        .file_stem()
                        .unwrap()
                        .to_string_lossy()
                        .to_string();
                    let file = File::open(entry.path())?;
                    let reader = BufReader::new(file);

                    // Try parsing as JSON first
                    let legend_data: serde_json::Value = match serde_json::from_reader(reader) {
                        Ok(v) => v,
                        Err(_) => {
                            // If JSON fails, try Msgpack (re-open file or read to buffer first)
                            // Since we consumed reader, let's read to buffer first for robust handling
                            let mut file = File::open(entry.path())?;
                            let mut buffer = Vec::new();
                            file.read_to_end(&mut buffer)?;
                            match rmp_serde::from_slice(&buffer) {
                                Ok(v) => v,
                                Err(_) => continue,
                            }
                        }
                    };

                    if let Some(obj) = legend_data.as_object() {
                        let pks: Vec<String> = obj
                            .get("primary_key")
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default();

                        let cols: Vec<String> = obj
                            .get("columns")
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default();

                        legend_cache.insert(legend_id, (pks, cols));
                    } else if let Some(l_arr) = legend_data.as_array() {
                        if l_arr.len() >= 2 {
                            let pks: Vec<String> = if let Some(pk_arr) = l_arr[0].as_array() {
                                pk_arr
                                    .iter()
                                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                    .collect()
                            } else {
                                vec![]
                            };

                            let cols: Vec<String> = if let Some(col_arr) = l_arr[1].as_array() {
                                col_arr
                                    .iter()
                                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                    .collect()
                            } else {
                                vec![]
                            };

                            legend_cache.insert(legend_id, (pks, cols));
                        }
                    }
                }
            }
        }
        Ok(legend_cache)
    }

    fn get_crs_wkt(&self, crs_id: &str) -> Result<String, Box<dyn std::error::Error>> {
        // crs_id might be "EPSG:4167"
        let crs_filename = format!("{crs_id}.wkt");
        let crs_path = self
            .base_path
            .join(".table-dataset/meta/crs")
            .join(&crs_filename);

        if crs_path.exists() {
            let mut crs_file = File::open(&crs_path)?;
            let mut wkt_string = String::new();
            crs_file.read_to_string(&mut wkt_string)?;
            Ok(wkt_string.trim().to_string())
        } else {
            Err(format!("CRS file not found: {}", crs_path.display()).into())
        }
    }

    fn feature_identifiers(&self) -> Box<dyn Iterator<Item = FeatureIdentifier> + Send> {
        let feature_dir = self.base_path.join(".table-dataset/feature");
        if !feature_dir.exists() {
            return Box::new(std::iter::empty());
        }

        let walker = WalkDir::new(feature_dir).into_iter();

        Box::new(
            walker
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
                .map(|e| FeatureIdentifier::Path(e.into_path())),
        )
    }

    fn read_feature<F, R>(
        &self,
        id: &FeatureIdentifier,
        f: F,
    ) -> Result<R, Box<dyn std::error::Error>>
    where
        F: FnOnce(&[u8]) -> R,
    {
        match id {
            FeatureIdentifier::Path(path) => {
                let mut file = File::open(path)?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                Ok(f(&buffer))
            }
            _ => Err("Invalid identifier for FsSource".into()),
        }
    }
}

// Imports removed

/// Implementation of KartSource for a Git repository
pub struct FsGit {
    repo_path: PathBuf,
    tree_id: Oid,
    prefix: Option<PathBuf>,
}

impl FsGit {
    pub fn new(
        path: impl Into<PathBuf>,
        revision: Option<&str>,
        prefix: Option<&str>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let repo_path = path.into();
        let repo = Repository::open_bare(&repo_path).or_else(|_| Repository::open(&repo_path))?;

        let obj = if let Some(rev) = revision {
            repo.revparse_single(rev)?
        } else {
            let head = repo.head()?;
            head.peel_to_commit()?.into_object()
        };

        let tree_id = obj.peel_to_tree()?.id();
        let prefix = prefix.map(PathBuf::from);

        Ok(Self {
            repo_path,
            tree_id,
            prefix,
        })
    }

    fn resolve_path(&self, path: &str) -> PathBuf {
        if let Some(ref p) = self.prefix {
            p.join(path)
        } else {
            PathBuf::from(path)
        }
    }

    // Helper to get blob content, used by get_schema, get_crs_wkt, and get_legends
    fn get_blob_content_by_path(&self, path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        thread_local! {
            static REPO_CACHE: std::cell::RefCell<HashMap<PathBuf, Repository>> = std::cell::RefCell::new(HashMap::new());
        }

        REPO_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            let repo = cache.entry(self.repo_path.clone()).or_insert_with(|| {
                Repository::open_bare(&self.repo_path)
                    .or_else(|_| Repository::open(&self.repo_path))
                    .expect("Failed to open repo")
            });

            let tree = repo.find_tree(self.tree_id)?;
            let full_path = self.resolve_path(path);
            let entry = tree.get_path(&full_path)?;
            let obj = entry.to_object(repo)?;

            if let Some(blob) = obj.as_blob() {
                Ok(blob.content().to_vec())
            } else {
                Err(format!("Path is not a blob: {}", path).into())
            }
        })
    }

    pub fn get_schema(&self) -> Result<KartSchema, Box<dyn std::error::Error>> {
        let buffer = self.get_blob_content_by_path(".table-dataset/meta/schema.json")?;
        let kart_schema: KartSchema = serde_json::from_slice(&buffer)?;
        Ok(kart_schema)
    }

    fn get_legends(&self) -> Result<HashMap<String, Legend>, Box<dyn std::error::Error>> {
        let mut legend_cache = HashMap::new();

        // We need to list files in .table-dataset/meta/legend
        // This is harder with just get_blob_content. We need to traverse the tree.
        // For now, let's assume we can list the directory.
        // Since listing is complex without a full tree traversal helper, and we want to be fast,
        // let's implement a specific tree traversal for this.

        thread_local! {
            static REPO_CACHE: std::cell::RefCell<HashMap<PathBuf, Repository>> = std::cell::RefCell::new(HashMap::new());
        }

        REPO_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            let repo = cache.entry(self.repo_path.clone()).or_insert_with(|| {
                Repository::open_bare(&self.repo_path)
                    .or_else(|_| Repository::open(&self.repo_path))
                    .expect("Failed to open repo")
            });

            let tree = repo.find_tree(self.tree_id)?;
            let legend_dir_path = self.resolve_path(".table-dataset/meta/legend");

            // Try to find the legend directory
            if let Ok(entry) = tree.get_path(&legend_dir_path) {
                if let Ok(legend_tree) = entry.to_object(repo)?.peel_to_tree() {
                    for entry in legend_tree.iter() {
                        if let Some(name) = entry.name() {
                            // Accept any file in legend directory
                            let legend_id = std::path::Path::new(name)
                                .file_stem()
                                .unwrap()
                                .to_string_lossy()
                                .to_string();
                            if let Ok(obj) = entry.to_object(repo) {
                                if let Some(blob) = obj.as_blob() {
                                    let l_val: serde_json::Value =
                                        match serde_json::from_slice(blob.content()) {
                                            Ok(v) => v,
                                            Err(_) => {
                                                // Fallback to rmp_serde if json fails, though extension is json
                                                match rmp_serde::from_slice(blob.content()) {
                                                    Ok(v) => v,
                                                    Err(_) => continue,
                                                }
                                            }
                                        };

                                    if let Some(obj) = l_val.as_object() {
                                        let pks = obj
                                            .get("primary_key")
                                            .and_then(|v| v.as_array())
                                            .map(|arr| {
                                                arr.iter()
                                                    .filter_map(|v| v.as_str().map(String::from))
                                                    .collect()
                                            })
                                            .unwrap_or_default();

                                        let cols = obj
                                            .get("columns")
                                            .and_then(|v| v.as_array())
                                            .map(|arr| {
                                                arr.iter()
                                                    .filter_map(|v| v.as_str().map(String::from))
                                                    .collect()
                                            })
                                            .unwrap_or_default();

                                        legend_cache.insert(legend_id, (pks, cols));
                                    } else if let Some(l_arr) = l_val.as_array() {
                                        if l_arr.len() >= 2 {
                                            let pks = if let Some(pk_arr) = l_arr[0].as_array() {
                                                pk_arr
                                                    .iter()
                                                    .filter_map(|v| {
                                                        v.as_str().map(|s| s.to_string())
                                                    })
                                                    .collect()
                                            } else {
                                                vec![]
                                            };

                                            let cols = if let Some(col_arr) = l_arr[1].as_array() {
                                                col_arr
                                                    .iter()
                                                    .filter_map(|v| {
                                                        v.as_str().map(|s| s.to_string())
                                                    })
                                                    .collect()
                                            } else {
                                                vec![]
                                            };

                                            legend_cache.insert(legend_id, (pks, cols));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(legend_cache)
        })
    }

    fn get_crs_wkt(&self, crs_id: &str) -> Result<String, Box<dyn std::error::Error>> {
        let path = format!(".table-dataset/meta/crs/{}.wkt", crs_id);
        let buffer = self.get_blob_content_by_path(&path)?;
        let wkt_string = String::from_utf8(buffer)?;
        Ok(wkt_string.trim().to_string())
    }

    fn feature_identifiers(&self) -> Box<dyn Iterator<Item = FeatureIdentifier> + Send> {
        // We need to traverse .table-dataset/feature
        // This is blocking, but it's done once at start.
        // To make it Send, we collect all OIDs into a Vec.

        let mut oids = Vec::new();

        if let Ok(repo) =
            Repository::open_bare(&self.repo_path).or_else(|_| Repository::open(&self.repo_path))
        {
            if let Ok(tree) = repo.find_tree(self.tree_id) {
                let feature_dir_path = self.resolve_path(".table-dataset/feature");
                if let Ok(entry) = tree.get_path(&feature_dir_path) {
                    if let Ok(feature_tree) = entry.to_object(&repo).and_then(|o| o.peel_to_tree())
                    {
                        feature_tree
                            .walk(git2::TreeWalkMode::PreOrder, |_, entry| {
                                if let Some(_name) = entry.name() {
                                    // Check if it looks like a feature file (usually just check if it's a blob)
                                    if entry.kind() == Some(ObjectType::Blob) {
                                        oids.push(FeatureIdentifier::Oid(entry.id()));
                                    }
                                }
                                git2::TreeWalkResult::Ok
                            })
                            .ok();
                    }
                }
            }
        }

        Box::new(oids.into_iter())
    }

    fn read_feature<F, R>(
        &self,
        id: &FeatureIdentifier,
        f: F,
    ) -> Result<R, Box<dyn std::error::Error>>
    where
        F: FnOnce(&[u8]) -> R,
    {
        match id {
            FeatureIdentifier::Oid(oid) => {
                thread_local! {
                    static REPO_CACHE: std::cell::RefCell<HashMap<PathBuf, Repository>> = std::cell::RefCell::new(HashMap::new());
                }

                REPO_CACHE.with(|cache| {
                    let mut cache = cache.borrow_mut();
                    let repo = cache.entry(self.repo_path.clone()).or_insert_with(|| {
                        Repository::open_bare(&self.repo_path)
                            .or_else(|_| Repository::open(&self.repo_path))
                            .expect("Failed to open repo")
                    });

                    // Direct OID lookup - FAST!
                    // Optimization: Use odb().read() to skip Blob wrapper creation
                    if let Ok(odb) = repo.odb() {
                        if let Ok(obj) = odb.read(*oid) {
                            return Ok(f(obj.data()));
                        }
                    }
                    // Fallback to find_blob if odb fails for some reason
                    if let Ok(blob) = repo.find_blob(*oid) {
                        return Ok(f(blob.content()));
                    }
                    Err(format!("Blob not found: {}", oid).into())
                })
            }
            _ => Err("Invalid identifier for FsGit".into()),
        }
    }
}
