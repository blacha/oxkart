use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use clap::Args;
use num_format::{Locale, ToFormattedString};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use rmpv::Value;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::source::{FsGit, FsSource, KartSourceEnum};

#[derive(Args, Debug, Clone)]
pub struct ExportArgs {
    /// Path to the dataset directory (e.g., sample/nz_topo_map_sheet)
    pub path: String,
    /// Output Parquet file path
    pub output: Option<String>,
    /// Compression (uncompressed, snappy, gzip, lzo, brotli, lz4, zstd)
    #[arg(long, default_value = "zstd")]
    pub compression: String,
    /// Compression level (for ZSTD, etc.)
    #[arg(long)]
    pub compression_level: Option<i32>,
    /// Max row group size (number of rows)
    #[arg(long, default_value_t = 8192)]
    pub row_group_size: usize,
    /// Git revision (branch, tag, sha) to read from (default: HEAD)
    #[arg(long)]
    pub git_rev: Option<String>,
    /// Path inside the Git repository to the dataset root
    #[arg(long)]
    pub git_prefix: Option<String>,
    /// Export all datasets found in the path
    #[arg(long)]
    pub all: bool,
}

fn extract_wkb(data: &[u8]) -> &[u8] {
    // Kart/GeoPackage binary format:
    // Header (8 bytes or more). Standard GPKG header is at least 8 bytes.
    // Magic: 0x47 0x50 (GP)
    // Version: 1 byte
    // Flags: 1 byte
    // SRS ID: 4 bytes
    // Envelope: variable length (determined by flags)
    // Then WKB.

    if data.len() < 8 || data[0] != 0x47 || data[1] != 0x50 {
        // Not a standard GPKG header we recognize, return as is or empty?
        // For now, assume it might be raw WKB if header missing (unlikely in Kart)
        return data;
    }

    let flags = data[3];
    let envelope_len = match (flags >> 1) & 0x07 {
        0 => 0,
        1 => 32,
        2 | 3 => 48,
        4 => 64,
        _ => 0, // Invalid
    };

    let offset = 8 + envelope_len;
    if data.len() > offset {
        &data[offset..]
    } else {
        data // Fallback
    }
}

pub fn run(args: ExportArgs) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Starting export...");

    let (is_git, final_path) = crate::commands::resolve_source_path(&args.path);

    if args.all {
        let datasets = crate::commands::find_datasets(&final_path, is_git, args.git_rev.as_deref())
            .unwrap_or_default();

        if datasets.is_empty() {
            return Err(Box::from("No datasets found to export."));
        }

        eprintln!("Found {} datasets to export", datasets.len());

        let output_base = if let Some(ref out) = args.output {
            let path = Path::new(out);
            if !path.exists() {
                std::fs::create_dir_all(path)?;
            }
            if !path.is_dir() {
                return Err(Box::from(
                    "Output path must be a directory when using --all",
                ));
            }
            Some(path.to_path_buf())
        } else {
            None
        };

        // Capture args and output_base for parallel iteration
        let args_arc = Arc::new(args.clone());
        let output_base_arc = Arc::new(output_base);

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        let failures = AtomicUsize::new(0);

        pool.install(|| {
            datasets.into_par_iter().for_each(|(name, source)| {
                let output_path = if let Some(ref base) = *output_base_arc {
                    base.join(format!("{}.parquet", name))
                } else {
                    let normalized = name.replace('-', "_");
                    Path::new(&format!("{}.parquet", normalized)).to_path_buf()
                };

                if let Err(e) = export_dataset(source, &output_path, &args_arc, &name) {
                    eprintln!("[{}] Export failed: {}", name, e);
                    failures.fetch_add(1, Ordering::Relaxed);
                }
            })
        });

        let fail_count = failures.load(Ordering::Relaxed);
        if fail_count > 0 {
            return Err(Box::from(format!(
                "{} datasets failed to export",
                fail_count
            )));
        }

        return Ok(());
    }

    let mut source = if is_git {
        eprintln!("Using Git source");
        if let Some(ref rev) = args.git_rev {
            eprintln!("Revision: {}", rev);
        }
        if let Some(ref prefix) = args.git_prefix {
            eprintln!("Prefix: {}", prefix);
        }
        KartSourceEnum::Git(
            FsGit::new(
                &final_path,
                args.git_rev.as_deref(),
                args.git_prefix.as_deref(),
            )
            .map_err(|e| e as Box<dyn std::error::Error>)?,
        )
    } else {
        eprintln!("Using Filesystem source");
        KartSourceEnum::Fs(FsSource::new(&final_path))
    };

    match source.get_schema() {
        Ok(_) => {} // successfully loaded schema, source is valid
        Err(e) => {
            let e: Box<dyn std::error::Error> = e; // Downcast for compatibility
            // Try to find datasets in the path if we are at the root
            let mut datasets =
                crate::commands::find_datasets(&final_path, is_git, args.git_rev.as_deref())
                    .unwrap_or_default();

            if datasets.len() > 1 {
                if let Some(ref output) = args.output {
                    let output_path = Path::new(output);
                    if let Some(file_stem) = output_path.file_stem() {
                        let stem_str = file_stem.to_string_lossy().to_string();
                        // Filter datasets that end with the stem (or some reasonable matching)
                        let matched_indices: Vec<usize> = datasets
                            .iter()
                            .enumerate()
                            .filter(|(_, (name, _))| {
                                // Simple logic: if dataset name exactly matches stem, or ends with stem
                                let normalized_name = name.replace('-', "_");
                                let normalized_stem = stem_str.replace('-', "_");
                                normalized_name.ends_with(&normalized_stem)
                            })
                            .map(|(i, _)| i)
                            .collect();

                        if matched_indices.len() == 1 {
                            let idx = matched_indices[0];
                            let val = datasets.remove(idx);
                            datasets = vec![val];
                        }
                    }
                }
            }

            if datasets.len() == 1 {
                let (name, new_source) = datasets.pop().unwrap();
                eprintln!("Automatically selected dataset: {}", name);
                source = new_source;
                source
                    .get_schema()
                    .map_err(|e| e as Box<dyn std::error::Error>)?;
            } else if datasets.len() > 1 {
                eprintln!("Multiple datasets found:");
                for (name, _) in datasets {
                    eprintln!("  - {}", name);
                }
                return Err(Box::from(
                    "Multiple datasets found. Please specify which one to export.",
                ));
            } else if is_git && args.git_prefix.is_none() {
                // Try to infer prefix from output filename
                let output_str = args
                    .output
                    .as_deref()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        let path = Path::new(&args.path);
                        let file_stem = path.file_name().unwrap_or_default().to_string_lossy();
                        format!("{}.parquet", file_stem.replace('-', "_"))
                    });
                let output_path = Path::new(&output_str);
                if let Some(file_stem) = output_path.file_stem() {
                    let inferred_prefix = file_stem.to_string_lossy().to_string();
                    eprintln!(
                        "Failed to load schema from root. Trying inferred prefix: '{}'",
                        inferred_prefix
                    );

                    // Re-initialize source with inferred prefix
                    let new_source =
                        FsGit::new(&final_path, args.git_rev.as_deref(), Some(&inferred_prefix))
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                    match new_source.get_schema() {
                        Ok(_) => {
                            source = KartSourceEnum::Git(new_source); // Update source to use the new one
                        }
                        Err(_) => return Err(e), // Return original error if inference fails
                    }
                } else {
                    return Err(e);
                }
            } else {
                return Err(e);
            }
        }
    };

    let output_str = args.output.clone().unwrap_or_else(|| {
        let path = Path::new(&args.path);
        let file_stem = path.file_name().unwrap_or_default().to_string_lossy();
        format!("{}.parquet", file_stem.replace('-', "_"))
    });
    let output_path = Path::new(&output_str);
    let table_name = output_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    export_dataset(source, output_path, &args, &table_name)
        .map_err(|e| e as Box<dyn std::error::Error>)
}

fn export_dataset(
    source: KartSourceEnum,
    output_path: &Path,
    args: &ExportArgs,
    table_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let kart_schema = source.get_schema()?;

    let mut arrow_fields = Vec::new();
    let mut field_indices: HashMap<String, usize> = HashMap::new();

    for (i, field) in kart_schema.iter().enumerate() {
        let arrow_type = match field.data_type.as_str() {
            "boolean" => DataType::Boolean,
            "integer" => DataType::Int64,
            "float" => DataType::Float64,
            "blob" => DataType::Binary,
            "geometry" => DataType::Binary,
            // All others map to Utf8 (text, date, time, timestamp, interval, numeric)
            _ => DataType::Utf8,
        };

        eprintln!(
            "[{}] Field '{}': '{}' -> '{:?}'",
            table_name, field.name, field.data_type, arrow_type
        );

        arrow_fields.push(Field::new(&field.name, arrow_type, true));
        field_indices.insert(field.id.clone(), i);
    }

    let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

    eprintln!("[{}] Loading legends...", table_name);
    let legend_cache = source.get_legends()?;
    eprintln!("[{}] Loaded {} legends", table_name, legend_cache.len());

    let paths_iter = source.feature_identifiers();

    let file = File::create(output_path)?;

    let mut columns = serde_json::Map::new();
    let mut geom_meta = serde_json::Map::new();
    geom_meta.insert(
        "encoding".to_string(),
        serde_json::Value::String("WKB".to_string()),
    );
    geom_meta.insert(
        "geometry_types".to_string(),
        serde_json::Value::Array(vec![]),
    );

    // Find geometry column and its CRS
    let mut geom_col_name = "geom".to_string(); // Default fallback

    if let Some(geom_field) = kart_schema.iter().find(|f| f.data_type == "geometry") {
        geom_col_name = geom_field.name.clone();
        if let Some(ref crs_id) = geom_field.geometry_crs {
            match source.get_crs_wkt(crs_id) {
                Ok(wkt) => {
                    // Clean WKT: remove newlines and extra whitespace
                    let clean_wkt = wkt
                        .replace('\n', "")
                        .replace('\r', "")
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ");

                    geom_meta.insert(
                        "crs".to_string(),
                        serde_json::json!({
                            "wkt": clean_wkt
                        }),
                    );
                    eprintln!("[{}] Loaded CRS for {crs_id}", table_name);
                }
                Err(e) => {
                    eprintln!("[{}] Failed to load CRS for {crs_id}: {e}", table_name);
                }
            }
        }
    }

    columns.insert(geom_col_name.clone(), serde_json::Value::Object(geom_meta));

    let mut geo_meta = serde_json::Map::new();
    geo_meta.insert(
        "version".to_string(),
        serde_json::Value::String("1.0.0".to_string()),
    );
    geo_meta.insert(
        "primary_column".to_string(),
        serde_json::Value::String(geom_col_name),
    );
    geo_meta.insert("columns".to_string(), serde_json::Value::Object(columns));

    let geo_json_str = serde_json::to_string(&geo_meta)?;

    let compression = match args.compression.to_lowercase().as_str() {
        "uncompressed" => parquet::basic::Compression::UNCOMPRESSED,
        "snappy" => parquet::basic::Compression::SNAPPY,
        "gzip" => parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default()),
        "lzo" => parquet::basic::Compression::LZO,
        "brotli" => parquet::basic::Compression::BROTLI(parquet::basic::BrotliLevel::default()),
        "lz4" => parquet::basic::Compression::LZ4,
        "zstd" => {
            let level = args.compression_level.unwrap_or(9);
            parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(level).unwrap_or_default(),
            )
        }
        _ => {
            eprintln!(
                "[{}] Unknown compression '{}', defaulting to ZSTD",
                table_name, args.compression
            );
            let level = args.compression_level.unwrap_or(9);
            parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(level).unwrap_or_default(),
            )
        }
    };

    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(args.row_group_size)
        .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue {
            key: "geo".to_string(),
            value: Some(geo_json_str),
        }]))
        .build();

    // Enum to avoid dynamic dispatch and downcasting
    enum KartBuilder {
        Boolean(BooleanBuilder),
        Int64(Int64Builder),
        Float64(Float64Builder),
        Binary(BinaryBuilder),
        String(StringBuilder),
    }

    let mut legend_schema_mapping: HashMap<String, Vec<Option<usize>>> = HashMap::new();

    for (legend_id, (_pks, cols)) in &legend_cache {
        let mut mapping = Vec::with_capacity(kart_schema.len());
        for field in &kart_schema {
            let msgpack_idx = cols.iter().position(|c| c == &field.id);
            mapping.push(msgpack_idx);
        }
        legend_schema_mapping.insert(legend_id.clone(), mapping);
    }

    let (tx, rx) = std::sync::mpsc::channel::<RecordBatch>();

    let arrow_schema_clone = arrow_schema.clone();
    let table_name_clone = table_name.to_string();

    let writer_handle = std::thread::spawn(
        move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let mut writer = ArrowWriter::try_new(file, arrow_schema_clone, Some(props))
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let mut total_features = 0;
            let start_time = std::time::Instant::now();

            while let Ok(batch) = rx.recv() {
                let batch_size = batch.num_rows();
                writer
                    .write(&batch)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

                let prev_count = total_features;
                total_features += batch_size;

                if total_features / 100_000 > prev_count / 100_000 {
                    let elapsed = start_time.elapsed();
                    let fps = total_features as f64 / elapsed.as_secs_f64();
                    eprintln!(
                        "[{}] Processed {} features in {:.2?} ({} features/sec)",
                        table_name_clone,
                        total_features.to_formatted_string(&Locale::en),
                        elapsed,
                        (fps as u64).to_formatted_string(&Locale::en)
                    );
                }
            }

            let elapsed = start_time.elapsed();
            let fps = total_features as f64 / elapsed.as_secs_f64();
            eprintln!(
                "[{}] Finished processing {} features in {:.2?} ({} features/sec)",
                table_name_clone,
                total_features.to_formatted_string(&Locale::en),
                elapsed,
                (fps as u64).to_formatted_string(&Locale::en)
            );

            writer
                .close()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            Ok(())
        },
    );

    use itertools::Itertools;

    let chunk_size = 4_096;

    paths_iter
        .batching(|it| {
            let mut batch = Vec::with_capacity(chunk_size);
            for _ in 0..chunk_size {
                if let Some(item) = it.next() {
                    batch.push(item);
                } else {
                    break;
                }
            }
            if batch.is_empty() { None } else { Some(batch) }
        })
        .par_bridge()
        .for_each_with(tx, |tx, chunk| {
            let mut col_builders: Vec<KartBuilder> = Vec::with_capacity(kart_schema.len());
            for field in &kart_schema {
                let builder = match field.data_type.as_str() {
                    "boolean" => KartBuilder::Boolean(BooleanBuilder::new()),
                    "integer" => KartBuilder::Int64(Int64Builder::new()),
                    "float" => KartBuilder::Float64(Float64Builder::new()),
                    "blob" => KartBuilder::Binary(BinaryBuilder::new()),
                    "geometry" => KartBuilder::Binary(BinaryBuilder::new()),
                    _ => KartBuilder::String(StringBuilder::new()),
                };
                col_builders.push(builder);
            }

            let mut row_count = 0;

            for path in chunk {
                let res = source.read_feature(&path, |buffer| {
                    if buffer.is_empty() {
                        return None;
                    }

                    let val: Result<Value, _> = rmp_serde::from_slice(buffer);

                    Some(val)
                });

                let val_result = match res {
                    Ok(Some(v)) => v,
                    Ok(None) => continue,
                    Err(e) => {
                        eprintln!("[{}] Failed to read feature {path}: {e}", table_name);
                        continue;
                    }
                };

                let val = match val_result {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("[{}] Failed to decode feature {path}: {e}", table_name);
                        continue;
                    }
                };

                let Value::Array(arr) = val else {
                    continue;
                };

                if arr.len() < 2 {
                    continue;
                }

                let Value::String(ref legend_id_val) = arr[0] else {
                    continue;
                };

                let legend_id = legend_id_val.as_str().unwrap_or_default();

                let Some(mapping) = legend_schema_mapping.get(legend_id) else {
                    continue;
                };

                let Value::Array(val_arr) = &arr[1] else {
                    continue;
                };

                row_count += 1;

                for (idx, msgpack_idx_opt) in mapping.iter().enumerate() {
                    let builder = &mut col_builders[idx];

                    let val_opt = if let Some(msgpack_idx) = msgpack_idx_opt {
                        if *msgpack_idx < val_arr.len() {
                            Some(&val_arr[*msgpack_idx])
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    match builder {
                        KartBuilder::Boolean(b_builder) => match val_opt {
                            Some(Value::Boolean(b)) => b_builder.append_value(*b),
                            _ => b_builder.append_null(),
                        },
                        KartBuilder::Int64(i_builder) => match val_opt {
                            Some(Value::Integer(n)) => {
                                if n.is_i64() {
                                    i_builder.append_value(n.as_i64().unwrap());
                                } else {
                                    i_builder.append_value(n.as_u64().unwrap() as i64);
                                }
                            }
                            _ => i_builder.append_null(),
                        },
                        KartBuilder::Float64(f_builder) => match val_opt {
                            Some(Value::F64(f)) => f_builder.append_value(*f),
                            Some(Value::F32(f)) => f_builder.append_value(*f as f64),
                            _ => f_builder.append_null(),
                        },
                        KartBuilder::String(s_builder) => match val_opt {
                            Some(Value::String(s)) => s_builder.append_value(s.as_str().unwrap()),
                            Some(Value::Integer(n)) => s_builder.append_value(n.to_string()),
                            Some(Value::F64(f)) => s_builder.append_value(f.to_string()),
                            Some(Value::F32(f)) => s_builder.append_value(f.to_string()),
                            Some(Value::Boolean(b)) => s_builder.append_value(b.to_string()),
                            Some(Value::Map(_)) | Some(Value::Array(_)) => {
                                if let Ok(json_str) = serde_json::to_string(val_opt.unwrap()) {
                                    s_builder.append_value(json_str);
                                } else {
                                    s_builder.append_null();
                                }
                            }
                            _ => s_builder.append_null(),
                        },
                        KartBuilder::Binary(bin_builder) => match val_opt {
                            Some(Value::Ext(type_id, data)) if *type_id == 71 => {
                                let wkb = extract_wkb(data);
                                bin_builder.append_value(wkb);
                            }
                            Some(Value::Binary(b)) => bin_builder.append_value(b),
                            _ => bin_builder.append_null(),
                        },
                    }
                }
            }

            if row_count > 0 {
                let mut columns: Vec<ArrayRef> = Vec::new();
                for builder in col_builders {
                    match builder {
                        KartBuilder::Boolean(mut b) => columns.push(Arc::new(b.finish())),
                        KartBuilder::Int64(mut b) => columns.push(Arc::new(b.finish())),
                        KartBuilder::Float64(mut b) => columns.push(Arc::new(b.finish())),
                        KartBuilder::String(mut b) => columns.push(Arc::new(b.finish())),
                        KartBuilder::Binary(mut b) => columns.push(Arc::new(b.finish())),
                    }
                }

                match RecordBatch::try_new(arrow_schema.clone(), columns) {
                    Ok(batch) => {
                        if let Err(e) = tx.send(batch) {
                            eprintln!("[{}] Failed to send batch: {e}", table_name);
                        }
                    }
                    Err(e) => {
                        eprintln!("[{}] Failed to create batch: {e}", table_name);
                    }
                }
            }
        });

    match writer_handle.join() {
        Ok(result) => {
            if let Err(e) = result {
                eprintln!("[{}] Writer thread error: {e}", table_name);
            }
        }
        Err(e) => eprintln!("[{}] Writer thread panicked: {e:?}", table_name),
    }

    eprintln!(
        "[{}] Successfully wrote GeoParquet to {}",
        table_name,
        output_path.display()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::RowAccessor;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn create_schema(
        root: &Path,
        fields: Vec<serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema_dir = root.join(".table-dataset/meta");
        fs::create_dir_all(&schema_dir)?;
        let schema_json = serde_json::to_string(&fields)?;
        fs::write(schema_dir.join("schema.json"), schema_json)?;
        Ok(())
    }

    fn create_legend(
        root: &Path,
        name: &str,
        pks: Vec<&str>,
        cols: Vec<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let legend_dir = root.join(".table-dataset/meta/legend");
        fs::create_dir_all(&legend_dir)?;
        let legend_data = serde_json::json!([pks, cols]);
        let legend_json = serde_json::to_string(&legend_data)?;
        fs::write(legend_dir.join(name), legend_json)?;
        Ok(())
    }

    fn create_crs(root: &Path, id: &str, wkt: &str) -> Result<(), Box<dyn std::error::Error>> {
        let crs_dir = root.join(".table-dataset/meta/crs");
        fs::create_dir_all(&crs_dir)?;
        fs::write(crs_dir.join(format!("{}.wkt", id)), wkt)?;
        Ok(())
    }

    #[test]
    fn test_json_export() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path();

        create_schema(
            root,
            vec![
                serde_json::json!({"id": "col1", "name": "name", "dataType": "text"}),
                serde_json::json!({"id": "col2", "name": "metadata", "dataType": "json"}),
            ],
        )?;

        create_legend(root, "legend1", vec![], vec!["col1", "col2"])?;

        let feature_dir = root.join(".table-dataset/feature");
        fs::create_dir_all(&feature_dir)?;

        let feature_data = Value::Array(vec![
            Value::String("legend1".into()),
            Value::Array(vec![
                Value::String("test_name".into()),
                Value::Map(vec![
                    (Value::String("key".into()), Value::String("value".into())),
                    (
                        Value::String("list".into()),
                        Value::Array(vec![Value::Integer(1.into()), Value::Integer(2.into())]),
                    ),
                ]),
            ]),
        ]);

        let mut feature_bytes = Vec::new();
        rmp_serde::encode::write(&mut feature_bytes, &feature_data)?;
        fs::write(feature_dir.join("feat1"), feature_bytes)?;

        let output_path = root.join("output.parquet");

        let args = ExportArgs {
            path: root.to_string_lossy().to_string(),
            output: Some(output_path.to_string_lossy().to_string()),
            compression: "uncompressed".to_string(),
            compression_level: None,
            row_group_size: 1000,
            git_rev: None,
            git_prefix: None,
            all: false,
        };

        run(args)?;

        let file = File::open(&output_path)?;
        let reader = SerializedFileReader::new(file)?;
        let iter = reader.get_row_iter(None)?;

        let rows: Vec<_> = iter.collect();
        assert_eq!(rows.len(), 1);

        let row = rows[0].as_ref().unwrap();
        let name = row.get_string(0)?;
        let metadata = row.get_string(1)?;

        assert_eq!(name, "test_name");

        let parsed: serde_json::Value = serde_json::from_str(metadata)?;
        assert_eq!(parsed["key"], "value");
        assert_eq!(parsed["list"][0], 1);
        assert_eq!(parsed["list"][1], 2);

        Ok(())
    }

    #[test]
    fn test_projection_export() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path();

        create_schema(
            root,
            vec![
                serde_json::json!({"id": "col1", "name": "geom", "dataType": "geometry", "geometryCRS": "2193"}),
            ],
        )?;

        let wkt_2193 = r#"PROJCS["NZGD2000 / New Zealand Transverse Mercator 2000",GEOGCS["NZGD2000",DATUM["New_Zealand_Geodetic_Datum_2000",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6167"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4167"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",173],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",1600000],PARAMETER["false_northing",10000000],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","2193"]]"#;
        create_crs(root, "2193", wkt_2193)?;

        create_legend(root, "legend1", vec![], vec!["col1"])?;

        let feature_dir = root.join(".table-dataset/feature");
        fs::create_dir_all(&feature_dir)?;

        let wkb = hex::decode("010100000000000000000000000000000000000000")?;

        let feature_data = Value::Array(vec![
            Value::String("legend1".into()),
            Value::Array(vec![Value::Binary(wkb)]),
        ]);

        let mut feature_bytes = Vec::new();
        rmp_serde::encode::write(&mut feature_bytes, &feature_data)?;
        fs::write(feature_dir.join("feat1"), feature_bytes)?;

        let output_path = root.join("output.parquet");

        let args = ExportArgs {
            path: root.to_string_lossy().to_string(),
            output: Some(output_path.to_string_lossy().to_string()),
            compression: "uncompressed".to_string(),
            compression_level: None,
            row_group_size: 1000,
            git_rev: None,
            git_prefix: None,
            all: false,
        };

        run(args)?;

        let file = File::open(&output_path)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();
        let kv_metadata = metadata.file_metadata().key_value_metadata().unwrap();

        let geo_meta_str = kv_metadata
            .iter()
            .find(|kv| kv.key == "geo")
            .unwrap()
            .value
            .as_ref()
            .unwrap();
        let geo_meta: serde_json::Value = serde_json::from_str(geo_meta_str)?;

        let crs = &geo_meta["columns"]["geom"]["crs"];

        let wkt_out = crs["wkt"].as_str().unwrap();
        assert!(wkt_out.contains(r#"AUTHORITY["EPSG","2193"]"#));

        Ok(())
    }

    #[test]
    fn test_data_fidelity() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path();

        create_schema(
            root,
            vec![
                serde_json::json!({"id": "col_text", "name": "text_field", "dataType": "text"}),
                serde_json::json!({"id": "col_int", "name": "int_field", "dataType": "integer"}),
                serde_json::json!({"id": "col_float", "name": "float_field", "dataType": "float"}),
                serde_json::json!({"id": "col_bool", "name": "bool_field", "dataType": "boolean"}),
                serde_json::json!({"id": "col_json", "name": "json_field", "dataType": "json"}),
                serde_json::json!({"id": "col_geom", "name": "geom_field", "dataType": "geometry"}),
            ],
        )?;

        create_legend(
            root,
            "legend1",
            vec![],
            vec![
                "col_text",
                "col_int",
                "col_float",
                "col_bool",
                "col_json",
                "col_geom",
            ],
        )?;

        let feature_dir = root.join(".table-dataset/feature");
        fs::create_dir_all(&feature_dir)?;

        let wkb = hex::decode("010100000000000000000000000000000000000000")?;

        let feature_data = Value::Array(vec![
            Value::String("legend1".into()),
            Value::Array(vec![
                Value::String("some text".into()),
                Value::Integer(42.into()),
                Value::F64(3.14159),
                Value::Boolean(true),
                Value::Map(vec![(
                    Value::String("foo".into()),
                    Value::String("bar".into()),
                )]),
                Value::Binary(wkb.clone()),
            ]),
        ]);

        let mut feature_bytes = Vec::new();
        rmp_serde::encode::write(&mut feature_bytes, &feature_data)?;
        fs::write(feature_dir.join("feat1"), feature_bytes)?;

        let output_path = root.join("output.parquet");

        let args = ExportArgs {
            path: root.to_string_lossy().to_string(),
            output: Some(output_path.to_string_lossy().to_string()),
            compression: "uncompressed".to_string(),
            compression_level: None,
            row_group_size: 1000,
            git_rev: None,
            git_prefix: None,
            all: false,
        };

        run(args)?;

        let file = File::open(&output_path)?;
        let reader = SerializedFileReader::new(file)?;
        let iter = reader.get_row_iter(None)?;

        let rows: Vec<_> = iter.collect();
        assert_eq!(rows.len(), 1);

        let row = rows[0].as_ref().unwrap();

        assert_eq!(row.get_string(0)?, "some text");
        assert_eq!(row.get_long(1)?, 42);
        assert!((row.get_double(2)? - 3.14159).abs() < 1e-10);
        assert_eq!(row.get_bool(3)?, true);

        let json_str = row.get_string(4)?;
        let json_val: serde_json::Value = serde_json::from_str(json_str)?;
        assert_eq!(json_val["foo"], "bar");

        let geom_bytes = row.get_bytes(5)?;
        assert_eq!(geom_bytes.data(), wkb.as_slice());

        Ok(())
    }

    #[test]
    fn test_multiple_datasets_collision() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path();

        // Dataset 1
        let ds1 = root.join("dataset1");
        fs::create_dir(&ds1)?;
        create_schema(
            &ds1,
            vec![serde_json::json!({"id": "col1", "name": "c1", "dataType": "text"})],
        )?;

        // Dataset 2
        let ds2 = root.join("dataset2");
        fs::create_dir(&ds2)?;
        create_schema(
            &ds2,
            vec![serde_json::json!({"id": "col1", "name": "c1", "dataType": "text"})],
        )?;

        let args = ExportArgs {
            path: root.to_string_lossy().to_string(),
            output: None,
            compression: "uncompressed".to_string(),
            compression_level: None,
            row_group_size: 1000,
            git_rev: None,
            git_prefix: None,
            all: false,
        };

        let result = run(args);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Multiple datasets found"));

        Ok(())
    }

    #[test]
    fn test_ambiguity_resolution() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path();

        // Dataset 1: matching
        let ds1 = root.join("my_dataset");
        fs::create_dir(&ds1)?;
        create_schema(
            &ds1,
            vec![serde_json::json!({"id": "col1", "name": "c1", "dataType": "text"})],
        )?;

        // Dataset 2: unrelated
        let ds2 = root.join("other_dataset");
        fs::create_dir(&ds2)?;
        create_schema(
            &ds2,
            vec![serde_json::json!({"id": "col1", "name": "c1", "dataType": "text"})],
        )?;

        // Output matches ds1
        let output_path = root.join("my_dataset.parquet");

        let args = ExportArgs {
            path: root.to_string_lossy().to_string(),
            output: Some(output_path.to_string_lossy().to_string()),
            compression: "uncompressed".to_string(),
            compression_level: None,
            row_group_size: 1000,
            git_rev: None,
            git_prefix: None,
            all: false,
        };

        // Should succeed by selecting ds1
        run(args)?;

        // Output ambiguous (matches neither uniquely or matches multiple - here checking no match behavior if name implies one)
        // If we provide output name that matches NEITHER, it should fail.
        let output_path_bad = root.join("random_output.parquet");
        let args_bad = ExportArgs {
            path: root.to_string_lossy().to_string(),
            output: Some(output_path_bad.to_string_lossy().to_string()),
            compression: "uncompressed".to_string(),
            compression_level: None,
            row_group_size: 1000,
            git_rev: None,
            git_prefix: None,
            all: false,
        };
        let result = run(args_bad);
        assert!(result.is_err()); // Ambiguous because none matched filter

        Ok(())
    }

    #[test]
    fn test_export_all() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path();

        // Dataset 1
        let ds1 = root.join("dataset1");
        fs::create_dir(&ds1)?;
        create_schema(
            &ds1,
            vec![serde_json::json!({"id": "col1", "name": "c1", "dataType": "text"})],
        )?;
        create_legend(&ds1, "legend", vec![], vec!["col1"])?;
        // Add feature to ds1
        let feat_dir1 = ds1.join(".table-dataset/feature");
        fs::create_dir_all(&feat_dir1)?;
        let feature_data1 = Value::Array(vec![
            Value::String("legend".into()),
            Value::Array(vec![Value::String("d1".into())]),
        ]);
        let mut bytes1 = Vec::new();
        rmp_serde::encode::write(&mut bytes1, &feature_data1)?;
        fs::write(feat_dir1.join("f1"), bytes1)?;

        // Dataset 2
        let ds2 = root.join("dataset2");
        fs::create_dir(&ds2)?;
        create_schema(
            &ds2,
            vec![serde_json::json!({"id": "col1", "name": "c1", "dataType": "text"})],
        )?;
        create_legend(&ds2, "legend", vec![], vec!["col1"])?;
        // Add feature to ds2
        let feat_dir2 = ds2.join(".table-dataset/feature");
        fs::create_dir_all(&feat_dir2)?;
        let feature_data2 = Value::Array(vec![
            Value::String("legend".into()),
            Value::Array(vec![Value::String("d2".into())]),
        ]);
        let mut bytes2 = Vec::new();
        rmp_serde::encode::write(&mut bytes2, &feature_data2)?;
        fs::write(feat_dir2.join("f1"), bytes2)?;

        // Case 1: export all to default loc (root)
        let args = ExportArgs {
            path: root.to_string_lossy().to_string(),
            output: Some(root.to_string_lossy().to_string()),
            compression: "uncompressed".to_string(),
            compression_level: None,
            row_group_size: 1000,
            git_rev: None,
            git_prefix: None,
            all: true,
        };
        run(args)?;

        assert!(root.join("dataset1.parquet").exists());
        assert!(root.join("dataset2.parquet").exists());

        // Case 2: export all to specific directory
        let out_dir = root.join("out");
        let args_dir = ExportArgs {
            path: root.to_string_lossy().to_string(),
            output: Some(out_dir.to_string_lossy().to_string()),
            compression: "uncompressed".to_string(),
            compression_level: None,
            row_group_size: 1000,
            git_rev: None,
            git_prefix: None,
            all: true,
        };
        run(args_dir)?;
        assert!(out_dir.join("dataset1.parquet").exists());
        assert!(out_dir.join("dataset2.parquet").exists());

        Ok(())
    }
}
