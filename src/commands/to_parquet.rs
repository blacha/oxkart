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

use crate::source::{FsGit, FsSource, KartSourceEnum};

#[derive(Args, Debug, Clone)]
pub struct ToParquetArgs {
    /// Path to the dataset directory (e.g., sample/nz_topo_map_sheet)
    pub path: String,
    /// Output Parquet file path
    pub output: String,
    /// Compression (uncompressed, snappy, gzip, lzo, brotli, lz4, zstd)
    #[arg(long, default_value = "zstd")]
    pub compression: String,
    /// Compression level (for ZSTD, etc.)
    #[arg(long)]
    pub compression_level: Option<i32>,
    /// Max row group size (number of rows)
    #[arg(long, default_value_t = 100000)]
    pub row_group_size: usize,
    /// Git revision (branch, tag, sha) to read from (default: HEAD)
    #[arg(long)]
    pub git_rev: Option<String>,
    /// Path inside the Git repository to the dataset root
    #[arg(long)]
    pub git_prefix: Option<String>,
}

fn extract_wkb(data: &[u8]) -> &[u8] {
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

pub fn run(args: ToParquetArgs) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Starting to-parquet conversion...");

    let path = std::path::Path::new(&args.path);
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

    let mut source = if is_git {
        eprintln!("Using Git source");
        if let Some(ref rev) = args.git_rev {
            eprintln!("Revision: {}", rev);
        }
        if let Some(ref prefix) = args.git_prefix {
            eprintln!("Prefix: {}", prefix);
        }
        KartSourceEnum::Git(FsGit::new(
            &final_path,
            args.git_rev.as_deref(),
            args.git_prefix.as_deref(),
        )?)
    } else {
        eprintln!("Using Filesystem source");
        KartSourceEnum::Fs(FsSource::new(&final_path))
    };

    let kart_schema = match source.get_schema() {
        Ok(s) => s,
        Err(e) => {
            if is_git && args.git_prefix.is_none() {
                // Try to infer prefix from output filename
                let output_path = Path::new(&args.output);
                if let Some(file_stem) = output_path.file_stem() {
                    let inferred_prefix = file_stem.to_string_lossy().to_string();
                    eprintln!(
                        "Failed to load schema from root. Trying inferred prefix: '{}'",
                        inferred_prefix
                    );

                    // Re-initialize source with inferred prefix
                    let new_source =
                        FsGit::new(&final_path, args.git_rev.as_deref(), Some(&inferred_prefix))?;
                    match new_source.get_schema() {
                        Ok(s) => {
                            source = KartSourceEnum::Git(new_source); // Update source to use the new one
                            s
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

        arrow_fields.push(Field::new(&field.name, arrow_type, true));
        field_indices.insert(field.id.clone(), i);
    }

    let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

    eprintln!("Loading legends...");
    let legend_cache = source.get_legends()?;
    eprintln!("Loaded {} legends", legend_cache.len());

    let paths_iter = source.feature_identifiers();

    let output_path = Path::new(&args.output);
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
                            "encoding": "wkt",
                            "wkt": clean_wkt
                        }),
                    );
                    eprintln!("Loaded CRS for {crs_id}");
                }
                Err(e) => {
                    eprintln!("Failed to load CRS for {crs_id}: {e}");
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
            let level = args.compression_level.unwrap_or(3);
            parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(level).unwrap_or_default(),
            )
        }
        _ => {
            eprintln!(
                "Unknown compression '{}', defaulting to ZSTD",
                args.compression
            );
            let level = args.compression_level.unwrap_or(3);
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
                total_features += batch_size;
            }

            let elapsed = start_time.elapsed();
            let fps = total_features as f64 / elapsed.as_secs_f64();
            eprintln!(
                "Finished processing {} features in {:.2?} ({} features/sec)",
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

    let chunk_size = 5000;

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
                        eprintln!("Failed to read feature {path}: {e}");
                        continue;
                    }
                };

                let val = match val_result {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Failed to decode feature {path}: {e}");
                        continue;
                    }
                };

                if let Value::Array(arr) = val {
                    if arr.len() >= 2 {
                        if let Value::String(ref legend_id_val) = arr[0] {
                            let legend_id = legend_id_val.as_str().unwrap_or_default();

                            if let Some(mapping) = legend_schema_mapping.get(legend_id) {
                                let values = &arr[1];
                                if let Value::Array(val_arr) = values {
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
                                                Some(Value::Boolean(b)) => {
                                                    b_builder.append_value(*b)
                                                }
                                                _ => b_builder.append_null(),
                                            },
                                            KartBuilder::Int64(i_builder) => match val_opt {
                                                Some(Value::Integer(n)) => {
                                                    if n.is_i64() {
                                                        i_builder.append_value(n.as_i64().unwrap());
                                                    } else {
                                                        i_builder.append_value(
                                                            n.as_u64().unwrap() as i64
                                                        );
                                                    }
                                                }
                                                _ => i_builder.append_null(),
                                            },
                                            KartBuilder::Float64(f_builder) => match val_opt {
                                                Some(Value::F64(f)) => f_builder.append_value(*f),
                                                Some(Value::F32(f)) => {
                                                    f_builder.append_value(*f as f64)
                                                }
                                                _ => f_builder.append_null(),
                                            },
                                            KartBuilder::String(s_builder) => match val_opt {
                                                Some(Value::String(s)) => {
                                                    s_builder.append_value(s.as_str().unwrap())
                                                }
                                                Some(Value::Integer(n)) => {
                                                    s_builder.append_value(n.to_string())
                                                }
                                                Some(Value::F64(f)) => {
                                                    s_builder.append_value(f.to_string())
                                                }
                                                Some(Value::F32(f)) => {
                                                    s_builder.append_value(f.to_string())
                                                }
                                                Some(Value::Boolean(b)) => {
                                                    s_builder.append_value(b.to_string())
                                                }
                                                _ => s_builder.append_null(),
                                            },
                                            KartBuilder::Binary(bin_builder) => match val_opt {
                                                Some(Value::Ext(type_id, data))
                                                    if *type_id == 71 =>
                                                {
                                                    let wkb = extract_wkb(data);
                                                    bin_builder.append_value(wkb);
                                                }
                                                Some(Value::Binary(b)) => {
                                                    bin_builder.append_value(b)
                                                }
                                                _ => bin_builder.append_null(),
                                            },
                                        }
                                    }
                                }
                            }
                        }
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
                            eprintln!("Failed to send batch: {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to create batch: {e}");
                    }
                }
            }
        });

    match writer_handle.join() {
        Ok(result) => {
            if let Err(e) = result {
                eprintln!("Writer thread error: {e}");
            }
        }
        Err(e) => eprintln!("Writer thread panicked: {e:?}"),
    }

    eprintln!("Successfully wrote GeoParquet to {}", output_path.display());

    Ok(())
}
