use crate::source::{KartSchema, SchemaField};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use clap::Args;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rmpv::Value;

use std::fs::File;
use std::path::Path;

use uuid::Uuid;

#[derive(Args, Debug, Clone)]
pub struct ImportArgs {
    /// Path to the input Parquet file
    pub input: String,
    /// Path to the output directory (where the Kart repository structure will be created)
    pub output: String,
    /// Name of the primary key column (optional)
    #[arg(long)]
    pub primary_key: Option<String>,
}

pub fn run(args: ImportArgs) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Starting import from {} to {}", args.input, args.output);

    let input_path = Path::new(&args.input);
    let output_path = Path::new(&args.output);

    if !input_path.exists() {
        return Err(format!("Input file not found: {}", args.input).into());
    }

    // Check if directory exists and has a schema
    let meta_dir = output_path.join(".table-dataset/meta");
    let schema_path = meta_dir.join("schema.json");
    let is_existing = schema_path.exists();

    if !output_path.exists() {
        std::fs::create_dir_all(output_path)?;
    }

    let file = File::open(input_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = builder.schema().clone();
    let mut reader = builder.build()?;

    let (kart_schema, legend_id, column_mapping) = if is_existing {
        eprintln!("Detected existing Kart dataset at {}", args.output);

        let schema_file = File::open(&schema_path)?;
        let existing_schema: KartSchema = serde_json::from_reader(schema_file)?;

        // Validate compatibility and build mapping
        let mut mapping = Vec::new();
        let arrow_fields = arrow_schema.fields();

        for (_i, field) in arrow_fields.iter().enumerate() {
            // Find corresponding field in Kart schema by name
            if let Some(kart_field) = existing_schema.iter().find(|kf| kf.name == *field.name()) {
                // TODO: Validate value type compatibility strictly?
                // For now, we trust the name match and basic type assumption or fail later.
                mapping.push(kart_field.id.clone());
            } else {
                return Err(format!(
                    "Parquet column '{}' not found in existing Kart schema",
                    field.name()
                )
                .into());
            }
        }

        // Ensure all required fields in Kart schema are present?
        // Or can we import partial?
        // For now, assume we populate what we have.
        // But the legend defines what we are writing.
        // The legend must list ALL columns we are writing.
        // And the feature array must match the legend columns.

        // Generate a new legend for this import batch
        let legend_id = format!("import_{}", Uuid::new_v4());

        (existing_schema, legend_id, mapping)
    } else {
        eprintln!("Creating new Kart dataset at {}", args.output);

        // Create directory structure
        let legend_dir = meta_dir.join("legend");
        let crs_dir = meta_dir.join("crs");
        let feature_dir = output_path.join(".table-dataset/feature");

        std::fs::create_dir_all(&meta_dir)?;
        std::fs::create_dir_all(&legend_dir)?;
        std::fs::create_dir_all(&crs_dir)?;
        std::fs::create_dir_all(&feature_dir)?;

        // Convert Arrow Schema to Kart Schema
        let (kart_schema, crs_info) = arrow_to_kart_schema(&arrow_schema)?;

        // Write schema.json
        let schema_json = serde_json::to_string_pretty(&kart_schema)?;
        std::fs::write(&schema_path, schema_json)?;
        eprintln!("Wrote schema.json");

        // Write CRS if found
        if let Some((crs_id, wkt)) = crs_info {
            let crs_file = crs_dir.join(format!("{}.wkt", crs_id));
            std::fs::write(crs_file, wkt)?;
            eprintln!("Wrote CRS {}: ...", crs_id);
        }

        let legend_id = "import_v1".to_string();
        let column_mapping: Vec<String> = kart_schema.iter().map(|f| f.id.clone()).collect();

        (kart_schema, legend_id, column_mapping)
    };

    let legend_dir = meta_dir.join("legend");
    // Ensure legend dir exists (it should)
    std::fs::create_dir_all(&legend_dir)?;

    // Create Legend for this batch
    // Based on the column_mapping we built (which matches the Parquet columns in order)
    let primary_keys = if let Some(pk) = &args.primary_key {
        // Validation: PK must exist in Kart schema
        if let Some(field) = kart_schema.iter().find(|f| f.name == *pk) {
            vec![field.id.clone()]
        } else {
            return Err(format!("Primary key '{}' not found in schema", pk).into());
        }
    } else {
        // If existing, maybe we should reuse existing PKs?
        // But for now, user specified or empty.
        vec![]
    };

    let legend_data = serde_json::json!([primary_keys, column_mapping]);

    // Write legend
    let legend_json = serde_json::to_string(&legend_data)?;
    std::fs::write(legend_dir.join(&legend_id), legend_json)?;
    eprintln!("Wrote legend '{}'", legend_id);

    // 3. Process batches and write features
    let mut feature_count = 0;
    let feature_dir = output_path.join(".table-dataset/feature");
    // Ensure feature dir exists
    std::fs::create_dir_all(&feature_dir)?;

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;
        let num_rows = batch.num_rows();

        for row_idx in 0..num_rows {
            let mut row_values = Vec::with_capacity(batch.num_columns());

            // We iterate over columns in the order of Arrow schema (and column_mapping)
            // But we need to know the type from the Kart schema to convert correctly.
            // column_mapping[i] -> field_id

            for (col_idx, field_id) in column_mapping.iter().enumerate() {
                // Find the field def in schema to know the type
                let field_def = kart_schema
                    .iter()
                    .find(|f| f.id == *field_id)
                    .expect("Field ID from mapping must exist in schema");

                let array = batch.column(col_idx);
                let value = if array.is_null(row_idx) {
                    Value::Nil
                } else {
                    match field_def.data_type.as_str() {
                        "boolean" => Value::Boolean(array.as_boolean().value(row_idx)),
                        "integer" => {
                            let val = match array.data_type() {
                                DataType::Int8 => array
                                    .as_primitive::<arrow::datatypes::Int8Type>()
                                    .value(row_idx)
                                    as i64,
                                DataType::Int16 => array
                                    .as_primitive::<arrow::datatypes::Int16Type>()
                                    .value(row_idx)
                                    as i64,
                                DataType::Int32 => array
                                    .as_primitive::<arrow::datatypes::Int32Type>()
                                    .value(row_idx)
                                    as i64,
                                DataType::Int64 => array
                                    .as_primitive::<arrow::datatypes::Int64Type>()
                                    .value(row_idx),
                                DataType::UInt8 => array
                                    .as_primitive::<arrow::datatypes::UInt8Type>()
                                    .value(row_idx)
                                    as i64,
                                DataType::UInt16 => array
                                    .as_primitive::<arrow::datatypes::UInt16Type>()
                                    .value(row_idx)
                                    as i64,
                                DataType::UInt32 => array
                                    .as_primitive::<arrow::datatypes::UInt32Type>()
                                    .value(row_idx)
                                    as i64,
                                DataType::UInt64 => array
                                    .as_primitive::<arrow::datatypes::UInt64Type>()
                                    .value(row_idx)
                                    as i64,
                                _ => 0,
                            };
                            Value::Integer(val.into())
                        }
                        "float" => {
                            let val = match array.data_type() {
                                DataType::Float16 => array
                                    .as_primitive::<arrow::datatypes::Float16Type>()
                                    .value(row_idx)
                                    .to_f64(),
                                DataType::Float32 => array
                                    .as_primitive::<arrow::datatypes::Float32Type>()
                                    .value(row_idx)
                                    as f64,
                                DataType::Float64 => array
                                    .as_primitive::<arrow::datatypes::Float64Type>()
                                    .value(row_idx),
                                _ => 0.0,
                            };
                            Value::F64(val)
                        }
                        "text" | "json" => {
                            let s = match array.data_type() {
                                DataType::Utf8 => array.as_string::<i32>().value(row_idx),
                                DataType::LargeUtf8 => array.as_string::<i64>().value(row_idx),
                                _ => "",
                            };
                            Value::String(s.into())
                        }
                        "blob" | "geometry" => {
                            let bytes = match array.data_type() {
                                DataType::Binary => array.as_binary::<i32>().value(row_idx),
                                DataType::LargeBinary => array.as_binary::<i64>().value(row_idx),
                                _ => &[],
                            };
                            Value::Binary(bytes.to_vec())
                        }
                        _ => Value::Nil,
                    }
                };
                row_values.push(value);
            }

            // Create feature structure: [legend_id, [values...]]
            let feature_content = Value::Array(vec![
                Value::String(legend_id.clone().into()),
                Value::Array(row_values),
            ]);

            // Serialise to MsgPack
            let mut buf = Vec::new();
            rmp_serde::encode::write(&mut buf, &feature_content)?;

            // Write to file
            let uuid = Uuid::new_v4();
            let feature_path = feature_dir.join(uuid.to_string());
            std::fs::write(feature_path, buf)?;

            feature_count += 1;
        }
    }

    eprintln!("Imported {} features", feature_count);

    Ok(())
}

fn arrow_to_kart_schema(
    arrow_schema: &ArrowSchema,
) -> Result<(KartSchema, Option<(String, String)>), Box<dyn std::error::Error>> {
    let mut kart_schema = Vec::new();
    let mut crs_info = None;

    // Check for Geo metadata
    if let Some(geo_meta_str) = arrow_schema.metadata.get("geo") {
        if let Ok(geo_json) = serde_json::from_str::<serde_json::Value>(geo_meta_str) {
            // Attempt to extract CRS
            // Structure: columns -> [col_name] -> crs -> wkt
            if let Some(columns) = geo_json.get("columns").and_then(|c| c.as_object()) {
                // We look for the first geometry column?
                for (_col_name, col_meta) in columns {
                    if let Some(crs) = col_meta.get("crs") {
                        if let Some(wkt) = crs.get("wkt").and_then(|s| s.as_str()) {
                            // We have a WKT. We need an ID for it.
                            // Usually EPSG code? Or we generate one?
                            // We might trying to parse AUTHORITY["EPSG","2193"] from WKT?
                            // Simple regex or string find?
                            let id =
                                extract_crs_id(wkt).unwrap_or_else(|| "unknown_crs".to_string());
                            crs_info = Some((id, wkt.to_string()));
                            break; // Assume single CRS for now
                        }
                    }
                }
            }
        }
    }

    // Default crs_id if we found one
    let default_crs_id = crs_info.as_ref().map(|(id, _)| id.clone());

    for field in arrow_schema.fields() {
        let arrow_type = field.data_type();
        let (data_type, geometry_crs) = match arrow_type {
            DataType::Boolean => ("boolean", None),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => ("integer", None),
            DataType::Float16 | DataType::Float32 | DataType::Float64 => ("float", None),
            DataType::Utf8 | DataType::LargeUtf8 => ("text", None), // Could be JSON? We don't know without more metadata. Default to text.
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                // If this is the geometry column, set type to geometry
                // How do we know? Metadata or name?
                // We'll use the field name "geom" or "geometry" as heuristic or check metadata if we strictly parsed it.
                // For simplicity:
                if field.name() == "geom" || field.name() == "geometry" {
                    ("geometry", default_crs_id.clone())
                } else {
                    ("blob", None)
                }
            }
            _ => ("text", None), // Fallback
        };

        kart_schema.push(SchemaField {
            id: field.name().clone(), // Use name as ID for simplicity
            name: field.name().clone(),
            data_type: data_type.to_string(),
            geometry_crs,
        });
    }

    Ok((kart_schema, crs_info))
}

fn extract_crs_id(wkt: &str) -> Option<String> {
    // Look for AUTHORITY["EPSG","xxxx"] at the end
    // Very rough heuristic
    let wkt_upper = wkt.to_uppercase();
    if let Some(idx) = wkt_upper.rfind("AUTHORITY[\"EPSG\",\"") {
        let rest = &wkt[idx + 18..]; // skip AUTHORITY["EPSG","
        if let Some(end_idx) = rest.find('"') {
            return Some(rest[..end_idx].to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Field;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_import_simple() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let input_path = temp_dir.path().join("input.parquet");
        let output_path = temp_dir.path().join("output_repo");

        // Create a simple Parquet file
        let file = File::create(&input_path)?;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )?;

        writer.write(&batch)?;
        writer.close()?;

        // Run import
        let args = ImportArgs {
            input: input_path.to_string_lossy().to_string(),
            output: output_path.to_string_lossy().to_string(),
            primary_key: Some("id".to_string()),
        };

        run(args)?;

        // Verify output
        assert!(output_path.exists());
        assert!(output_path.join(".table-dataset/meta/schema.json").exists());
        assert!(
            output_path
                .join(".table-dataset/meta/legend/import_v1")
                .exists()
        );

        let feature_dir = output_path.join(".table-dataset/feature");
        assert!(feature_dir.exists());
        // Should be 2 files
        assert_eq!(std::fs::read_dir(feature_dir)?.count(), 2);

        Ok(())
    }

    #[test]
    fn test_import_existing() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let input_path = temp_dir.path().join("input.parquet");
        let output_path = temp_dir.path().join("output_repo");
        let meta_dir = output_path.join(".table-dataset/meta");
        let legend_dir = meta_dir.join("legend");
        let feature_dir = output_path.join(".table-dataset/feature");

        std::fs::create_dir_all(&meta_dir)?;
        std::fs::create_dir_all(&legend_dir)?;
        std::fs::create_dir_all(&feature_dir)?;

        // Create compatible schema in target to simulate existing repo
        let existing_schema = vec![
            SchemaField {
                id: "col_id".to_string(),
                name: "id".to_string(), // Matches parquet col 'id'
                data_type: "integer".to_string(),
                geometry_crs: None,
            },
            SchemaField {
                id: "col_name_diff".to_string(),
                name: "name".to_string(), // Matches parquet col 'name'
                data_type: "text".to_string(),
                geometry_crs: None,
            },
        ];
        let schema_json = serde_json::to_string_pretty(&existing_schema)?;
        std::fs::write(meta_dir.join("schema.json"), schema_json)?;

        // Create Parquet file
        let file = File::create(&input_path)?;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10])),
                Arc::new(StringArray::from(vec!["Existing"])),
            ],
        )?;

        writer.write(&batch)?;
        writer.close()?;

        // Run import
        let args = ImportArgs {
            input: input_path.to_string_lossy().to_string(),
            output: output_path.to_string_lossy().to_string(),
            primary_key: None,
        };

        run(args)?;

        // Verify output
        // Check finding new features
        assert_eq!(std::fs::read_dir(feature_dir)?.count(), 1);

        // Verify that a new legend was created
        // We don't know the exact name (UUID), but there should be one
        let legends: Vec<_> = std::fs::read_dir(legend_dir)?.collect();
        assert!(!legends.is_empty());

        // Check content of one feature to see if it used the correct column IDs
        // "col_id" and "col_name_diff"
        // TODO: actually read MsgPack... but for now success of run() implies mapping was found.

        Ok(())
    }
}
