use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use log::{info, trace};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::errors::{ErrorKind, RWError};
use super::key_value_pair::KeyValuePair;
use super::key_value_store::KeyValueStore;

/// The Arrow schema shared by all Iceberg CDC tables produced by construct-cache.
fn cdc_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("timestamp", DataType::Int64, false),
    ]))
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// A lightweight catalog interface for the construct-cache.
///
/// Implementations manage *where* and *how* Iceberg-compatible Parquet
/// data files are stored and retrieved.  The trait is intentionally small
/// and implementation-agnostic so that the underlying storage backend
/// can be freely swapped (e.g. local filesystem, remote service, cloud
/// object store, or any custom backend) without changing consumer code.
pub trait IcebergCatalog: Send + Sync {
    /// Write a CDC checkpoint of the given `KeyValueStore` to the catalog.
    ///
    /// The implementation decides the file naming / path conventions.
    /// Returns the path (or identifier) of the written data file.
    fn write_checkpoint(&self, store: &KeyValueStore) -> Result<String, RWError>;

    /// Read the latest state for a table identified by `table_name` and
    /// return a `KeyValueStore` reflecting the most-recent value for every
    /// key (last-writer-wins by timestamp).
    fn read_latest(&self, table_name: &str) -> Result<KeyValueStore, RWError>;

    /// List the table names (i.e. KV store names) known to this catalog.
    fn list_tables(&self) -> Result<Vec<String>, RWError>;
}

// ---------------------------------------------------------------------------
// Filesystem implementation
// ---------------------------------------------------------------------------

/// A catalog backed by the local filesystem.
///
/// Layout on disk:
/// ```text
/// <warehouse_path>/
///   <table_name>/
///     data/
///       <epoch_ms>.parquet   -- CDC data files
/// ```
pub struct FileSystemCatalog {
    warehouse_path: PathBuf,
}

impl FileSystemCatalog {
    pub fn new(warehouse_path: &str) -> Result<Self, RWError> {
        let path = PathBuf::from(warehouse_path);
        if !path.exists() {
            fs::create_dir_all(&path).map_err(|e| RWError {
                kind_: ErrorKind::FileOpenError,
                context_: format!("Cannot create warehouse dir: {}", e),
            })?;
        }
        Ok(Self {
            warehouse_path: path,
        })
    }

    /// Return `<warehouse>/<table>/data/` creating it if necessary.
    fn table_data_dir(&self, table_name: &str) -> Result<PathBuf, RWError> {
        let dir = self.warehouse_path.join(table_name).join("data");
        if !dir.exists() {
            fs::create_dir_all(&dir).map_err(|e| RWError {
                kind_: ErrorKind::FileOpenError,
                context_: format!("Cannot create table data dir: {}", e),
            })?;
        }
        Ok(dir)
    }
}

impl IcebergCatalog for FileSystemCatalog {
    fn write_checkpoint(&self, store: &KeyValueStore) -> Result<String, RWError> {
        let table_name = store.name();
        let data_dir = self.table_data_dir(table_name)?;

        // Build Arrow arrays from the store.
        let data = store.data();
        let mut keys: Vec<String> = Vec::new();
        let mut values: Vec<Option<String>> = Vec::new();
        let mut timestamps: Vec<i64> = Vec::new();

        for (k, v) in &data.values {
            keys.push(k.clone());
            values.push(Some(v.value.clone()));
            timestamps.push(v.timestamp);
        }

        let schema = cdc_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(values)),
                Arc::new(Int64Array::from(timestamps)),
            ],
        )
        .map_err(|e| RWError {
            kind_: ErrorKind::FileWriteError,
            context_: format!("Arrow batch error: {}", e),
        })?;

        // Use current epoch millis as file name for ordering.
        let epoch_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let file_path = data_dir.join(format!("{}.parquet", epoch_ms));

        let file = fs::File::create(&file_path).map_err(|e| RWError {
            kind_: ErrorKind::FileOpenError,
            context_: e.to_string(),
        })?;

        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| RWError {
            kind_: ErrorKind::FileWriteError,
            context_: format!("Parquet writer init: {}", e),
        })?;

        writer.write(&batch).map_err(|e| RWError {
            kind_: ErrorKind::FileWriteError,
            context_: format!("Parquet write: {}", e),
        })?;

        writer.close().map_err(|e| RWError {
            kind_: ErrorKind::FileWriteError,
            context_: format!("Parquet close: {}", e),
        })?;

        let path_str = file_path.to_string_lossy().to_string();
        info!("Wrote CDC checkpoint: {}", path_str);
        Ok(path_str)
    }

    fn read_latest(&self, table_name: &str) -> Result<KeyValueStore, RWError> {
        let data_dir = self.warehouse_path.join(table_name).join("data");
        if !data_dir.exists() {
            return Err(RWError {
                kind_: ErrorKind::FileReadError,
                context_: format!("No data directory for table '{}'", table_name),
            });
        }

        // Collect all parquet files, sorted by name (epoch ordering).
        let mut parquet_files: Vec<PathBuf> = fs::read_dir(&data_dir)
            .map_err(|e| RWError {
                kind_: ErrorKind::FileReadError,
                context_: e.to_string(),
            })?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();

        parquet_files.sort();

        if parquet_files.is_empty() {
            return Err(RWError {
                kind_: ErrorKind::FileReadError,
                context_: format!("No parquet files for table '{}'", table_name),
            });
        }

        // Last-writer-wins merge: iterate all files in order and keep the
        // entry with the highest timestamp for each key.
        let mut latest: HashMap<String, (String, i64)> = HashMap::new();

        for pf in &parquet_files {
            let file = fs::File::open(pf).map_err(|e| RWError {
                kind_: ErrorKind::FileOpenError,
                context_: e.to_string(),
            })?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| RWError {
                kind_: ErrorKind::DataDecodeError,
                context_: format!("Parquet reader init: {}", e),
            })?;

            let reader = builder.build().map_err(|e| RWError {
                kind_: ErrorKind::DataDecodeError,
                context_: format!("Parquet reader build: {}", e),
            })?;

            for batch_result in reader {
                let batch = batch_result.map_err(|e| RWError {
                    kind_: ErrorKind::DataDecodeError,
                    context_: format!("Parquet batch read: {}", e),
                })?;

                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| RWError {
                        kind_: ErrorKind::DataDecodeError,
                        context_: "key column is not StringArray".into(),
                    })?;

                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| RWError {
                        kind_: ErrorKind::DataDecodeError,
                        context_: "value column is not StringArray".into(),
                    })?;

                let timestamps = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| RWError {
                        kind_: ErrorKind::DataDecodeError,
                        context_: "timestamp column is not Int64Array".into(),
                    })?;

                for row in 0..batch.num_rows() {
                    let k = keys.value(row).to_string();
                    let v = values.value(row).to_string();
                    let ts = timestamps.value(row);

                    match latest.get(&k) {
                        Some((_, existing_ts)) if *existing_ts >= ts => {
                            // Keep existing – it has a newer or equal timestamp.
                        }
                        _ => {
                            latest.insert(k, (v, ts));
                        }
                    }
                }
            }
        }

        // Build a KeyValueStore from the merged state.
        let mut store = KeyValueStore::new(table_name);
        for (k, (v, ts)) in latest {
            store.add(KeyValuePair::new(&k, &v, ts));
        }

        trace!(
            "Loaded {} keys from Iceberg table '{}'",
            store.data().values.len(),
            table_name
        );
        Ok(store)
    }

    fn list_tables(&self) -> Result<Vec<String>, RWError> {
        let mut tables = Vec::new();
        let entries = fs::read_dir(&self.warehouse_path).map_err(|e| RWError {
            kind_: ErrorKind::FileReadError,
            context_: e.to_string(),
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| RWError {
                kind_: ErrorKind::FileReadError,
                context_: e.to_string(),
            })?;
            if entry.path().is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    tables.push(name.to_string());
                }
            }
        }

        Ok(tables)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    fn test_warehouse(name: &str) -> String {
        let dir = format!("/tmp/construct_cache_test_{}", name);
        // Clean slate
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn test_write_and_read_checkpoint() {
        let wh = test_warehouse("write_read");
        let catalog = FileSystemCatalog::new(&wh).unwrap();

        let mut store = KeyValueStore::new("my_table");
        store.add(KeyValuePair::new("alpha", "one", 100));
        store.add(KeyValuePair::new("beta", "two", 200));

        let path = catalog.write_checkpoint(&store).unwrap();
        assert!(Path::new(&path).exists());

        let loaded = catalog.read_latest("my_table").unwrap();
        assert_eq!(loaded.name(), "my_table");
        assert_eq!(loaded.get("alpha").unwrap().value(), "one");
        assert_eq!(loaded.get("beta").unwrap().value(), "two");
    }

    #[test]
    fn test_last_writer_wins() {
        let wh = test_warehouse("lww");
        let catalog = FileSystemCatalog::new(&wh).unwrap();

        // First checkpoint.
        let mut store1 = KeyValueStore::new("lww_table");
        store1.add(KeyValuePair::new("key1", "old_value", 100));
        catalog.write_checkpoint(&store1).unwrap();

        // Small sleep so the filenames (epoch ms) differ.
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Second checkpoint with a newer timestamp for the same key.
        let mut store2 = KeyValueStore::new("lww_table");
        store2.add(KeyValuePair::new("key1", "new_value", 300));
        catalog.write_checkpoint(&store2).unwrap();

        let loaded = catalog.read_latest("lww_table").unwrap();
        assert_eq!(loaded.get("key1").unwrap().value(), "new_value");
        assert_eq!(loaded.get("key1").unwrap().timestamp(), 300);
    }

    #[test]
    fn test_list_tables() {
        let wh = test_warehouse("list_tables");
        let catalog = FileSystemCatalog::new(&wh).unwrap();

        let mut s1 = KeyValueStore::new("table_a");
        s1.add(KeyValuePair::new("k", "v", 1));
        catalog.write_checkpoint(&s1).unwrap();

        let mut s2 = KeyValueStore::new("table_b");
        s2.add(KeyValuePair::new("k", "v", 1));
        catalog.write_checkpoint(&s2).unwrap();

        let mut tables = catalog.list_tables().unwrap();
        tables.sort();
        assert_eq!(tables, vec!["table_a", "table_b"]);
    }

    #[test]
    fn test_read_nonexistent_table() {
        let wh = test_warehouse("nonexistent");
        let catalog = FileSystemCatalog::new(&wh).unwrap();
        let err = catalog.read_latest("no_such_table").unwrap_err();
        assert_eq!(err.kind_, ErrorKind::FileReadError);
    }
}
