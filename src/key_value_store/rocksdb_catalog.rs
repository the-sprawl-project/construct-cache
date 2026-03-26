use crate::key_value_store::key_value_pair::KeyValuePair;
use crate::key_value_store::key_value_store::KeyValueStore;
use crate::key_value_store::errors::{RWError, ErrorKind};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use rocksdb::{DB, Options};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotInfo {
    pub id: i64,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    pub summary: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum WalOp {
    Create {
        key: String,
        value: String,
        timestamp: i64,
    },
    Update {
        key: String,
        value: String,
        timestamp: i64,
    },
    Delete {
        key: String,
    },
}

pub trait StoreCatalog: Send + Sync {
    fn write_checkpoint(&self, table_name: &str, store: &KeyValueStore) -> Result<String, RWError>;
    fn read_latest(&self, table_name: &str) -> Result<KeyValueStore, RWError>;
    fn read_version(&self, table_name: &str, snapshot_id: i64) -> Result<KeyValueStore, RWError>;
    fn rollback(&self, table_name: &str, snapshot_id: i64) -> Result<(), RWError>;
    fn list_snapshots(&self, table_name: &str) -> Result<Vec<SnapshotInfo>, RWError>;
    fn list_tables(&self) -> Result<Vec<String>, RWError>;
    fn append_to_wal(&self, table_name: &str, op: WalOp) -> Result<usize, RWError>;
    fn clear_wal(&self, table_name: &str) -> Result<(), RWError>;
}

pub struct RocksDBCatalog {
    warehouse_path: PathBuf,
}

impl RocksDBCatalog {
    pub fn new(warehouse_path: &str) -> Result<Self, RWError> {
        let clean_path = warehouse_path.strip_prefix("file://").unwrap_or(warehouse_path);
        let path = PathBuf::from(clean_path);
        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|e| RWError {
                kind_: ErrorKind::FileOpenError,
                context_: format!("Cannot create warehouse dir: {}", e),
            })?;
        }
        Ok(Self {
            warehouse_path: path
        })
    }
    
    fn db_path(&self, table_name: &str) -> PathBuf {
        self.warehouse_path.join(table_name).join("data")
    }
    
    fn checkpoint_path(&self, table_name: &str, snapshot_id: i64) -> PathBuf {
        self.warehouse_path.join(table_name).join("snapshots").join(format!("v{}", snapshot_id))
    }
}

impl StoreCatalog for RocksDBCatalog {
    fn write_checkpoint(&self, table_name: &str, store: &KeyValueStore) -> Result<String, RWError> {
        let db_path = self.db_path(table_name);
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, &db_path).map_err(|e| RWError {
            kind_: ErrorKind::FileOpenError,
            context_: format!("RocksDB open error: {:?}", e),
        })?;
        
        let data = store.data();
        for (k, v) in &data.values {
            let value_json = serde_json::json!({
                "value": v.value,
                "timestamp": v.timestamp
            });
            let serialized = serde_json::to_string(&value_json).unwrap();
            db.put(k, serialized).map_err(|e| RWError {
                kind_: ErrorKind::FileWriteError,
                context_: format!("RocksDB put error: {:?}", e),
            })?;
        }
        
        // ensure parent dir for snapshot exists
        let snaps_dir = self.warehouse_path.join(table_name).join("snapshots");
        std::fs::create_dir_all(&snaps_dir).map_err(|e| RWError {
            kind_: ErrorKind::FileWriteError,
            context_: format!("Cannot create snaps dir: {}", e),
        })?;
        
        let epoch_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
            
        let ckpt_path = self.checkpoint_path(table_name, epoch_ns);
        rocksdb::checkpoint::Checkpoint::new(&db)
            .and_then(|ckpt| ckpt.create_checkpoint(&ckpt_path))
            .map_err(|e| RWError {
                kind_: ErrorKind::FileWriteError,
                context_: format!("RocksDB checkpoint error: {:?}", e),
            })?;
            
        Ok(ckpt_path.to_string_lossy().to_string())
    }

    fn read_latest(&self, table_name: &str) -> Result<KeyValueStore, RWError> {
        let db_path = self.db_path(table_name);
        if !db_path.exists() {
            return Ok(KeyValueStore::new(table_name));
        }
        
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, &db_path).map_err(|e| RWError {
            kind_: ErrorKind::FileOpenError,
            context_: format!("RocksDB open error: {:?}", e),
        })?;
        
        let mut store = KeyValueStore::new(table_name);
        for item in db.iterator(rocksdb::IteratorMode::Start) {
            let (k, v) = item.unwrap();
            let key_str = String::from_utf8(k.into_vec()).unwrap();
            let v_str = String::from_utf8(v.into_vec()).unwrap();
            
            if let Ok(value_json) = serde_json::from_str::<serde_json::Value>(&v_str) {
                let val = value_json["value"].as_str().unwrap_or("");
                let ts = value_json["timestamp"].as_i64().unwrap_or(0);
                store.add(KeyValuePair::new(&key_str, val, ts));
            }
        }
        
        Ok(store)
    }

    fn read_version(&self, table_name: &str, snapshot_id: i64) -> Result<KeyValueStore, RWError> {
        let ckpt_path = self.checkpoint_path(table_name, snapshot_id);
        if !ckpt_path.exists() {
            return Err(RWError {
                kind_: ErrorKind::FileOpenError,
                context_: format!("Snapshot not found"),
            });
        }
        
        let opts = Options::default();
        let db = DB::open_for_read_only(&opts, &ckpt_path, false).map_err(|e| RWError {
            kind_: ErrorKind::FileOpenError,
            context_: format!("RocksDB open error: {:?}", e),
        })?;
        
        let mut store = KeyValueStore::new(table_name);
        for item in db.iterator(rocksdb::IteratorMode::Start) {
            let (k, v) = item.unwrap();
            let key_str = String::from_utf8(k.into_vec()).unwrap();
            let v_str = String::from_utf8(v.into_vec()).unwrap();
            
            if let Ok(value_json) = serde_json::from_str::<serde_json::Value>(&v_str) {
                let val = value_json["value"].as_str().unwrap_or("");
                let ts = value_json["timestamp"].as_i64().unwrap_or(0);
                store.add(KeyValuePair::new(&key_str, val, ts));
            }
        }
        
        Ok(store)
    }

    fn rollback(&self, table_name: &str, snapshot_id: i64) -> Result<(), RWError> {
        let ckpt_path = self.checkpoint_path(table_name, snapshot_id);
        if !ckpt_path.exists() {
            return Err(RWError {
                kind_: ErrorKind::FileOpenError,
                context_: format!("Snapshot not found"),
            });
        }
        let db_path = self.db_path(table_name);
        let _ = std::fs::remove_dir_all(&db_path);
        
        let command = format!("cp -r {} {}", ckpt_path.to_string_lossy(), db_path.to_string_lossy());
        std::process::Command::new("sh").arg("-c").arg(&command).output().unwrap();
        Ok(())
    }

    fn list_snapshots(&self, table_name: &str) -> Result<Vec<SnapshotInfo>, RWError> {
        let snaps_dir = self.warehouse_path.join(table_name).join("snapshots");
        let mut snaps = vec![];
        if !snaps_dir.exists() {
            return Ok(snaps);
        }
        for entry in std::fs::read_dir(snaps_dir).unwrap() {
            let entry = entry.unwrap();
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with('v') {
                    if let Ok(id) = name[1..].parse::<i64>() {
                        snaps.push(SnapshotInfo {
                            id,
                            timestamp_ms: id / 1_000_000,
                            manifest_list: entry.path().to_string_lossy().to_string(),
                            summary: HashMap::new(),
                        });
                    }
                }
            }
        }
        Ok(snaps)
    }

    fn list_tables(&self) -> Result<Vec<String>, RWError> {
        let mut tables = Vec::new();
        if !self.warehouse_path.exists() {
            return Ok(tables);
        }
        let entries = std::fs::read_dir(&self.warehouse_path).map_err(|e| RWError {
            kind_: ErrorKind::FileReadError,
            context_: e.to_string(),
        })?;

        for entry in entries.filter_map(|e| e.ok()) {
            if entry.path().is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    tables.push(name.to_string());
                }
            }
        }
        Ok(tables)
    }

    fn append_to_wal(&self, _table_name: &str, _op: WalOp) -> Result<usize, RWError> {
        Ok(0)
    }

    fn clear_wal(&self, _table_name: &str) -> Result<(), RWError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_warehouse(name: &str) -> String {
        format!("/tmp/construct_cache_rocksdb_test_{}", name)
    }

    #[test]
    fn test_rocksdb_catalog_init() {
        let wh = test_warehouse("init");
        let _ = fs::remove_dir_all(&wh);
        let catalog = RocksDBCatalog::new(&wh).unwrap();
        assert!(catalog.warehouse_path.exists());
    }

    #[test]
    fn test_rocksdb_catalog_read_write_checkpoint() {
        let wh = test_warehouse("rw");
        let _ = fs::remove_dir_all(&wh);
        let catalog = RocksDBCatalog::new(&wh).unwrap();
        
        let table_name = "test_store";
        let mut store = KeyValueStore::new(table_name);
        
        store.add(KeyValuePair::new("key1", "val1", 100));
        store.add(KeyValuePair::new("key2", "val2", 200));
        
        let ckpt_path = catalog.write_checkpoint(table_name, &store).unwrap();
        assert!(!ckpt_path.is_empty());
        
        let loaded_store = catalog.read_latest(table_name).unwrap();
        assert_eq!(loaded_store.get("key1").unwrap().value(), "val1");
        assert_eq!(loaded_store.get("key2").unwrap().value(), "val2");
    }

    #[test]
    fn test_rocksdb_catalog_list_snapshots_and_time_travel() {
        let wh = test_warehouse("tt");
        let _ = fs::remove_dir_all(&wh);
        let catalog = RocksDBCatalog::new(&wh).unwrap();
        
        let table_name = "test_tt";
        let mut store = KeyValueStore::new(table_name);
        
        store.add(KeyValuePair::new("key1", "val1", 100));
        let _ckpt_path1 = catalog.write_checkpoint(table_name, &store).unwrap();
        
        // Wait briefly to ensure different epoch_ns for snapshot id
        std::thread::sleep(std::time::Duration::from_millis(2));
        
        store.add(KeyValuePair::new("key2", "val2", 200));
        let _ckpt_path2 = catalog.write_checkpoint(table_name, &store).unwrap();
        
        let snaps = catalog.list_snapshots(table_name).unwrap();
        assert_eq!(snaps.len(), 2, "Should have 2 snapshots");

        let mut sorted_snaps = snaps;
        sorted_snaps.sort_by_key(|s| s.id);
        
        let v1_id = sorted_snaps[0].id;
        let v2_id = sorted_snaps[1].id;
        
        let tt_store1 = catalog.read_version(table_name, v1_id).unwrap();
        assert_eq!(tt_store1.get("key1").unwrap().value(), "val1");
        assert!(tt_store1.get("key2").is_none());
        
        let tt_store2 = catalog.read_version(table_name, v2_id).unwrap();
        assert_eq!(tt_store2.get("key1").unwrap().value(), "val1");
        assert_eq!(tt_store2.get("key2").unwrap().value(), "val2");
    }
}
