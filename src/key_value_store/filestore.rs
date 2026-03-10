use super::errors;
use super::key_value_store::KeyValueStore;

use std::fs::{self, File};
use std::io::prelude::*;

use crate::{
    key_value_store::errors::{ErrorKind, RWError},
    proto::KeyValueStoreMsg,
};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use log::trace;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use prost::Message;
use std::sync::Arc;

pub fn write_to_file(store: KeyValueStore, target_file: &str) -> Result<(), errors::RWError> {
    let msg = store.data();
    // Encoding to a Vec instead of a raw slice allows for custom files sizes
    // not bound by the buffer size. Does this have a performance cost though?
    let bytes = msg.encode_to_vec();
    let mut file;
    match File::create(target_file) {
        Ok(f) => {
            file = f;
        }
        Err(e) => {
            return Err(RWError {
                kind_: ErrorKind::FileOpenError,
                context_: e.to_string(),
            })
        }
    };
    match file.write_all(&bytes) {
        Ok(_) => {
            trace!("Backup length: {:?}", bytes.len());
        }
        Err(e) => {
            return Err(RWError {
                kind_: ErrorKind::FileWriteError,
                context_: e.to_string(),
            })
        }
    };
    Ok(())
}

pub fn write_to_parquet_file(
    store: KeyValueStore,
    target_file: &str,
) -> Result<(), errors::RWError> {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut timestamps = Vec::new();

    for (k, v) in store.data().values {
        keys.push(k);
        values.push(Some(v.value));
        timestamps.push(v.timestamp);
    }

    let key_array = StringArray::from(keys);
    let value_array = StringArray::from(values);
    let timestamp_array = Int64Array::from(timestamps);

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(key_array),
            Arc::new(value_array),
            Arc::new(timestamp_array),
        ],
    );

    let batch = match batch {
        Ok(b) => b,
        Err(e) => {
            return Err(RWError {
                kind_: ErrorKind::FileWriteError,
                context_: format!("Arrow formatting error: {}", e),
            });
        }
    };

    let file = match File::create(target_file) {
        Ok(f) => f,
        Err(e) => {
            return Err(RWError {
                kind_: ErrorKind::FileOpenError,
                context_: e.to_string(),
            });
        }
    };

    let props = WriterProperties::builder().build();
    let mut writer = match ArrowWriter::try_new(file, schema, Some(props)) {
        Ok(w) => w,
        Err(e) => {
            return Err(RWError {
                kind_: ErrorKind::FileWriteError,
                context_: format!("Parquet writer init error: {}", e),
            });
        }
    };

    if let Err(e) = writer.write(&batch) {
        return Err(RWError {
            kind_: ErrorKind::FileWriteError,
            context_: format!("Parquet batch write error: {}", e),
        });
    }

    if let Err(e) = writer.close() {
        return Err(RWError {
            kind_: ErrorKind::FileWriteError,
            context_: format!("Parquet file close error: {}", e),
        });
    }

    Ok(())
}

pub fn read_from_file(src_file: &str) -> Result<KeyValueStore, errors::RWError> {
    let buf = match fs::read(src_file) {
        Ok(b) => {
            if b.is_empty() {
                return Err(RWError {
                    kind_: ErrorKind::FileReadError,
                    context_: String::from("Empty file!"),
                });
            }
            b
        }
        Err(e) => {
            return Err(RWError {
                kind_: ErrorKind::FileOpenError,
                context_: e.to_string(),
            });
        }
    };
    match KeyValueStoreMsg::decode(buf.as_slice()) {
        Ok(msg) => Ok(KeyValueStore::from(msg)),
        Err(e) => Err(RWError {
            kind_: ErrorKind::DataDecodeError,
            context_: e.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    /// Test cases added:
    /// 1. (De)serialize between rust objet and protobuf
    /// 2. File I/O
    /// 3. Attempt reading from invalid file, check error
    /// 4. Attempt reading bad data, check error
    ///    Remaining:
    /// 5. Permissions check
    use super::*;
    use crate::key_value_store::key_value_pair::KeyValuePair;

    fn create_simple_kv_store() -> KeyValueStore {
        let mut kvs = KeyValueStore::new("test");
        kvs.add(KeyValuePair::new("Hello", "Value1", 100));
        kvs.add(KeyValuePair::new("Goodbye", "Value2", 200));
        kvs
    }

    fn equality_test(lhs: KeyValueStore, rhs: KeyValueStore) {
        assert_eq!(lhs.name(), rhs.name(), "Name Mismatch!");
        for (k, v) in lhs.all() {
            if let Some(val) = rhs.get(&k.clone()) {
                assert_eq!(v, val.value(), "Mismatch for key: {:?}", k);
            } else {
                panic!("No value found for key {:?}!", k);
            }
        }
    }

    #[test]
    fn test_obj_to_msg_to_obj() {
        let kvs = create_simple_kv_store();
        let msg = kvs.clone().data();
        let kvs_2 = KeyValueStore::from(msg);

        equality_test(kvs, kvs_2);
    }

    #[test]
    fn test_file_io() {
        let kvs = create_simple_kv_store();
        let file_name = "/tmp/test.buf";
        match write_to_file(kvs.clone(), file_name) {
            Ok(_) => {}
            Err(e) => {
                panic!("Write error: {:?}!", e);
            }
        }
        let kvs2 = match read_from_file(file_name) {
            Ok(k) => k,
            Err(e) => {
                panic!("Read error: {:?}!", e);
            }
        };
        equality_test(kvs, kvs2);
    }

    #[test]
    fn test_file_io_parquet() {
        let kvs = create_simple_kv_store();
        let file_name = "/tmp/test.parquet";
        match write_to_parquet_file(kvs.clone(), file_name) {
            Ok(_) => {}
            Err(e) => {
                panic!("Write parquet error: {:?}!", e);
            }
        }

        let metadata = std::fs::metadata(file_name).unwrap();
        assert!(metadata.len() > 0, "Parquet file should not be empty");
    }

    #[test]
    fn test_file_io_read_empty_file() {
        let file_name = "/dev/null";
        match read_from_file(file_name) {
            Ok(_) => {
                panic!("Expected failure!")
            }
            Err(e) => {
                assert_eq!(e.kind_, ErrorKind::FileReadError);
            }
        }
    }

    #[test]
    fn test_file_io_read_non_existent_file() {
        let file_name = "/tmp/file_does_not_exist";
        match read_from_file(file_name) {
            Ok(_) => {
                panic!("Expected failure!")
            }
            Err(e) => {
                assert_eq!(e.kind_, ErrorKind::FileOpenError);
            }
        }
    }

    #[test]
    fn test_file_io_read_bad_data() {
        let file_name = "/tmp/bad_data_file";
        let garbage_str = "This string is garbage";
        let bytes = garbage_str.as_bytes();
        let mut file;
        match File::create(file_name) {
            Ok(f) => {
                file = f;
            }
            Err(e) => {
                panic!("Test cannot create file! {:?}", e);
            }
        };
        match file.write_all(bytes) {
            Ok(_) => {
                trace!("bytes: {:?}, len: {:?}", bytes, bytes.len());
            }
            Err(e) => {
                panic!("Test cannot write to file! {:?}", e);
            }
        }
        match read_from_file(file_name) {
            Ok(_) => {
                panic!("Expected failure!");
            }
            Err(e) => {
                assert_eq!(e.kind_, ErrorKind::DataDecodeError);
            }
        }
    }
}
