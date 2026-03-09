use log::error;
use std::str::FromStr;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use crate::key_value_store::key_value_pair::KeyValuePair;
use super::filestore::write_to_file;
use crate::proto::{KeyValueStoreMsg, ValueWithTimestamp};

use prost::Message;
use std::fs::File;
use crate::key_value_store::errors::{ErrorKind, RWError};
use std::io::prelude::*;
use log::trace;

#[derive(Debug, PartialEq, Clone)]
pub struct KeyValueStore {
    data_: KeyValueStoreMsg,
}

impl KeyValueStore {
    pub fn new(name: &str) -> KeyValueStore {
        let mut data = KeyValueStoreMsg::default();
        data.name = String::from_str(name).expect("Cannot accept name");
        KeyValueStore {
            data_: data
        }
    }

    pub fn from(store: KeyValueStoreMsg) -> KeyValueStore {
        KeyValueStore { data_: store }
    }

    pub fn get(&self, key: &str) -> Option<KeyValuePair> {
        let returnable: Option<KeyValuePair>;
        match self.data_.values.get(key) {
            Some(x) => {
                returnable = Some(
                    KeyValuePair::new(key, x.value.as_str(), x.timestamp));
            },
            None => {
                returnable = None;
            }
        }
        return returnable;
    }

    pub fn add(&mut self, pair: KeyValuePair) -> bool {
        if let Entry::Vacant(
            v) = self.data_.values.entry(
            pair.key().to_string()) {
            v.insert(ValueWithTimestamp {
                value: pair.value().to_string(),
                timestamp: pair.timestamp(),
            });
            return true;
        }
        return false;
    }

    pub fn update(&mut self, pair: KeyValuePair) -> bool {
        if let Entry::Vacant(_) = self.data_.values.entry(pair.key().to_string()) {
            return false;
        }
        self.data_.values.insert(
            pair.key().to_string(),
            ValueWithTimestamp {
                value: pair.value().to_string(),
                timestamp: pair.timestamp(),
            }
        );
        return true;
    }

    pub fn delete(&mut self, key: &str) -> bool {
        match self.data_.values.remove(key) {
            Some(_) => true,
            None => false
        }
    }

    pub fn name(&self) -> &str {
        &self.data_.name.as_str()
    }

    pub fn all(&self) -> HashMap<String, String> {
        // This is _such_ a waste of space.
        let mut map = HashMap::new();
        for (k, v) in &self.data_.values {
            map.insert(k.clone(), v.value.clone());
        }
        return map;
    }

    pub fn data(&self) -> KeyValueStoreMsg {
        return self.data_.clone();
    }

    pub fn write_to_file(&self, target_file: &str) -> Result<(), RWError> {
        let msg = self.data();
        // This is the same thing from filestore. We will need to deprecate
        // that.
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
                trace!("Backup length {:?}", bytes.len());
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

    pub fn read_from_file(&mut self, src_file: &str) -> Result<(), RWError> {
        let buf = match std::fs::read(src_file) {
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
            Ok(msg) => {
                self.data_ = msg;
                Ok(())
            }
            Err(e) => Err(RWError {
                kind_: ErrorKind::DataDecodeError,
                context_: e.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crud() {
        let store_name = "test_store";
        let mut store = KeyValueStore::new(store_name);
        let first_key = "one";
        let first_pair = KeyValuePair::new(
            first_key, "uno", 100);
        store.add(first_pair);

        // test read idempotency
        {
            let val = store.get(first_key).expect(
                "Expected value in store!");
            let val_2 = store.get(first_key).expect(
                "Expected value in store!");
            assert_eq!(val.key(), val_2.key());
            assert_eq!(val.value(), val_2.value());
            
            // try reading something that does not exist
            assert_eq!(store.get("nonexistent"), None);
            assert_eq!(store.get("nonexistent"), None);
        }

        // test add - ensure that adding duplicates returns false
        {
            let duplicate_add = store.add(KeyValuePair::new(
                first_key, "uno again", 100
            ));
            assert_eq!(duplicate_add, false);
            let acceptable_add = store.add(KeyValuePair::new(
                "two", "dos", 200
            ));
            assert_eq!(acceptable_add, true);
        }

        // test update
        {
            // with a key that exists
            store.update(KeyValuePair::new(
                first_key, "one_again", 300
            ));
            let new_val = store.get(first_key).expect(
                "Expected value in store!");
            assert_eq!(new_val.value(), "one_again");

            // with a key that does not exist
            store.update(KeyValuePair::new(
                "another_key", "another_value", 400
            ));
            let new_val_2 = store.get("another_key");
            assert_eq!(new_val_2, None, "Unexpected entry!");
        }

        // test delete
        {
            // on a key that exists
            let res = store.delete(first_key);
            assert_eq!(res, true);

            // on a key that does not exist
            let res_2 = store.delete("404");
            assert_eq!(res_2, false);
        }

        assert_eq!(store.name(), store_name);
    }
}
