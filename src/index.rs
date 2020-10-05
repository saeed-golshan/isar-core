use crate::error::Result;
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::object::object_id::ObjectId;
use crate::object::property::{DataType, Property};
use crate::query::where_clause::WhereClause;
use std::mem::transmute;
use wyhash::wyhash;

pub const MAX_STRING_INDEX_SIZE: usize = 1500;

/*

Null values are always considered the "smallest" element.

 */

#[derive(Copy, Clone, PartialEq)]
pub enum IndexType {
    Primary,
    Secondary,
    SecondaryDup,
}

#[derive(Clone)]
pub struct Index {
    prefix: [u8; 2],
    properties: Vec<Property>,
    index_type: IndexType,
    hash_value: bool,
    db: Db,
}

impl Index {
    pub(crate) fn new(
        id: u16,
        properties: Vec<Property>,
        index_type: IndexType,
        hash_value: bool,
        db: Db,
    ) -> Self {
        Index {
            prefix: u16::to_le_bytes(id),
            properties,
            index_type,
            hash_value,
            db,
        }
    }

    pub fn create_for_object(&self, txn: &Txn, oid: ObjectId, object: &[u8]) -> Result<()> {
        let index_key = self.create_key(object);
        let oid_bytes = oid.to_bytes();
        self.db.put(txn, &index_key, oid_bytes)
    }

    pub fn delete_for_object(&self, txn: &Txn, oid: ObjectId, object: &[u8]) -> Result<()> {
        let index_key = self.create_key(object);
        let oid_bytes = oid.to_bytes();
        if self.index_type == IndexType::SecondaryDup {
            self.db.delete(txn, &index_key, Some(oid_bytes))
        } else {
            self.db.delete(txn, &index_key, None)
        }
    }

    pub fn clear(&self, txn: &Txn) -> Result<()> {
        self.db.cursor(txn)?.delete_key_prefix(&self.prefix)
    }

    pub fn create_where_clause(&self) -> WhereClause {
        WhereClause::new(&self.prefix, self.index_type)
    }

    fn create_key(&self, object: &[u8]) -> Vec<u8> {
        let mut bytes = self.prefix.to_vec();
        if self.hash_value {
            let property = self.properties.first().unwrap();
            assert_eq!(property.data_type, DataType::String);
            let value = property.get_bytes(object);
            bytes.extend(Self::get_string_value_key(value))
        } else {
            let index_iter = self
                .properties
                .iter()
                .flat_map(|field| match field.data_type {
                    DataType::Int => {
                        let value = field.get_int(object);
                        Self::get_int_key(value)
                    }
                    DataType::Double => {
                        let value = field.get_double(object);
                        Self::get_double_key(value)
                    }
                    DataType::Bool => {
                        let value = field.get_bool(object);
                        Self::get_bool_key(value)
                    }
                    DataType::String => {
                        let value = field.get_bytes(object);
                        Self::get_string_hash_key(value)
                    }
                    _ => unreachable!(),
                });
            bytes.extend(index_iter);
        }
        bytes
    }

    pub fn get_int_key(value: i64) -> Vec<u8> {
        let unsigned = unsafe { transmute::<i64, u64>(value) };
        u64::to_be_bytes(unsigned ^ 1 << 63).to_vec()
    }

    #[allow(clippy::transmute_float_to_int)]
    pub fn get_double_key(value: f64) -> Vec<u8> {
        if !value.is_nan() {
            let mut bits = unsafe { std::mem::transmute::<f64, u64>(value) };
            if value == 0.0 {
                bits = 0;
            }
            if value.is_sign_positive() {
                bits ^= 0x8000000000000000;
            } else if value.is_sign_negative() {
                bits ^= 0xFFFFFFFFFFFFFFFF;
            }
            u64::to_be_bytes(bits + 1).to_vec()
        } else {
            vec![0]
        }
    }

    pub fn get_bool_key(value: Option<bool>) -> Vec<u8> {
        match value {
            None => vec![0],
            Some(false) => vec![1],
            Some(true) => vec![2],
        }
    }

    pub fn get_string_hash_key(value: Option<&[u8]>) -> Vec<u8> {
        if let Some(value) = value {
            let hash = wyhash(value, 0);
            u64::to_be_bytes(hash).to_vec()
        } else {
            vec![]
        }
    }

    pub fn get_string_value_key(value: Option<&[u8]>) -> Vec<u8> {
        if let Some(value) = value {
            let mut bytes = vec![1];
            if value.len() >= MAX_STRING_INDEX_SIZE {
                bytes.extend_from_slice(&value[0..MAX_STRING_INDEX_SIZE]);
                let hash = wyhash(&bytes, 0);
                let hash_bytes = u64::to_le_bytes(hash);
                bytes.extend_from_slice(&hash_bytes);
            } else {
                bytes.extend_from_slice(value);
            }
            bytes
        } else {
            vec![0]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_key() {}

    #[test]
    fn test_get_int_key() {
        let pairs = vec![
            (i64::MIN, vec![0, 0, 0, 0, 0, 0, 0, 0]),
            (i64::MIN + 1, vec![0, 0, 0, 0, 0, 0, 0, 1]),
            (-1, vec![127, 255, 255, 255, 255, 255, 255, 255]),
            (0, vec![128, 0, 0, 0, 0, 0, 0, 0]),
            (1, vec![128, 0, 0, 0, 0, 0, 0, 1]),
            (i64::MAX - 1, vec![255, 255, 255, 255, 255, 255, 255, 254]),
            (i64::MAX, vec![255, 255, 255, 255, 255, 255, 255, 255]),
        ];
        for (val, bytes) in pairs {
            assert_eq!(Index::get_int_key(val), bytes);
        }
    }

    #[test]
    fn test_get_double_key() {}

    #[test]
    fn test_get_bool_index_key() {
        assert_eq!(Index::get_bool_key(None), vec![0]);
        assert_eq!(Index::get_bool_key(Some(false)), vec![1]);
        assert_eq!(Index::get_bool_key(Some(true)), vec![2]);
    }

    #[test]
    fn test_get_string_hash_key() {
        let long_str = (0..1500).map(|_| "a").collect::<String>();

        let pairs: Vec<(&str, Vec<u8>)> = vec![
            ("hello", vec![196, 78, 229, 110, 148, 114, 106, 255]),
            (
                "this is just a test",
                vec![35, 152, 168, 2, 106, 235, 53, 50],
            ),
            (
                &long_str[..1499],
                vec![241, 58, 121, 152, 47, 193, 215, 217],
            ),
            (&long_str[..], vec![107, 96, 243, 122, 159, 148, 180, 244]),
        ];
        for (str, hash) in pairs {
            assert_eq!(hash, Index::get_string_hash_key(Some(str.as_bytes())));
        }
    }

    #[test]
    fn test_get_string_value_key() {
        let long_str = (0..1500).map(|_| "a").collect::<String>();

        let pairs: Vec<(&str, Vec<u8>)> = vec![
            ("hello", vec![196, 78, 229, 110, 148, 114, 106, 255]),
            (
                "this is just a test",
                vec![35, 152, 168, 2, 106, 235, 53, 50],
            ),
            (
                &long_str[..1499],
                vec![241, 58, 121, 152, 47, 193, 215, 217],
            ),
            (&long_str[..], vec![107, 96, 243, 122, 159, 148, 180, 244]),
        ];
        for (str, hash) in pairs {
            assert_eq!(hash, Index::get_string_hash_key(Some(str.as_bytes())));
        }
    }
}
