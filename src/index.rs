use crate::error::Result;
use crate::field::{DataType, Field};
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::object_id::ObjectId;
use crate::query::key_range::{KeyRange, KeyRangeIterator};
use std::mem::transmute;
use wyhash::wyhash;

const MAX_STRING_INDEX_SIZE: usize = 1500;

#[derive(Clone)]
pub struct Index {
    id: u32,
    fields: Vec<Field>,
    unique: bool,
    hash_value: Option<bool>,
    pub client_index: usize,
    db: Db,
}

impl Index {
    pub(crate) fn new(
        bank_id: u16,
        id: u16,
        fields: Vec<Field>,
        unique: bool,
        hash_value: Option<bool>,
        client_index: usize,
        db: Db,
    ) -> Self {
        let id = (bank_id as u32) << 16 | id as u32;
        Index {
            id,
            fields,
            unique,
            hash_value,
            client_index,
            db,
        }
    }

    pub fn get_id(&self) -> u32 {
        self.id
    }

    pub fn is_unique(&self) -> bool {
        self.unique
    }

    pub fn put(&self, txn: &Txn, oid: &ObjectId, object: &[u8]) -> Result<()> {
        let key = self.create_key(object);
        self.db.put(txn, &key, &oid.to_bytes())
    }

    pub fn delete(&self, txn: &Txn, oid: &ObjectId, object: &[u8]) -> Result<()> {
        let key = self.create_key(object);
        self.db.delete(txn, &key, Some(&oid.to_bytes()))
    }

    pub fn clear(&self, txn: &Txn) -> Result<()> {
        let mut cursor = self.db.cursor(txn)?;
        cursor.delete_key_prefix(&self.get_prefix())
    }

    pub fn iter<'a, 'txn>(
        &self,
        txn: &'txn Txn,
        range: &'a mut KeyRange,
    ) -> Result<KeyRangeIterator<'a, 'txn>> {
        let cursor = self.db.cursor(txn)?;
        range.add_prefix(&self.get_prefix());
        range.iter(cursor)
    }

    fn get_prefix(&self) -> [u8; 4] {
        u32::to_le_bytes(self.id)
    }

    fn create_key(&self, object: &[u8]) -> Vec<u8> {
        let mut bytes = self.get_prefix().to_vec();
        if let Some(hash_value) = self.hash_value {
            let field = self.fields.first().unwrap();
            assert!(field.data_type == DataType::String || field.data_type == DataType::StringList);
            bytes.extend(Self::get_string_value_key(field, object))
        } else {
            let index_iter = self.fields.iter().flat_map(|field| match field.data_type {
                DataType::Int => Self::get_int_key(field, object),
                DataType::Double => Self::get_double_key(field, object),
                DataType::Bool => Self::get_bool_key(field, object),
                DataType::String => Self::get_string_hash_key(field, object),
                _ => unreachable!(),
            });
            bytes.extend(index_iter);
        }
        bytes
    }

    #[inline]
    fn get_int_key(field: &Field, object: &[u8]) -> Vec<u8> {
        let value = field.get_int(object);
        let unsigned = unsafe { transmute::<i64, u64>(value) };
        u64::to_be_bytes(unsigned ^ 1 << 63).to_vec()
    }

    #[inline]
    fn get_double_key(field: &Field, object: &[u8]) -> Vec<u8> {
        let value = field.get_double(object);
        let mut bits = unsafe { std::mem::transmute::<f64, u64>(value) };
        if value == 0.0 {
            bits = 0;
        }
        if value.is_sign_positive() {
            bits ^= 0x8000000000000000;
        } else if value.is_sign_negative() {
            bits ^= 0xFFFFFFFFFFFFFFFF;
        }
        u64::to_be_bytes(bits).to_vec()
    }

    #[inline]
    fn get_bool_key(field: &Field, object: &[u8]) -> Vec<u8> {
        if field.get_bool(object) {
            vec![1]
        } else {
            vec![0]
        }
    }

    #[inline]
    fn get_string_hash_key(field: &Field, object: &[u8]) -> Vec<u8> {
        let bytes = field.get_bytes(object);
        let hash = wyhash(bytes, 0);
        u64::to_be_bytes(hash).to_vec()
    }

    #[inline]
    fn get_string_value_key(field: &Field, object: &[u8]) -> Vec<u8> {
        let string_bytes = field.get_bytes(object);
        if string_bytes.len() >= MAX_STRING_INDEX_SIZE {
            let mut bytes = (&string_bytes[0..MAX_STRING_INDEX_SIZE]).to_vec();
            let hash = wyhash(&bytes, 0);
            let hash_bytes = u64::to_le_bytes(hash);
            bytes.extend_from_slice(&hash_bytes);
            bytes
        } else {
            string_bytes.to_vec()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field::{DataType, Field};

    #[test]
    fn test_get_int_key() {
        let field = Field::new("test".to_string(), DataType::Int, 0);

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
            let obj = i64::to_le_bytes(val).to_vec();
            assert_eq!(Index::get_int_key(&field, &obj), bytes);
        }
    }

    #[test]
    fn test_get_double_key() {}

    #[test]
    fn test_get_bool_index_key() {
        let field = Field::new("test".to_string(), DataType::Bool, 0);

        let pairs = vec![
            (vec![0], vec![0]),
            (vec![1], vec![1]),
            (vec![2], vec![0]),
            (vec![123], vec![0]),
        ];
        for (obj, bytes) in pairs {
            assert_eq!(Index::get_bool_key(&field, &obj), bytes);
        }
    }

    #[test]
    fn test_get_string_hash_key() {
        let field = Field::new("test".to_string(), DataType::String, 0);

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
            let mut str_bytes = u32::to_le_bytes(str.len() as u32).to_vec();
            str_bytes.extend_from_slice(&u32::to_le_bytes(4));
            str_bytes.extend_from_slice(str.as_bytes());
            assert_eq!(hash, Index::get_string_hash_key(&field, &str_bytes));
        }
    }

    #[test]
    fn test_get_string_value_key() {
        let field = Field::new("test".to_string(), DataType::String, 0);

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
            let mut str_bytes = u32::to_le_bytes(str.len() as u32).to_vec();
            str_bytes.extend_from_slice(&u32::to_le_bytes(4));
            str_bytes.extend_from_slice(str.as_bytes());
            assert_eq!(hash, Index::get_string_hash_key(&field, &str_bytes));
        }
    }
}
