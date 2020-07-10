use crate::data_dbs::IndexType;
use crate::field::{DataType, Field};
use std::mem::transmute;
use wyhash::wyhash;

pub const MAX_STRING_INDEX_SIZE: usize = 1500;

#[derive(Clone)]
pub struct Index {
    id: u32,
    fields: Vec<Field>,
    index_type: IndexType,
    hash_value: Option<bool>,
}

impl Index {
    pub(crate) fn new(
        bank_id: u16,
        id: u16,
        fields: Vec<Field>,
        index_type: IndexType,
        hash_value: Option<bool>,
    ) -> Self {
        let id = (bank_id as u32) << 16 | id as u32;
        Index {
            id,
            fields,
            index_type,
            hash_value,
        }
    }

    pub fn get_prefix(&self) -> [u8; 4] {
        u32::to_le_bytes(self.id)
    }

    pub fn get_type(&self) -> IndexType {
        IndexType::Secondary
    }

    pub fn create_key(&self, object: &[u8]) -> Vec<u8> {
        let mut bytes = self.get_prefix().to_vec();
        if let Some(true) = self.hash_value {
            let field = self.fields.first().unwrap();
            assert_eq!(field.data_type, DataType::String);
            let value = field.get_bytes(object);
            bytes.extend(Self::get_string_value_key(value))
        } else {
            let index_iter = self.fields.iter().flat_map(|field| match field.data_type {
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

    pub fn get_double_key(value: f64) -> Vec<u8> {
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

    pub fn get_bool_key(value: bool) -> Vec<u8> {
        if value {
            vec![1]
        } else {
            vec![0]
        }
    }

    pub fn get_string_hash_key(value: &[u8]) -> Vec<u8> {
        let hash = wyhash(value, 0);
        u64::to_be_bytes(hash).to_vec()
    }

    pub fn get_string_value_key(value: &[u8]) -> Vec<u8> {
        if value.len() >= MAX_STRING_INDEX_SIZE {
            let mut bytes = (&value[0..MAX_STRING_INDEX_SIZE]).to_vec();
            let hash = wyhash(&bytes, 0);
            let hash_bytes = u64::to_le_bytes(hash);
            bytes.extend_from_slice(&hash_bytes);
            bytes
        } else {
            value.to_vec()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field::{DataType, Field};

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
        assert_eq!(Index::get_bool_key(false), vec![0]);
        assert_eq!(Index::get_bool_key(true), vec![1]);
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
            assert_eq!(hash, Index::get_string_hash_key(str.as_bytes()));
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
            assert_eq!(hash, Index::get_string_hash_key(str.as_bytes()));
        }
    }
}
