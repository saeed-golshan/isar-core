use crate::error::{IsarError, Result};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::convert::TryInto;

const NULL_INT: i64 = i64::MIN;
const NULL_BOOL: u8 = 0;
const NULL_LENGTH: u32 = u32::MAX;

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub struct Field {
    pub data_type: DataType,
    pub offset: usize,
}

impl Field {
    pub fn new(data_type: DataType, offset: usize) -> Self {
        Field { data_type, offset }
    }

    #[inline]
    pub fn is_null(&self, data: &[u8]) -> bool {
        match self.data_type {
            DataType::Int => self.get_int(data) == NULL_INT,
            DataType::Double => self.get_double(data).is_nan(),
            DataType::Bool => data[self.offset as usize] == NULL_BOOL,
            _ => self.get_data_offset(data) == 0,
        }
    }

    #[inline]
    pub fn get_int(&self, object: &[u8]) -> i64 {
        let offset = self.offset as usize;
        let bytes: [u8; 8] = object[offset..offset + 8].try_into().unwrap();
        i64::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_double(&self, object: &[u8]) -> f64 {
        let offset = self.offset as usize;
        let bytes: [u8; 8] = object[offset..offset + 8].try_into().unwrap();
        f64::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_bool(&self, object: &[u8]) -> bool {
        object[self.offset as usize] == 1
    }

    #[inline]
    pub fn get_data_offset(&self, object: &[u8]) -> usize {
        let offset = self.offset as usize;
        let bytes: [u8; 4] = object[offset..offset + 4].try_into().unwrap();
        u32::from_le_bytes(bytes) as usize
    }

    #[inline]
    pub fn get_length(&self, object: &[u8]) -> usize {
        let offset = self.offset as usize + 4;
        let bytes: [u8; 4] = object[offset..offset + 4].try_into().unwrap();
        u32::from_le_bytes(bytes) as usize
    }

    #[inline]
    pub fn get_bytes<'a>(&self, object: &'a [u8]) -> &'a [u8] {
        let len = self.get_length(object);
        if len == NULL_LENGTH as usize {
            panic!("Cannot read null property.")
        }
        let offset = self.get_data_offset(object);
        &object[offset..offset + len]
    }
}

#[derive(Ord, PartialOrd, PartialEq, Eq, Clone, Copy, Serialize_repr, Deserialize_repr, Debug)]
#[repr(u8)]
pub enum DataType {
    Int = 0,
    Double = 1,
    Bool = 2,
    String = 3,
    Bytes = 4,
    IntList = 5,
    DoubleList = 6,
    BoolList = 7,
    StringList = 8,
}

impl DataType {
    pub fn from_type_id(id: u8) -> Result<Self> {
        let data_type = match id {
            0 => DataType::Int,
            1 => DataType::Double,
            2 => DataType::Bool,
            3 => DataType::String,
            4 => DataType::Bytes,
            5 => DataType::IntList,
            6 => DataType::DoubleList,
            7 => DataType::BoolList,
            8 => DataType::StringList,
            _ => {
                return Err(IsarError::DbCorrupted {
                    source: None,
                    message: format!(
                        "Field data type {} is not a valid type. Database may be corrupted.",
                        id
                    ),
                });
            }
        };
        Ok(data_type)
    }

    pub fn to_type_id(&self) -> u8 {
        *self as u8
    }

    pub fn is_dynamic(&self) -> bool {
        match *self {
            DataType::Int | DataType::Double | DataType::Bool => false,
            _ => true,
        }
    }

    pub fn get_static_size(&self) -> u8 {
        if *self == DataType::Bool {
            1
        } else {
            8
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::field::*;

    #[test]
    fn test_int_field_is_null() {
        let field = Field::new(DataType::Int, 0);
        let null_bytes = i64::to_le_bytes(NULL_INT);
        assert!(field.is_null(&null_bytes));

        let bytes = i64::to_le_bytes(0);
        assert_eq!(field.is_null(&bytes), false);
    }

    #[test]
    fn test_double_field_is_null() {
        let field = Field::new(DataType::Double, 0);
        let null_bytes = f64::to_le_bytes(f64::NAN);
        assert!(field.is_null(&null_bytes));

        let bytes = f64::to_le_bytes(0.0);
        assert_eq!(field.is_null(&bytes), false);
    }

    #[test]
    fn test_bool_field_is_null() {
        let field = Field::new(DataType::Bool, 0);
        let null_bytes = [NULL_BOOL];
        assert!(field.is_null(&null_bytes));

        let bytes = [1];
        assert_eq!(field.is_null(&bytes), false);

        let bytes = [123];
        assert_eq!(field.is_null(&bytes), false);
    }

    #[test]
    fn test_string_field_is_null() {
        let field = Field::new(DataType::String, 0);
        let null_bytes = u32::to_le_bytes(NULL_LENGTH);
        assert!(field.is_null(&null_bytes));

        let bytes = [0, 0, 0, 0];
        assert_eq!(field.is_null(&bytes), false);
    }

    #[test]
    fn test_bytes_field_is_null() {
        let field = Field::new(DataType::Bytes, 0);
        let null_bytes = u32::to_le_bytes(NULL_LENGTH);
        assert!(field.is_null(&null_bytes));

        let bytes = [0, 0, 0, 0];
        assert_eq!(field.is_null(&bytes), false);
    }
}
