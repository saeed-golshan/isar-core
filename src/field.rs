use crate::error::{IsarError, Result};
use itertools::Itertools;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::convert::TryInto;
use std::{mem, slice};

/*
Binary format:

All numbers are little endian!

-- STATIC DATA --
int1: i64
..
intN

double1: f64
..
doubleN

bool1: u8
..
boolN

padding: (number of bools % 8) * \0

-- POINTERS --
int_list1_offset: u32 (relative to beginning) OR 0 for null list
int_list1_length: u32 OR 0 for null list
..
int_listN_offset
int_listN_length

double_list1_offset
double_list1_length
..
double_listN_offset
double_listN_length

bool_list1_offset
bool_list1_length
..
bool_listN_offset
bool_listN_length

string1_offset: u32 (relative to beginning) OR 0 for null string
string1_length: u32 number of BYTES OR 0 for null string
..
stringN_offset
stringN_length

bytes1_offset: u32 (relative to beginning) OR 0 for null bytes
bytes1_length: u32 number of bytes OR 0 for null bytes
..
bytesN_offset
bytesN_length

 */

struct DataPosition {
    pub offset: u32,
    pub length: u32,
}

impl DataPosition {
    pub fn is_null(&self) -> bool {
        self.offset == 0
    }
}
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub offset: usize,
}

impl Field {
    const NULL_INT: i64 = i64::MIN;

    pub fn new(name: &str, data_type: DataType, offset: usize) -> Self {
        Field {
            name: name.to_string(),
            data_type,
            offset,
        }
    }

    #[inline]
    pub fn is_null(&self, object: &[u8]) -> bool {
        match self.data_type {
            DataType::Int => self.get_int(object) == Self::NULL_INT,
            DataType::Double => self.get_double(object).is_nan(),
            DataType::Bool => self.get_bool(object).is_none(),
            _ => self.get_length(object).is_none(),
        }
    }

    #[inline]
    pub fn get_int(&self, object: &[u8]) -> i64 {
        let bytes: [u8; 8] = object[self.offset..self.offset + 8].try_into().unwrap();
        i64::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_double(&self, object: &[u8]) -> f64 {
        let bytes: [u8; 8] = object[self.offset..self.offset + 8].try_into().unwrap();
        f64::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_bool(&self, object: &[u8]) -> Option<bool> {
        match object[self.offset] {
            0 => None,
            1 => Some(false),
            _ => Some(true),
        }
    }

    #[inline]
    pub fn get_length(&self, object: &[u8]) -> Option<usize> {
        let data_position = self.get_list_position(object, self.offset);
        if !data_position.is_null() {
            Some(data_position.length as usize)
        } else {
            None
        }
    }

    pub fn get_bytes<'a>(&self, object: &'a [u8]) -> Option<&'a [u8]> {
        self.get_list(object, self.offset)
    }

    pub fn get_int_list<'a>(&self, object: &'a [u8]) -> Option<&'a [i64]> {
        self.get_list(object, self.offset)
    }

    pub fn get_double_list<'a>(&self, object: &'a [u8]) -> Option<&'a [f64]> {
        self.get_list(object, self.offset)
    }

    /*pub fn get_bytes_list_positions<'a>(&self, object: &'a [u8]) -> Option<&'a [DataPosition]> {
        self.get_list(object, self.offset)
    }*/

    pub fn get_bytes_list<'a>(&self, object: &'a [u8]) -> Option<Vec<Option<&'a [u8]>>> {
        let positions_offset = self.get_list_position(object, self.offset);
        if positions_offset.is_null() {
            return None;
        }
        let lists = (0..positions_offset.length)
            .map(|i| {
                let list_offset = positions_offset.offset + i;
                self.get_list(object, list_offset as usize)
            })
            .collect_vec();
        Some(lists)
    }

    #[inline]
    fn get_list_position<'a>(&self, object: &'a [u8], offset: usize) -> &'a DataPosition {
        let bytes = &object[offset..offset + 8];
        &Self::transmute_verify_alignment::<DataPosition>(bytes)[0]
    }

    fn get_list<'a, T>(&self, object: &'a [u8], offset: usize) -> Option<&'a [T]> {
        let data_position = self.get_list_position(object, offset);
        if data_position.is_null() {
            return None;
        }
        let type_size = mem::size_of::<T>();
        let offset = data_position.offset as usize;
        let len_in_bytes = data_position.length as usize * type_size;
        let list_bytes = &object[offset..offset + len_in_bytes];
        Some(&Self::transmute_verify_alignment::<T>(list_bytes))
    }

    fn transmute_verify_alignment<T>(bytes: &[u8]) -> &[T] {
        let type_size = mem::size_of::<T>();
        let alignment = bytes.as_ref().as_ptr() as usize;
        assert_eq!(alignment % type_size, 0, "Wrong alignment.");
        let ptr = bytes.as_ptr() as *const DataPosition;
        unsafe { slice::from_raw_parts::<T>(ptr as *const T, bytes.len() / type_size) }
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
    BytesList = 9,
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
        let field = Field::new("", DataType::Int, 0);
        let null_bytes = i64::to_le_bytes(Field::NULL_INT);
        assert!(field.is_null(&null_bytes));

        let bytes = i64::to_le_bytes(0);
        assert_eq!(field.is_null(&bytes), false);
    }

    #[test]
    fn test_double_field_is_null() {
        let field = Field::new("", DataType::Double, 0);
        let null_bytes = f64::to_le_bytes(f64::NAN);
        assert!(field.is_null(&null_bytes));

        let bytes = f64::to_le_bytes(0.0);
        assert_eq!(field.is_null(&bytes), false);
    }

    #[test]
    fn test_bool_field_is_null() {
        let field = Field::new("", DataType::Bool, 0);
        let null_bytes = [0];
        assert!(field.is_null(&null_bytes));

        let bytes = [1];
        assert_eq!(field.is_null(&bytes), false);

        let bytes = [123];
        assert_eq!(field.is_null(&bytes), false);
    }

    /*#[test]
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
    }*/
}
