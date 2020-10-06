use enum_ordinalize::Ordinalize;
use itertools::Itertools;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::convert::TryInto;
use std::{mem, slice};

/*
Binary format:

All numbers are little endian!

-- STATIC DATA --
int1-N: i32
long1-N: i64
float1-N: f32
double1-N: f64
bool1-N: u8

padding: (number of bools % 4) * \0

-- POINTERS --
int_list_offset: u32 (relative to beginning) OR 0 for null list
int_list_length: u32 OR 0 for null list

long_list_offset
long_list_length

float_list_offset
float_list_length

double_list_offset
double_list_length

bool_list_offset
bool_list_length

string_offset: u32 (relative to beginning) OR 0 for null string
string_length: u32 number of BYTES OR 0 for null string

bytes_offset: u32 (relative to beginning) OR 0 for null bytes
bytes_length: u32 number of bytes OR 0 for null bytes

padding: (len(bool_lists) + len(string lists) + len(bytes_lists)) % 4
 */

#[repr(C)]
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
pub struct Property {
    pub name: String,
    pub data_type: DataType,
    pub offset: usize,
}

impl Property {
    pub const NULL_INT: i32 = i32::MIN;
    pub const NULL_LONG: i64 = i64::MIN;
    pub const NULL_FLOAT: f32 = f32::NAN;
    pub const NULL_DOUBLE: f64 = f64::NAN;
    pub const NULL_BOOL: u8 = 0;
    pub const FALSE_BOOL: u8 = 1;
    pub const TRUE_BOOL: u8 = 2;

    pub fn new(name: &str, data_type: DataType, offset: usize) -> Self {
        Property {
            name: name.to_string(),
            data_type,
            offset,
        }
    }

    #[inline]
    pub fn is_null(&self, object: &[u8]) -> bool {
        match self.data_type {
            DataType::Int => self.get_int(object) == Self::NULL_INT,
            DataType::Long => self.get_long(object) == Self::NULL_LONG,
            DataType::Float => self.get_float(object).is_nan(),
            DataType::Double => self.get_double(object).is_nan(),
            DataType::Bool => self.get_bool(object).is_none(),
            _ => self.get_length(object).is_none(),
        }
    }

    #[inline]
    pub fn get_int(&self, object: &[u8]) -> i32 {
        //TODO rely on alignment and transmute
        let bytes: [u8; 4] = object[self.offset..self.offset + 4].try_into().unwrap();
        i32::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_long(&self, object: &[u8]) -> i64 {
        let bytes: [u8; 8] = object[self.offset..self.offset + 8].try_into().unwrap();
        i64::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_float(&self, object: &[u8]) -> f32 {
        //TODO rely on alignment and transmute
        let bytes: [u8; 4] = object[self.offset..self.offset + 4].try_into().unwrap();
        f32::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_double(&self, object: &[u8]) -> f64 {
        let bytes: [u8; 8] = object[self.offset..self.offset + 8].try_into().unwrap();
        f64::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_bool(&self, object: &[u8]) -> Option<bool> {
        match object[self.offset] {
            Self::NULL_BOOL => None,
            Self::TRUE_BOOL => Some(false),
            Self::FALSE_BOOL => Some(true),
            _ => panic!("Unexpected bool value"),
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

    pub fn get_int_list<'a>(&self, object: &'a [u8]) -> Option<&'a [i32]> {
        self.get_list(object, self.offset)
    }

    pub fn get_long_list<'a>(&self, object: &'a [u8]) -> Option<&'a [i64]> {
        self.get_list(object, self.offset)
    }

    pub fn get_float_list<'a>(&self, object: &'a [u8]) -> Option<&'a [f32]> {
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
        let ptr = bytes.as_ptr() as *const u8;
        unsafe { slice::from_raw_parts::<T>(ptr as *const T, bytes.len() / type_size) }
    }
}

#[derive(
    Ord, PartialOrd, PartialEq, Eq, Clone, Copy, Serialize_repr, Deserialize_repr, Debug, Ordinalize,
)]
#[repr(u8)]
pub enum DataType {
    Int = 0,
    Long = 1,
    Float = 2,
    Double = 3,
    Bool = 4,
    String = 5,
    Bytes = 6,
    IntList = 7,
    LongList = 8,
    FloatList = 9,
    DoubleList = 10,
    BoolList = 11,
    StringList = 12,
    BytesList = 13,
}

impl DataType {
    pub fn is_dynamic(&self) -> bool {
        match *self {
            DataType::Int
            | DataType::Long
            | DataType::Float
            | DataType::Double
            | DataType::Bool => false,
            _ => true,
        }
    }

    pub fn get_static_size(&self) -> usize {
        if *self == DataType::Bool {
            1
        } else {
            8
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::object::property::{DataType, Property};

    #[test]
    fn test_int_property_is_null() {
        let property = Property::new("", DataType::Int, 0);
        let null_bytes = i32::to_le_bytes(Property::NULL_INT);
        assert!(property.is_null(&null_bytes));

        let bytes = i32::to_le_bytes(0);
        assert_eq!(property.is_null(&bytes), false);
    }

    #[test]
    fn test_double_property_is_null() {
        let property = Property::new("", DataType::Double, 0);
        let null_bytes = f64::to_le_bytes(f64::NAN);
        assert!(property.is_null(&null_bytes));

        let bytes = f64::to_le_bytes(0.0);
        assert_eq!(property.is_null(&bytes), false);
    }

    #[test]
    fn test_bool_property_is_null() {
        let property = Property::new("", DataType::Bool, 0);
        let null_bytes = [0];
        assert!(property.is_null(&null_bytes));

        let bytes = [1];
        assert_eq!(property.is_null(&bytes), false);

        let bytes = [123];
        assert_eq!(property.is_null(&bytes), false);
    }

    /*#[test]
    fn test_string_property_is_null() {
        let property = Property::new(DataType::String, 0);
        let null_bytes = u32::to_le_bytes(NULL_LENGTH);
        assert!(property.is_null(&null_bytes));

        let bytes = [0, 0, 0, 0];
        assert_eq!(property.is_null(&bytes), false);
    }

    #[test]
    fn test_bytes_property_is_null() {
        let property = Property::new(DataType::Bytes, 0);
        let null_bytes = u32::to_le_bytes(NULL_LENGTH);
        assert!(property.is_null(&null_bytes));

        let bytes = [0, 0, 0, 0];
        assert_eq!(property.is_null(&bytes), false);
    }*/
}
