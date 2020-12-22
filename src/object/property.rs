use crate::object::data_type::DataType;
use itertools::Itertools;
use std::convert::TryInto;
use std::hash::Hasher;
use std::{mem, slice};

/*
Binary format:

All numbers are little endian!

-- STATIC DATA --
bool1-N: u8

padding: -offset % 4

int1-N: i32
float1-N: f32

padding: -offset % 8

long1-N: i64
double1-N: f64

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

padding: -(len(bool_lists) + len(string lists) + len(bytes_lists)) % 4
 */

#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct DynamicPosition {
    pub offset: u32,
    pub length: u32,
}

impl DynamicPosition {
    pub fn is_null(&self) -> bool {
        self.offset == 0
    }
}

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub struct Property {
    pub data_type: DataType,
    pub offset: usize,
}

impl Property {
    pub const NULL_INT: i32 = i32::MIN;
    pub const NULL_LONG: i64 = i64::MIN;
    pub const NULL_FLOAT: f32 = f32::NAN;
    pub const NULL_DOUBLE: f64 = f64::NAN;
    pub const FALSE_BOOL: u8 = 0;
    pub const TRUE_BOOL: u8 = 1;
    pub const NULL_BOOL: u8 = 2;

    pub fn new(data_type: DataType, offset: usize) -> Self {
        Property { data_type, offset }
    }

    #[inline]
    pub fn is_null(&self, object: &[u8]) -> bool {
        match self.data_type {
            DataType::Int => self.get_int(object) == Self::NULL_INT,
            DataType::Long => self.get_long(object) == Self::NULL_LONG,
            DataType::Float => self.get_float(object).is_nan(),
            DataType::Double => self.get_double(object).is_nan(),
            DataType::Bool => !matches!(
                self.get_bool(object),
                Property::TRUE_BOOL | Property::FALSE_BOOL
            ),
            _ => self.get_length(object).is_none(),
        }
    }

    #[inline]
    pub fn get_int(&self, object: &[u8]) -> i32 {
        assert_eq!(self.data_type, DataType::Int);
        let bytes: [u8; 4] = object[self.offset..self.offset + 4].try_into().unwrap();
        i32::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_long(&self, object: &[u8]) -> i64 {
        assert_eq!(self.data_type, DataType::Long);
        let bytes: [u8; 8] = object[self.offset..self.offset + 8].try_into().unwrap();
        i64::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_float(&self, object: &[u8]) -> f32 {
        assert_eq!(self.data_type, DataType::Float);
        let bytes: [u8; 4] = object[self.offset..self.offset + 4].try_into().unwrap();
        f32::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_double(&self, object: &[u8]) -> f64 {
        assert_eq!(self.data_type, DataType::Double);
        let bytes: [u8; 8] = object[self.offset..self.offset + 8].try_into().unwrap();
        f64::from_le_bytes(bytes)
    }

    #[inline]
    pub fn get_bool(&self, object: &[u8]) -> u8 {
        assert_eq!(self.data_type, DataType::Bool);
        object[self.offset]
    }

    pub(crate) fn get_dynamic_position(&self, object: &[u8]) -> Option<DynamicPosition> {
        let list_offset_bytes: [u8; 4] = object[self.offset..self.offset + 4].try_into().unwrap();
        let list_offset = u32::from_le_bytes(list_offset_bytes);
        if list_offset == 0 {
            return None;
        }
        let list_length_bytes: [u8; 4] = object[(self.offset + 4)..(self.offset + 8)]
            .try_into()
            .unwrap();
        let list_length = u32::from_le_bytes(list_length_bytes);
        Some(DynamicPosition {
            offset: list_offset,
            length: list_length,
        })
    }

    pub(crate) fn get_dynamic_positions<'a>(
        &self,
        object: &'a [u8],
    ) -> Option<&'a [DynamicPosition]> {
        let position = self.get_dynamic_position(object)?;
        Some(self.get_list(object, position))
    }

    #[inline]
    pub fn get_length(&self, object: &[u8]) -> Option<usize> {
        assert!(self.data_type.is_dynamic());
        let pos = self.get_dynamic_position(object)?;
        Some(pos.length as usize)
    }

    #[inline]
    pub fn get_string<'a>(&self, object: &'a [u8]) -> Option<&'a str> {
        assert_eq!(self.data_type, DataType::String);
        let position = self.get_dynamic_position(object)?;
        let bytes = self.get_list(object, position);
        Some(std::str::from_utf8(bytes).unwrap())
    }

    #[inline]
    pub fn get_bytes<'a>(&self, object: &'a [u8]) -> Option<&'a [u8]> {
        assert!(self.data_type == DataType::Bytes || self.data_type == DataType::String);
        let position = self.get_dynamic_position(object)?;
        Some(self.get_list(object, position))
    }

    pub fn get_bool_list<'a>(&self, object: &'a [u8]) -> Option<&'a [u8]> {
        assert_eq!(self.data_type, DataType::BoolList);
        let position = self.get_dynamic_position(object)?;
        Some(self.get_list(object, position))
    }

    pub fn get_int_list<'a>(&self, object: &'a [u8]) -> Option<&'a [i32]> {
        assert_eq!(self.data_type, DataType::IntList);
        let position = self.get_dynamic_position(object)?;
        Some(self.get_list(object, position))
    }

    pub fn get_long_list<'a>(&self, object: &'a [u8]) -> Option<&'a [i64]> {
        assert_eq!(self.data_type, DataType::LongList);
        let position = self.get_dynamic_position(object)?;
        Some(self.get_list(object, position))
    }

    pub fn get_float_list<'a>(&self, object: &'a [u8]) -> Option<&'a [f32]> {
        assert_eq!(self.data_type, DataType::FloatList);
        let position = self.get_dynamic_position(object)?;
        Some(self.get_list(object, position))
    }

    pub fn get_double_list<'a>(&self, object: &'a [u8]) -> Option<&'a [f64]> {
        assert_eq!(self.data_type, DataType::DoubleList);
        let position = self.get_dynamic_position(object)?;
        Some(self.get_list(object, position))
    }

    pub fn get_string_list<'a>(&self, object: &'a [u8]) -> Option<Vec<Option<&'a str>>> {
        assert_eq!(self.data_type, DataType::StringList);
        let positions = self.get_dynamic_positions(object)?;
        let string_list = positions
            .iter()
            .map(|position| {
                if position.is_null() {
                    None
                } else {
                    let bytes = self.get_list(object, *position);
                    Some(std::str::from_utf8(bytes).unwrap())
                }
            })
            .collect_vec();
        Some(string_list)
    }

    pub fn get_bytes_list<'a>(&self, object: &'a [u8]) -> Option<Vec<Option<&'a [u8]>>> {
        assert_eq!(self.data_type, DataType::BytesList);
        let positions = self.get_dynamic_positions(object)?;
        let bytes_list = positions
            .iter()
            .map(|position| {
                if position.is_null() {
                    None
                } else {
                    Some(self.get_list(object, *position))
                }
            })
            .collect_vec();
        Some(bytes_list)
    }

    fn get_list<'a, T>(&self, object: &'a [u8], data_position: DynamicPosition) -> &'a [T] {
        let list_length = data_position.length as usize;
        let list_offset = data_position.offset as usize;

        let type_size = mem::size_of::<T>();
        let len_in_bytes = list_length * type_size;
        let list_bytes = &object[list_offset..list_offset + len_in_bytes];

        let alignment = list_bytes.as_ref().as_ptr() as usize;
        assert_eq!(alignment % type_size, 0, "Wrong alignment.");
        let ptr = list_bytes.as_ptr() as *const u8;
        unsafe { slice::from_raw_parts::<T>(ptr as *const T, list_length) }
    }

    fn get_raw<'a>(&self, object: &'a [u8]) -> &'a [u8] {
        match self.data_type {
            DataType::Bool => &object[self.offset..self.offset],
            DataType::Int | DataType::Float => &object[self.offset..self.offset + 4],
            DataType::Long | DataType::Double => &object[self.offset..self.offset + 8],
            _ => {
                let pos = self.get_dynamic_position(object);
                if let Some(pos) = pos {
                    match self.data_type {
                        DataType::StringList | DataType::BytesList => &[],
                        _ => {
                            let offset = pos.offset as usize;
                            let len_in_bytes =
                                pos.length as usize * self.data_type.get_element_size();
                            &object[offset..offset + len_in_bytes]
                        }
                    }
                } else {
                    &[]
                }
            }
        }
    }

    pub fn hash_value<H: Hasher>(&self, object: &[u8], hasher: &mut H) {
        if self.data_type.is_dynamic() {
            let len = self.get_length(object).map_or(0, |len| (len + 1) as u32);
            hasher.write_u32(len);
            if self.data_type == DataType::StringList || self.data_type == DataType::BytesList {
                if let Some(positions) = self.get_dynamic_positions(object) {
                    for pos in positions {
                        if pos.is_null() {
                            hasher.write_u32(0);
                        } else {
                            hasher.write_u32(pos.length + 1);
                        }
                    }
                }
            }
        }
        hasher.write(self.get_raw(object));
    }
}

#[cfg(test)]
mod tests {
    use crate::object::property::{DataType, Property};
    use crate::utils::debug::align;

    #[test]
    fn test_get_bool() {
        let property = Property::new(DataType::Bool, 0);

        let bytes = [Property::FALSE_BOOL];
        assert_eq!(property.get_bool(&bytes), Property::FALSE_BOOL);

        let bytes = [Property::TRUE_BOOL];
        assert_eq!(property.get_bool(&bytes), Property::TRUE_BOOL);

        let null_bytes = [Property::NULL_BOOL];
        assert_eq!(property.get_bool(&null_bytes), Property::NULL_BOOL);
    }

    #[test]
    fn test_bool_is_null() {
        let property = Property::new(DataType::Bool, 0);

        let null_bytes = [123];
        assert!(property.is_null(&null_bytes));

        let bytes = [0];
        assert!(!property.is_null(&bytes));

        let bytes = [1];
        assert!(!property.is_null(&bytes));
    }

    #[test]
    fn test_int_is_null() {
        let property = Property::new(DataType::Int, 0);

        let null_bytes = i32::to_le_bytes(Property::NULL_INT);
        assert!(property.is_null(&null_bytes));

        let bytes = i32::to_le_bytes(0);
        assert!(!property.is_null(&bytes));
    }

    #[test]
    fn test_get_int() {
        let property = Property::new(DataType::Int, 0);

        let bytes = i32::to_le_bytes(123);
        assert_eq!(property.get_int(&bytes), 123);

        let null_bytes = i32::to_le_bytes(Property::NULL_INT);
        assert_eq!(property.get_int(&null_bytes), Property::NULL_INT);
    }

    #[test]
    fn test_get_float() {
        let property = Property::new(DataType::Float, 0);

        let bytes = f32::to_le_bytes(123.123);
        assert!((property.get_float(&bytes) - 123.123).abs() < std::f32::consts::TAU);

        let null_bytes = f32::to_le_bytes(Property::NULL_FLOAT);
        assert!(property.get_float(&null_bytes).is_nan());
    }

    #[test]
    fn test_float_is_null() {
        let property = Property::new(DataType::Float, 0);

        let null_bytes = f32::to_le_bytes(Property::NULL_FLOAT);
        assert!(property.is_null(&null_bytes));

        let bytes = i32::to_le_bytes(0);
        assert!(!property.is_null(&bytes));
    }

    #[test]
    fn test_get_long() {
        let property = Property::new(DataType::Long, 0);

        let bytes = i64::to_le_bytes(123123123123123123);
        assert_eq!(property.get_long(&bytes), 123123123123123123);

        let null_bytes = i64::to_le_bytes(Property::NULL_LONG);
        assert_eq!(property.get_long(&null_bytes), Property::NULL_LONG);
    }

    #[test]
    fn test_long_is_null() {
        let property = Property::new(DataType::Long, 0);

        let null_bytes = i64::to_le_bytes(Property::NULL_LONG);
        assert!(property.is_null(&null_bytes));

        let bytes = i64::to_le_bytes(0);
        assert!(!property.is_null(&bytes));
    }

    #[test]
    fn test_get_double() {
        let property = Property::new(DataType::Double, 0);

        let bytes = f64::to_le_bytes(123123.123123123);
        assert!((property.get_double(&bytes) - 123123.123123123).abs() < std::f64::consts::TAU);

        let null_bytes = f64::to_le_bytes(Property::NULL_DOUBLE);
        assert!(property.get_double(&null_bytes).is_nan());
    }

    #[test]
    fn test_double_is_null() {
        let property = Property::new(DataType::Double, 0);

        let null_bytes = f64::to_le_bytes(f64::NAN);
        assert!(property.is_null(&null_bytes));

        let bytes = f64::to_le_bytes(0.0);
        assert!(!property.is_null(&bytes));
    }

    #[test]
    fn test_get_string() {
        let property = Property::new(DataType::String, 0);

        let mut bytes = vec![8, 0, 0, 0, 5, 0, 0, 0];
        bytes.extend_from_slice(b"hello");
        assert_eq!(property.get_string(&bytes), Some("hello"));

        let bytes = [8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_string(&bytes), Some(""));

        let bytes = [0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_string(&bytes), None);
    }

    #[test]
    fn test_string_is_null() {
        let property = Property::new(DataType::String, 0);

        let mut bytes = vec![8, 0, 0, 0, 5, 0, 0, 0];
        bytes.extend_from_slice(b"hello");
        assert_eq!(property.is_null(&bytes), false);

        let bytes = [8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.is_null(&bytes), false);

        let bytes = [0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.is_null(&bytes), true);
    }

    #[test]
    fn test_get_bytes() {
        let property = Property::new(DataType::Bytes, 0);

        let mut bytes = vec![8, 0, 0, 0, 5, 0, 0, 0];
        bytes.extend_from_slice(b"hello");
        assert_eq!(property.get_bytes(&bytes), Some(&b"hello"[..]));

        let bytes = [8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_bytes(&bytes), Some(&[][..]));

        let bytes = [0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_bytes(&bytes), None);
    }

    #[test]
    fn test_bytes_is_null() {
        let property = Property::new(DataType::Bytes, 0);

        let mut bytes = vec![8, 0, 0, 0, 5, 0, 0, 0];
        bytes.extend_from_slice(b"hello");
        assert_eq!(property.is_null(&bytes), false);

        let bytes = [8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.is_null(&bytes), false);

        let bytes = [0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.is_null(&bytes), true);
    }

    #[test]
    fn test_get_length() {
        let property = Property::new(DataType::BoolList, 0);

        let bytes = align(&[8, 0, 0, 0, 1, 0, 0, 0]);
        assert_eq!(property.get_length(&bytes), Some(1));

        let bytes = align(&[0, 0, 0, 0, 1, 0, 0, 0]);
        assert_eq!(property.get_length(&bytes), None);
    }

    #[test]
    fn test_list_is_null() {
        let property = Property::new(DataType::BoolList, 0);

        let null_bytes = align(&[0, 0, 0, 0, 0, 0, 0, 0]);
        assert!(property.is_null(&null_bytes));

        let bytes = align(&[8, 0, 0, 0, 1, 0, 0, 0, 1]);
        assert!(!property.is_null(&bytes));

        let bytes = align(&[8, 0, 0, 0, 0, 0, 0, 0]);
        assert!(!property.is_null(&bytes));
    }

    #[test]
    fn test_get_bool_list() {
        let property = Property::new(DataType::BoolList, 0);

        let bytes = align(&[8, 0, 0, 0, 5, 0, 0, 0, 1, 0, 2, 1, 5]);
        assert_eq!(property.get_bool_list(&bytes), Some(&[1, 0, 2, 1, 5][..]));

        let bytes = [8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_bool_list(&bytes), Some(&[][..]));

        let bytes = align(&[0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(property.get_bool_list(&bytes), None);
    }

    #[test]
    fn test_get_int_list() {
        let property = Property::new(DataType::IntList, 0);

        let bytes = align(&[8, 0, 0, 0, 2, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0]);
        assert_eq!(property.get_int_list(&bytes), Some(&[5i32, 6][..]));

        let bytes = [8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_int_list(&bytes), Some(&[][..]));

        let bytes = align(&[0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(property.get_int_list(&bytes), None);
    }

    #[test]
    fn test_get_long_list() {
        let property = Property::new(DataType::LongList, 0);

        let bytes = align(&[
            8, 0, 0, 0, 2, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0,
        ]);
        assert_eq!(property.get_long_list(&bytes), Some(&[5i64, 6][..]));

        let bytes = [8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_long_list(&bytes), Some(&[][..]));

        let bytes = align(&[0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(property.get_long_list(&bytes), None);
    }

    #[test]
    fn test_get_float_list() {
        let property = Property::new(DataType::FloatList, 0);

        let mut bytes = vec![8, 0, 0, 0, 2, 0, 0, 0];
        bytes.extend_from_slice(&10.5f32.to_le_bytes());
        bytes.extend_from_slice(&20.6f32.to_le_bytes());
        let bytes = align(&bytes);
        assert_eq!(property.get_float_list(&bytes), Some(&[10.5f32, 20.6][..]));

        let bytes = [8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_float_list(&bytes), Some(&[][..]));

        let bytes = align(&[0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(property.get_float_list(&bytes), None);
    }

    #[test]
    fn test_get_double_list() {
        let property = Property::new(DataType::DoubleList, 0);

        let mut bytes = vec![8, 0, 0, 0, 2, 0, 0, 0];
        bytes.extend_from_slice(&10.5f64.to_le_bytes());
        bytes.extend_from_slice(&20.6f64.to_le_bytes());
        let bytes = align(&bytes);
        assert_eq!(property.get_double_list(&bytes), Some(&[10.5f64, 20.6][..]));

        let bytes = [8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_double_list(&bytes), Some(&[][..]));

        let bytes = align(&[0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(property.get_double_list(&bytes), None);
    }

    #[test]
    fn test_get_string_list() {
        let property = Property::new(DataType::StringList, 0);

        let mut bytes = vec![
            8, 0, 0, 0, 3, 0, 0, 0, 32, 0, 0, 0, 5, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
        ];
        bytes.extend_from_slice(b"hello");
        assert_eq!(
            property.get_string_list(&bytes),
            Some(vec![Some("hello"), Some(""), None])
        );

        let bytes = [8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_string_list(&bytes), Some(vec![]));

        let bytes = [0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_string_list(&bytes), None);
    }

    #[test]
    fn test_get_bytes_list() {
        let property = Property::new(DataType::BytesList, 0);

        let mut bytes = vec![
            8, 0, 0, 0, 3, 0, 0, 0, 32, 0, 0, 0, 5, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0,
        ];
        bytes.extend_from_slice(b"hello");
        assert_eq!(
            property.get_bytes_list(&bytes),
            Some(vec![Some(&b"hello"[..]), Some(&[][..]), None])
        );

        let bytes = [8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_bytes_list(&bytes), Some(vec![]));

        let bytes = [0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(property.get_bytes_list(&bytes), None);
    }
}
