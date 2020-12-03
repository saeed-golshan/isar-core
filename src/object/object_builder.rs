use crate::object::data_type::DataType;
use crate::object::object_info::ObjectInfo;
use crate::object::property::Property;
use core::mem;
use itertools::Itertools;
use std::slice::from_raw_parts;

pub struct ObjectBuilder<'a> {
    object: Vec<u8>,
    object_info: &'a ObjectInfo,
    property_index: usize,
    dynamic_offset: usize,
}

impl<'a> ObjectBuilder<'a> {
    pub(crate) fn new(object_info: &ObjectInfo) -> ObjectBuilder {
        ObjectBuilder {
            object: Vec::with_capacity(object_info.static_size),
            object_info,
            property_index: 0,
            dynamic_offset: object_info.static_size,
        }
    }

    fn get_next_property(&mut self) -> (usize, DataType) {
        let property = self
            .object_info
            .properties
            .get(self.property_index)
            .unwrap();
        self.property_index += 1;
        (property.offset, property.data_type)
    }

    fn write_at(&mut self, offset: usize, bytes: &[u8]) {
        if offset + bytes.len() > self.object.len() {
            let required = offset + bytes.len();
            self.object.resize(required, 0);
        }
        self.object[offset..(offset + bytes.len())].clone_from_slice(&bytes[..]);
    }

    pub fn write_int(&mut self, value: i32) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::Int);
        self.write_at(offset, &value.to_le_bytes());
    }

    pub fn write_long(&mut self, value: i64) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::Long);
        self.write_at(offset, &value.to_le_bytes());
    }

    pub fn write_float(&mut self, value: f32) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::Float);
        self.write_at(offset, &value.to_le_bytes());
    }

    pub fn write_double(&mut self, value: f64) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::Double);
        self.write_at(offset, &value.to_le_bytes());
    }

    pub fn write_bool(&mut self, value: Option<bool>) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::Bool);
        self.write_at(offset, &Self::bool_to_byte(value).to_le_bytes());
    }

    pub fn write_string(&mut self, value: Option<&str>) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::String);
        self.write_list(offset, value.map(|s| s.as_bytes()));
    }

    pub fn write_bytes(&mut self, value: Option<&[u8]>) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::Bytes);
        self.write_list(offset, value);
    }

    pub fn write_int_list(&mut self, value: Option<&[i32]>) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::IntList);
        self.write_list(offset, value);
    }

    pub fn write_long_list(&mut self, value: Option<&[i64]>) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::LongList);
        self.write_list(offset, value);
    }

    pub fn write_float_list(&mut self, value: Option<&[f32]>) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::FloatList);
        self.write_list(offset, value);
    }

    pub fn write_double_list(&mut self, value: Option<&[f64]>) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::DoubleList);
        self.write_list(offset, value);
    }

    pub fn write_bool_list(&mut self, value: Option<&[Option<bool>]>) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::BoolList);
        if let Some(value) = value {
            let list = value.iter().map(|b| Self::bool_to_byte(*b)).collect_vec();
            self.write_list(offset, Some(&list));
        } else {
            self.write_list::<u8>(offset, None);
        }
    }

    pub fn write_string_list(&mut self) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::StringList);
        self.write_list::<u8>(offset, None);
    }

    pub fn to_bytes(&self) -> &[u8] {
        &self.object
    }

    fn write_list<T>(&mut self, offset: usize, list: Option<&[T]>) {
        if let Some(list) = list {
            self.write_at(offset, &(self.dynamic_offset as u32).to_le_bytes());
            self.write_at(offset + 4, &(list.len() as u32).to_le_bytes());
            let type_size = mem::size_of::<T>();
            let ptr = list.as_ptr() as *const T;
            let bytes = unsafe { from_raw_parts::<u8>(ptr as *const u8, list.len() * type_size) };
            self.write_at(self.dynamic_offset, bytes);
            self.dynamic_offset += bytes.len();
        } else {
            self.write_at(offset, &0u64.to_le_bytes());
        }
    }

    fn bool_to_byte(value: Option<bool>) -> u8 {
        match value {
            Some(false) => Property::FALSE_BOOL,
            Some(true) => Property::TRUE_BOOL,
            None => Property::NULL_BOOL,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::object_builder::ObjectBuilder;
    use crate::object::object_info::ObjectInfo;
    use crate::object::property::Property;

    macro_rules! builder {
        ($var:ident, $($property:expr), *) => {
            let info = ObjectInfo::new(vec![$($property,)*]);
            let mut $var = ObjectBuilder::new(&info);
        };
    }

    #[test]
    pub fn test_write_int() {
        builder!(b, Property::new(DataType::Int, 0));

        b.write_int(123);
        assert_eq!(b.to_bytes(), 123i64.to_le_bytes())
    }

    #[test]
    #[should_panic]
    pub fn test_write_int_wrong_type() {
        builder!(b, Property::new(DataType::Double, 0));
        b.write_int(123);
    }

    #[test]
    pub fn test_write_double() {
        builder!(b, Property::new(DataType::Double, 0));
        b.write_double(123.0);
        assert_eq!(b.to_bytes(), 123f64.to_le_bytes());

        builder!(b, Property::new(DataType::Double, 0));
        b.write_double(f64::NAN);
        assert_eq!(b.to_bytes(), f64::NAN.to_le_bytes());
    }

    #[test]
    #[should_panic]
    pub fn test_write_double_wrong_type() {
        builder!(b, Property::new(DataType::Bool, 0));
        b.write_double(123.0);
    }

    #[test]
    pub fn test_write_bool() {
        builder!(b, Property::new(DataType::Bool, 0));
        b.write_bool(None);
        assert_eq!(b.to_bytes(), &[Property::NULL_BOOL]);

        builder!(b, Property::new(DataType::Bool, 0));
        b.write_bool(Some(false));
        assert_eq!(b.to_bytes(), &[Property::FALSE_BOOL]);

        builder!(b, Property::new(DataType::Bool, 0));
        b.write_bool(Some(true));
        assert_eq!(b.to_bytes(), &[Property::TRUE_BOOL]);
    }

    #[test]
    #[should_panic]
    pub fn test_write_bool_wrong_type() {
        builder!(b, Property::new(DataType::String, 0));
        b.write_bool(Some(true));
    }

    #[test]
    pub fn test_write_multiple_static_types() {
        /*builder!(
            b,
            Property::new( DataType::Int, 0),
            Property::new( DataType::Int, 8),
            Property::new( DataType::Double, 16),
            Property::new( DataType::Bool, 24),
            Property::new( DataType::Double, 25)
        );

        b.write_int(i64::MAX);
        b.write_long(i64::MIN);
        b.write_double(consts::PI);
        b.write_bool(None);
        b.write_float(consts::E);

        let mut bytes = i64::MAX.to_le_bytes().to_vec();
        bytes.extend_from_slice(&i64::MIN.to_le_bytes());
        bytes.extend_from_slice(&consts::PI.to_le_bytes());
        bytes.push(Property::NULL_BOOL);
        bytes.extend_from_slice(&consts::E.to_le_bytes());

        assert_eq!(b.to_bytes(), bytes);*/
    }
}
