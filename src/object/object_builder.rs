use crate::error::{illegal_state, Result};
use crate::object::object_info::ObjectInfo;
use crate::object::property::{DataType, Property};
use core::mem;
use itertools::Itertools;
use std::slice::from_raw_parts;

struct ObjectBuilder<'a> {
    object: Vec<u8>,
    object_info: &'a ObjectInfo,
    property_index: usize,
    dynamic_offset: usize,
}

impl<'a> ObjectBuilder<'a> {
    pub fn new(object_info: &ObjectInfo) -> ObjectBuilder {
        let last_property = object_info.properties.last().unwrap();
        ObjectBuilder {
            object: vec![],
            object_info,
            property_index: 0,
            dynamic_offset: last_property.offset + last_property.data_type.get_static_size(),
        }
    }

    fn get_next_property(&mut self) -> (usize, DataType) {
        self.property_index += 1;
        let property = self
            .object_info
            .properties
            .get(self.property_index)
            .unwrap();
        (property.offset, property.data_type)
    }

    fn write_at(&mut self, offset: usize, bytes: &[u8]) {
        if offset + bytes.len() > self.object.len() {
            let required = offset + bytes.len();
            self.object.resize(required, 0);
        }
        self.object
            .splice(offset..bytes.len(), bytes.iter().cloned());
    }

    pub fn write_int(&mut self, value: i64) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::Int);
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

    pub fn write_bytes(&mut self, value: Option<&[u8]>) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::Bytes);
        self.write_list(offset, value);
    }

    pub fn write_int_list(&mut self, value: Option<&[i64]>) {
        let (offset, data_type) = self.get_next_property();
        assert_eq!(data_type, DataType::IntList);
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
            None => Property::NULL_BOOL,
            Some(false) => Property::FALSE_BOOL,
            Some(true) => Property::TRUE_BOOL,
        }
    }
}
