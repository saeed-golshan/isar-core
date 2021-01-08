use crate::{from_c_str, isar_try};
use core::slice;
use isar_core::object::data_type::DataType;
use isar_core::schema::collection_schema::CollectionSchema;
use isar_core::schema::Schema;
use std::ffi::CStr;
use std::os::raw::c_char;

#[no_mangle]
pub extern "C" fn isar_schema_create() -> *mut Schema {
    Box::into_raw(Box::new(Schema::new()))
}

#[no_mangle]
pub unsafe extern "C" fn isar_schema_create_collection(
    collection_schema: *mut *const CollectionSchema,
    name: *const c_char,
) -> i32 {
    isar_try! {
        let name_str = from_c_str(name)?;
        let col = CollectionSchema::new(name_str);
        let col_ptr = Box::into_raw(Box::new(col));
        collection_schema.write(col_ptr);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_schema_add_collection(
    schema: &mut Schema,
    collection_schema: *mut CollectionSchema,
) -> i32 {
    isar_try! {
        let collection_schema = Box::from_raw(collection_schema);
        schema.add_collection(*collection_schema)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_schema_add_property(
    collection_schema: &mut CollectionSchema,
    name: *const c_char,
    data_type: u8,
) -> i32 {
    let data_type = DataType::from_ordinal(data_type).unwrap(); // TODO throw error
    isar_try! {
        let name_str = from_c_str(name)?;
        collection_schema.add_property(&name_str, data_type)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_schema_add_index(
    collection_schema: &mut CollectionSchema,
    property_names: *const *const c_char,
    property_names_length: u32,
    unique: bool,
    hash_value: bool,
) -> i32 {
    let property_names_slice =
        slice::from_raw_parts(property_names, property_names_length as usize);
    let property_names: Vec<&str> = property_names_slice
        .iter()
        .map(|&p| CStr::from_ptr(p))
        .map(|cs| cs.to_bytes())
        .map(|bs| std::str::from_utf8(bs).unwrap())
        .collect();
    isar_try! {
        collection_schema.add_index(&property_names, unique,hash_value)?;
    }
}
