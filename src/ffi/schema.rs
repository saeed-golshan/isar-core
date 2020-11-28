use crate::object::data_type::DataType;
use crate::schema::collection_schema::CollectionSchema;
use crate::schema::Schema;
use crate::utils::from_c_str;
use std::os::raw::c_char;

#[no_mangle]
pub extern "C" fn isar_schema_create() -> *mut Schema {
    Box::into_raw(Box::new(Schema::new()))
}

#[no_mangle]
pub unsafe extern "C" fn isar_schema_add_collection(
    schema: Option<&mut Schema>,
    collection: *mut CollectionSchema,
) -> u8 {
    isar_try! {
        let collection = Box::from_raw(collection);
        schema.unwrap().add_collection(*collection)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_schema_collection_create(
    collection: *mut *const CollectionSchema,
    name: *const c_char,
) -> u8 {
    isar_try! {
        let name_str = from_c_str(name)?;
        let col = CollectionSchema::new(name_str);
        let col_ptr = Box::into_raw(Box::new(col));
        collection.write(col_ptr);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_schema_collection_add_property(
    collection: Option<&mut CollectionSchema>,
    name: *const c_char,
    data_type: u8,
) -> u8 {
    let data_type = DataType::from_ordinal(data_type).unwrap(); // TODO throw error
    isar_try! {
        let name_str = from_c_str(name)?;
        collection.unwrap().add_property(&name_str, data_type)?;
    }
}

/*#[no_mangle]
pub extern "C" fn isar_schema_collection_add_index(
    collection: Option<&mut CollectionSchema>,
    property_names: *const c_char,
    unique: bool,
    hash_value: bool,
) -> u8 {
    isar_try! {
        let name_str = from_c_str(name)?;
        let data_type = DataType::from_type_id(data_type)?;
        collection.unwrap().add_property(&name_str, data_type);
    }
}*/
