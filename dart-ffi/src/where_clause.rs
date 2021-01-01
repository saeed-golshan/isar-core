use crate::from_c_str;
use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::object::object_id::ObjectId;
use isar_core::query::where_clause::WhereClause;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn isar_wc_create(
    collection: Option<&IsarCollection>,
    wc: *mut *const WhereClause,
    primary: bool,
    index_index: u32,
) -> u8 {
    isar_try! {
        let where_clause = if primary {
            Some(collection.unwrap().create_primary_where_clause())
        } else {
            collection.unwrap().create_secondary_where_clause(index_index as usize)
        };
        if let Some(where_clause) = where_clause {
            let ptr = Box::into_raw(Box::new(where_clause));
            wc.write(ptr);
        } else {
            illegal_arg("Unknown index.")?;
        };
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_oid(
    where_clause: Option<&mut WhereClause>,
    time: u32,
    rand_counter: u64,
) -> u8 {
    let oid = ObjectId::new(0, time, rand_counter);
    where_clause.unwrap().add_oid(oid);
    0
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_lower_oid_time(
    where_clause: Option<&mut WhereClause>,
    time: u32,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_lower_oid_time(time, include)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_lower_byte(
    where_clause: Option<&mut WhereClause>,
    value: u8,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_lower_byte(value,include)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_upper_byte(
    where_clause: Option<&mut WhereClause>,
    value: u8,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_upper_byte(value,include)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_upper_oid(
    where_clause: Option<&mut WhereClause>,
    time: u32,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_upper_oid_time(time, include)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_lower_int(
    where_clause: Option<&mut WhereClause>,
    value: i32,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_lower_int(value, include)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_upper_int(
    where_clause: Option<&mut WhereClause>,
    value: i32,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_upper_int(value, include)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_lower_long(
    where_clause: Option<&mut WhereClause>,
    value: i64,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_lower_long(value, include)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_upper_long(
    where_clause: Option<&mut WhereClause>,
    value: i64,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_upper_long(value, include)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_lower_float(
    where_clause: Option<&mut WhereClause>,
    value: f32,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_lower_float(value, include)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_upper_float(
    where_clause: Option<&mut WhereClause>,
    value: f32,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_upper_float(value, include)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_lower_double(
    where_clause: Option<&mut WhereClause>,
    value: f64,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_lower_double(value, include)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_upper_double(
    where_clause: Option<&mut WhereClause>,
    value: f64,
    include: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_upper_double(value, include)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_string_hash(
    where_clause: Option<&mut WhereClause>,
    value: *const c_char,
) {
    let str = if !value.is_null() {
        Some(from_c_str(value).unwrap())
    } else {
        None
    };
    where_clause.unwrap().add_string_hash(str);
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_lower_string_value(
    where_clause: Option<&mut WhereClause>,
    value: *const c_char,
    include: bool,
) {
    let str = if !value.is_null() {
        Some(from_c_str(value).unwrap())
    } else {
        None
    };
    where_clause.unwrap().add_lower_string_value(str, include);
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_upper_string_value(
    where_clause: Option<&mut WhereClause>,
    value: *const c_char,
    include: bool,
) {
    let str = if !value.is_null() {
        Some(from_c_str(value).unwrap())
    } else {
        None
    };
    where_clause.unwrap().add_upper_string_value(str, include);
}
