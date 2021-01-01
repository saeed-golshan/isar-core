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
pub unsafe extern "C" fn isar_wc_add_oid_time(
    where_clause: Option<&mut WhereClause>,
    lower: u32,
    upper: u32,
) {
    where_clause.unwrap().add_oid_time(lower, upper);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_byte(where_clause: Option<&mut WhereClause>, lower: u8, upper: u8) {
    where_clause.unwrap().add_byte(lower, upper);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_int(where_clause: Option<&mut WhereClause>, lower: i32, upper: i32) {
    where_clause.unwrap().add_int(lower, upper);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_float(
    where_clause: Option<&mut WhereClause>,
    lower: f32,
    include_lower: bool,
    upper: f32,
    include_upper: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_float(lower, include_lower,upper,include_upper)?;
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_long(where_clause: Option<&mut WhereClause>, lower: i64, upper: i64) {
    where_clause.unwrap().add_long(lower, upper);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_double(
    where_clause: Option<&mut WhereClause>,
    lower: f64,
    include_lower: bool,
    upper: f64,
    include_upper: bool,
) -> u8 {
    isar_try! {
        where_clause.unwrap().add_double(lower, include_lower,upper,include_upper)?;
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
    lower: *const c_char,
    upper: *const c_char,
    include: bool,
) {
    let lower_str = if !lower.is_null() {
        Some(from_c_str(lower).unwrap())
    } else {
        None
    };
    let upper_str = if !upper.is_null() {
        Some(from_c_str(upper).unwrap())
    } else {
        None
    };
    where_clause.unwrap().add_string_value(lower_str, upper_str);
}
