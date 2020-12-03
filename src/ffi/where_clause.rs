use crate::collection::IsarCollection;
use crate::error::illegal_arg;
use crate::query::where_clause::WhereClause;
use crate::utils::from_c_str;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn isar_wc_create(
    collection: Option<&IsarCollection>,
    wc: *mut *const WhereClause,
    primary: bool,
    index_index: u32,
) -> u8 {
    let index = if primary {
        None
    } else {
        Some(index_index as usize)
    };
    isar_try! {
        let where_clause = collection.unwrap().create_where_clause(index);
        if let Some(where_clause) = where_clause {
            let ptr = Box::into_raw(Box::new(where_clause));
            wc.write(ptr);
        } else {
            illegal_arg("Unknown index.")?;
        };
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_lower_oid(
    where_clause: Option<&mut WhereClause>,
    time: *const u32,
    rand_counter: *const u64,
) {
    let time = if time.is_null() { None } else { Some(*time) };
    let rand_counter = if rand_counter.is_null() {
        None
    } else {
        Some(*rand_counter)
    };
    where_clause.unwrap().add_lower_oid(time, rand_counter);
}

#[no_mangle]
pub unsafe extern "C" fn isar_wc_add_upper_oid(
    where_clause: Option<&mut WhereClause>,
    time: *const u32,
    rand_counter: *const u64,
) {
    let time = if time.is_null() { None } else { Some(*time) };
    let rand_counter = if rand_counter.is_null() {
        None
    } else {
        Some(*rand_counter)
    };
    where_clause.unwrap().add_upper_oid(time, rand_counter);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_lower_int(
    where_clause: Option<&mut WhereClause>,
    value: i32,
    include: bool,
) {
    where_clause.unwrap().add_lower_int(value, include);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_upper_int(
    where_clause: Option<&mut WhereClause>,
    value: i32,
    include: bool,
) {
    where_clause.unwrap().add_upper_int(value, include);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_lower_long(
    where_clause: Option<&mut WhereClause>,
    value: i64,
    include: bool,
) {
    where_clause.unwrap().add_lower_long(value, include);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_upper_long(
    where_clause: Option<&mut WhereClause>,
    value: i64,
    include: bool,
) {
    where_clause.unwrap().add_upper_long(value, include);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_lower_float(
    where_clause: Option<&mut WhereClause>,
    value: f32,
    include: bool,
) {
    where_clause.unwrap().add_lower_float(value, include);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_upper_float(
    where_clause: Option<&mut WhereClause>,
    value: f32,
    include: bool,
) {
    where_clause.unwrap().add_upper_float(value, include);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_lower_double(
    where_clause: Option<&mut WhereClause>,
    value: f64,
    include: bool,
) {
    where_clause.unwrap().add_lower_double(value, include);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_upper_double(
    where_clause: Option<&mut WhereClause>,
    value: f64,
    include: bool,
) {
    where_clause.unwrap().add_upper_double(value, include);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_bool(where_clause: Option<&mut WhereClause>, value: u8) {
    let value = match value {
        0 => None,
        1 => Some(false),
        _ => Some(true),
    };
    where_clause.unwrap().add_bool(value);
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
