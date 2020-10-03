use crate::collection::IsarCollection;
use crate::query::where_clause::WhereClause;
use crate::utils::from_c_str;
use std::os::raw::c_char;

#[no_mangle]
pub extern "C" fn isar_wc_create(
    collection: Option<&IsarCollection>,
    index: u32,
) -> *mut WhereClause {
    let where_clause = collection.unwrap().create_where_clause(index as usize);
    Box::into_raw(Box::new(where_clause))
}

#[no_mangle]
pub extern "C" fn isar_wc_add_int(
    where_clause: Option<&mut WhereClause>,
    lower: bool,
    value: i64,
    include: bool,
) {
    if lower {
        where_clause.unwrap().add_lower_int(value, include);
    } else {
        where_clause.unwrap().add_upper_int(value, include);
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_double(
    where_clause: Option<&mut WhereClause>,
    lower: bool,
    value: f64,
    include: bool,
) {
    if lower {
        where_clause.unwrap().add_lower_double(value, include);
    } else {
        where_clause.unwrap().add_upper_double(value, include);
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_bool(
    where_clause: Option<&mut WhereClause>,
    lower: bool,
    value: bool,
) {
    if lower {
        where_clause.unwrap().add_lower_bool(value);
    } else {
        where_clause.unwrap().add_upper_bool(value);
    }
}

#[no_mangle]
pub extern "C" fn isar_wc_add_string_hash(
    where_clause: Option<&mut WhereClause>,
    value: *const c_char,
) {
    let str = from_c_str(value).unwrap();
    where_clause.unwrap().add_string_hash(str);
}

#[no_mangle]
pub extern "C" fn isar_wc_add_string_value(
    where_clause: Option<&mut WhereClause>,
    lower: bool,
    value: *const c_char,
    include: bool,
) {
    let str = from_c_str(value).unwrap();
    if lower {
        where_clause.unwrap().add_lower_string_value(str, include);
    } else {
        where_clause.unwrap().add_upper_string_value(str, include);
    }
}
