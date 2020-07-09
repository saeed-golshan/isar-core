use crate::bank::IsarBank;
use crate::query::where_clause::WhereClause;
use crate::utils::from_c_str;
use std::os::raw::c_char;

#[no_mangle]
pub extern "C" fn isar_create_where_clause(
    bank: Option<&IsarBank>,
    index: u32,
    lower_key_size: u32,
    upper_key_size: u32,
) -> *mut WhereClause {
    let where_clause = bank.unwrap().new_where_clause(
        index as usize,
        lower_key_size as usize,
        upper_key_size as usize,
    );
    Box::into_raw(Box::new(where_clause))
}

#[no_mangle]
pub extern "C" fn isar_where_clause_add_int(
    where_clause: Option<&mut WhereClause>,
    lower: bool,
    value: i64,
) {
    where_clause.unwrap().add_int(lower, value);
}

#[no_mangle]
pub extern "C" fn isar_where_clause_add_double(
    where_clause: Option<&mut WhereClause>,
    lower: bool,
    value: f64,
) {
    where_clause.unwrap().add_double(lower, value);
}

#[no_mangle]
pub extern "C" fn isar_where_clause_add_bool(
    where_clause: Option<&mut WhereClause>,
    lower: bool,
    value: bool,
) {
    where_clause.unwrap().add_bool(lower, value);
}

#[no_mangle]
pub extern "C" fn isar_where_clause_add_string_hash(
    where_clause: Option<&mut WhereClause>,
    lower: bool,
    value: *const c_char,
) {
    let str = from_c_str(value).unwrap();
    where_clause.unwrap().add_string_hash(lower, str);
}

#[no_mangle]
pub extern "C" fn isar_where_clause_add_string_value(
    where_clause: Option<&mut WhereClause>,
    lower: bool,
    value: *const c_char,
) {
    let str = from_c_str(value).unwrap();
    where_clause.unwrap().add_string_value(lower, str);
}
