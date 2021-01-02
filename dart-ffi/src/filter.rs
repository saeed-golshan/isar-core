use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::query::filter::{And, Filter, IsNull, Or};
use std::slice;

#[no_mangle]
pub unsafe extern "C" fn isar_filter_and_or(
    filter: *mut *const Filter,
    and: bool,
    conditions: *mut *mut Filter,
    length: u32,
) -> u8 {
    let filters = slice::from_raw_parts(conditions, length as usize)
        .iter()
        .map(|f| *Box::from_raw(*f))
        .collect();
    let and_or = if and {
        And::filter(filters)
    } else {
        Or::filter(filters)
    };
    let ptr = Box::into_raw(Box::new(and_or));
    filter.write(ptr);
    0
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_is_null(
    collection: Option<&IsarCollection>,
    filter: *mut *const Filter,
    is_null: bool,
    property_index: u32,
) -> u8 {
    let property = collection
        .unwrap()
        .get_property_by_index(property_index as usize);
    isar_try! {
        if let Some(property) = property {
            let query_filter = IsNull::filter(property, is_null);
            let ptr = Box::into_raw(Box::new(query_filter));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[macro_export]
macro_rules! primitive_filter_ffi {
    ($filter_name:ident, $function_name:ident, $type:ty) => {
        #[no_mangle]
        pub unsafe extern "C" fn $function_name(
            collection: Option<&IsarCollection>,
            filter: *mut *const Filter,
            lower: $type,
            upper: $type,
            property_index: u32,
        ) -> u8 {
            let property = collection
                .unwrap()
                .get_property_by_index(property_index as usize);
            isar_try! {
                if let Some(property) = property {
                    let query_filter = isar_core::query::filter::$filter_name::filter(property, lower, upper)?;
                    let ptr = Box::into_raw(Box::new(query_filter));
                    filter.write(ptr);
                } else {
                    illegal_arg("Property does not exist.")?;
                }
            }
        }
    }
}

primitive_filter_ffi!(ByteBetween, isar_filter_byte, u8);
primitive_filter_ffi!(IntBetween, isar_filter_int, i32);
primitive_filter_ffi!(FloatBetween, isar_filter_float, f32);
primitive_filter_ffi!(LongBetween, isar_filter_long, i64);
primitive_filter_ffi!(DoubleBetween, isar_filter_double, f64);
