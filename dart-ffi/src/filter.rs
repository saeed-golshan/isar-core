use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::query::filter::{Filter, IntBetween, LongBetween};

#[no_mangle]
pub unsafe extern "C" fn isar_filter_int_between(
    collection: Option<&IsarCollection>,
    filter: *mut *const Filter,
    lower: i32,
    upper: i32,
    property_index: u32,
) -> u8 {
    let property = collection
        .unwrap()
        .get_property_by_index(property_index as usize);
    isar_try! {
        if let Some(property) = property {
            let int_between = IntBetween::filter(property, lower, upper)?;
            let ptr = Box::into_raw(Box::new(int_between));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_filter_long_between(
    collection: Option<&IsarCollection>,
    filter: *mut *const Filter,
    lower: i64,
    upper: i64,
    property_index: u32,
) -> u8 {
    let property = collection
        .unwrap()
        .get_property_by_index(property_index as usize);
    isar_try! {
        if let Some(property) = property {
            let int_between = LongBetween::filter(property, lower, upper)?;
            let ptr = Box::into_raw(Box::new(int_between));
            filter.write(ptr);
        } else {
            illegal_arg("Property does not exist.")?;
        }
    }
}
