use crate::bank::IsarBank;
use crate::error::illegal_arg;
use crate::instance::IsarInstance;
use crate::utils::from_c_str;
use std::os::raw::c_char;

#[no_mangle]
pub unsafe extern "C" fn isar_create_instance(
    isar: *mut *const IsarInstance,
    path: *const c_char,
    max_size: u32,
    schema_json: *const c_char,
) -> u8 {
    isar_try! {
        let path_str = from_c_str(path)?;
        let schemas_str = from_c_str(schema_json)?;
        let new_isar = IsarInstance::create(path_str, max_size, schemas_str)?;
        let isar_ptr = Box::into_raw(Box::new(new_isar));
        isar.write(isar_ptr);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_get_bank<'a>(
    isar: Option<&'a IsarInstance>,
    bank: *mut &'a IsarBank,
    index: u32,
) -> u8 {
    isar_try! {
        let new_bank = isar.unwrap().get_bank(index as usize);
        if let Some(new_bank) = new_bank {
            bank.write(new_bank);
        } else {
            illegal_arg("Provided index is invalid.")?;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_test() -> u8 {
    return 0;
}
