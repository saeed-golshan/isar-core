use crate::async_txn::run_async;
use crate::dart::dart_post_int;
use crate::dart::DartPort;
use crate::error::ErrCode;
use crate::from_c_str;
use isar_core::collection::IsarCollection;
use isar_core::error::illegal_arg;
use isar_core::instance::IsarInstance;
use isar_core::schema::Schema;
use once_cell::sync::Lazy;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::os::raw::c_char;
use std::sync::Mutex;

static INSTANCES: Lazy<Mutex<HashMap<String, IsarInstance>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

struct IsarInstanceSend(*mut *const IsarInstance);

unsafe impl Send for IsarInstanceSend {}

#[no_mangle]
pub unsafe extern "C" fn isar_create_instance(
    isar: *mut *const IsarInstance,
    path: *const c_char,
    max_size: i64,
    schema: *mut Schema,
    port: DartPort,
) {
    let isar = IsarInstanceSend(isar);
    let path = from_c_str(path).unwrap().to_string();
    let schema = Box::from_raw(schema);
    run_async(move || {
        let mut lock = INSTANCES.lock().unwrap();
        let instance = match lock.entry(path) {
            Entry::Occupied(e) => Ok(&*e.into_mut()),
            Entry::Vacant(e) => {
                let new_isar = IsarInstance::create(e.key(), max_size as usize, *schema);
                match new_isar {
                    Ok(new_isar) => Ok(&*e.insert(new_isar)),
                    Err(e) => Err(e),
                }
            }
        };

        match instance {
            Ok(instance) => {
                isar.0.write(instance);
                dart_post_int(port, 0);
            }
            Err(e) => {
                dart_post_int(port, e.err_code());
            }
        }
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_get_collection<'a>(
    isar: &'a IsarInstance,
    collection: *mut &'a IsarCollection,
    index: u32,
) -> i32 {
    isar_try! {
        let new_collection = isar.get_collection(index as usize);
        if let Some(new_collection) = new_collection {
            collection.write(new_collection);
        } else {
            illegal_arg("Collection index is invalid.")?;
        }
    }
}
