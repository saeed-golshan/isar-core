use crate::raw_object_set::RawObject;
use isar_core::collection::{IsarCollection, PendingPut};
use isar_core::error::illegal_arg;
use isar_core::txn::IsarTxn;

#[no_mangle]
pub unsafe extern "C" fn isar_get(
    collection: Option<&IsarCollection>,
    txn: Option<&IsarTxn>,
    object: &mut RawObject,
) -> u8 {
    isar_try! {
        let collection = collection.unwrap();
        let object_id = object.get_object_id(collection);
        if object_id.is_none() {
            illegal_arg("ObjectId needs to be provided.")?;
        }
        let result = collection.get(txn.unwrap(), object_id.unwrap())?;
        if let Some(result) = result {
            object.set_object(result);
        } else {
            object.set_empty();
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_prepare_put<'a>(
    collection: Option<&'a mut IsarCollection>,
    txn: Option<&mut IsarTxn>,
    object: &mut RawObject,
    pending_put: *mut *const PendingPut<'a>,
) -> u8 {
    isar_try! {
        let collection = collection.unwrap();
        let oid = object.get_object_id(collection);
        let mut pending = collection.prepare_put(txn.unwrap(), oid, object.get_length() as usize)?;
        object.set_object(pending.get_writable_space());
        object.set_object_id(pending.get_oid());
        pending_put.write(Box::into_raw(Box::new(pending)));
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_finish_put(
    collection: Option<&mut IsarCollection>,
    txn: Option<&mut IsarTxn>,
    pending_put: *mut PendingPut,
) -> u8 {
    let pending_put = *Box::from_raw(pending_put);
    isar_try! {
        let collection = collection.unwrap();
        let oid = collection.finish_put(txn.unwrap(), pending_put)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete(
    collection: Option<&IsarCollection>,
    txn: Option<&mut IsarTxn>,
    object: &RawObject,
) -> u8 {
    isar_try! {
        let collection = collection.unwrap();
        collection.delete(txn.unwrap(), object.get_object_id(collection).unwrap())?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_clear(
    collection: Option<&IsarCollection>,
    txn: Option<&mut IsarTxn>,
    object: &RawObject,
) -> u8 {
    isar_try! {
        let collection = collection.unwrap();
        collection.clear(txn.unwrap())?;
    }
}
