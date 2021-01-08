use crate::async_txn::IsarAsyncTxn;
use crate::raw_object_set::{RawObject, RawObjectSend};
use isar_core::collection::IsarCollection;
use isar_core::error::Result;
use isar_core::txn::IsarTxn;

#[no_mangle]
pub unsafe extern "C" fn isar_get(
    collection: &IsarCollection,
    txn: &IsarTxn,
    object: &mut RawObject,
) -> i32 {
    isar_try! {
        let object_id = object.get_object_id(collection).unwrap();
        let result = collection.get(txn, object_id)?;
        if let Some(result) = result {
            object.set_object(result);
        } else {
            object.set_empty();
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_get_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    object: &'static mut RawObject,
) {
    let object = RawObjectSend(object);
    let oid = object.0.get_object_id(collection).unwrap();
    txn.exec(move |txn| -> Result<()> {
        let result = collection.get(txn, oid)?;
        if let Some(result) = result {
            object.0.set_object(result);
        } else {
            object.0.set_empty();
        }
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_put(
    collection: &mut IsarCollection,
    txn: &mut IsarTxn,
    object: &mut RawObject,
) -> i32 {
    isar_try! {
        let oid = object.get_object_id(collection);
        let data = object.object_as_slice();
        let oid = collection.put(txn, oid, data)?;
        object.set_object_id(oid);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_put_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    object: &'static mut RawObject,
) {
    let object = RawObjectSend(object);
    let oid = object.0.get_object_id(collection);
    txn.exec(move |txn| -> Result<()> {
        let data = object.0.object_as_slice();
        let oid = collection.put(txn, oid, data)?;
        object.0.set_object_id(oid);
        Ok(())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    object: &RawObject,
) -> i32 {
    isar_try! {
    let oid = object.get_object_id(collection).unwrap();
        collection.delete(txn, oid)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
    object: &RawObject,
) {
    let oid = object.get_object_id(collection).unwrap();
    txn.exec(move |txn| collection.delete(txn, oid));
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete_all(collection: &IsarCollection, txn: &mut IsarTxn) -> i32 {
    isar_try! {
        collection.delete_all(txn)?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete_all_async(
    collection: &'static IsarCollection,
    txn: &IsarAsyncTxn,
) {
    txn.exec(move |txn| collection.delete_all(txn));
}
