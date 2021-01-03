use crate::async_txn::{AsyncResponse, IsarAsyncTxn};
use crate::raw_object_set::{RawObject, RawObjectSend};
use isar_core::collection::IsarCollection;
use isar_core::error::Result;
use isar_core::txn::IsarTxn;

#[no_mangle]
pub unsafe extern "C" fn isar_get(
    collection: &IsarCollection,
    txn: &IsarTxn,
    object: &mut RawObject,
) -> u8 {
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
    txn.exec(move |txn| -> Result<AsyncResponse> {
        let result = collection.get(txn, oid)?;
        if let Some(result) = result {
            object.0.set_object(result);
        } else {
            object.0.set_empty();
        }
        Ok(AsyncResponse::success())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_put(
    collection: &mut IsarCollection,
    txn: &mut IsarTxn,
    object: &mut RawObject,
) -> u8 {
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
    txn.exec(move |txn| -> Result<AsyncResponse> {
        let data = object.0.object_as_slice();
        let oid = collection.put(txn, oid, data)?;
        object.0.set_object_id(oid);
        Ok(AsyncResponse::success())
    });
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete(
    collection: &IsarCollection,
    txn: &mut IsarTxn,
    object: &RawObject,
) -> u8 {
    isar_try! {
        collection.delete(txn, object.get_object_id(collection).unwrap())?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_clear(collection: &IsarCollection, txn: &mut IsarTxn) -> u8 {
    isar_try! {
        collection.clear(txn)?;
    }
}
