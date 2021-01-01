use crate::raw_object_set::RawObject;
use isar_core::collection::IsarCollection;
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
pub unsafe extern "C" fn isar_put(
    collection: Option<&mut IsarCollection>,
    txn: Option<&mut IsarTxn>,
    object: &mut RawObject,
) -> u8 {
    isar_try! {
        let collection = collection.unwrap();
        let oid = object.get_object_id(collection);
        let data = object.object_as_slice();
        let oid = collection.put(txn.unwrap(), oid, data)?;
        object.set_object_id(oid);
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
) -> u8 {
    isar_try! {
        let collection = collection.unwrap();
        collection.clear(txn.unwrap())?;
    }
}
