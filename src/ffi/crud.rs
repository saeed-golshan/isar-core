use crate::collection::IsarCollection;
use crate::error::illegal_arg;
use crate::lmdb::txn::Txn;
use crate::object::object_set::RawObject;

#[no_mangle]
pub unsafe extern "C" fn isar_get(
    collection: Option<&IsarCollection>,
    txn: Option<&Txn>,
    object: &mut RawObject,
) -> u8 {
    let object_id = object.get_object_id();
    isar_try! {
        if object_id.is_none() {
            illegal_arg("ObjectId needs to be provided.")?;
        }
        let result = collection.unwrap().get(txn.unwrap(), object_id.unwrap())?;
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
    txn: Option<&Txn>,
    object: &mut RawObject,
) -> u8 {
    let oid = object.get_object_id();
    isar_try! {
        let data = object.object_as_slice();
        let oid = collection.unwrap().put(txn.unwrap(), oid, data)?;
        object.set_object_id(&oid);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete(
    collection: Option<&IsarCollection>,
    txn: Option<&Txn>,
    object: &mut RawObject,
) -> u8 {
    let oid = object.get_object_id().unwrap();
    isar_try! {
        collection.unwrap().delete(txn.unwrap(), oid)?;
    }
}
