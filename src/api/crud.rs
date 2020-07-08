use crate::bank::IsarBank;
use crate::error::illegal_arg;
use crate::lmdb::txn::Txn;
use crate::object_set::RawObject;

#[no_mangle]
pub unsafe extern "C" fn isar_get(
    bank: Option<&IsarBank>,
    txn: Option<&Txn>,
    object: &mut RawObject,
) -> u8 {
    let object_id = object.get_object_id();
    isar_try! {
        if object_id.is_none() {
            illegal_arg("ObjectId needs to be provided.")?;
        }
        let result = bank.unwrap().get(txn.unwrap(), &object_id.unwrap())?;
        if let Some(result) = result {
            object.set_object(result);
        } else {
            object.set_empty();
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_put(
    bank: Option<&mut IsarBank>,
    txn: Option<&Txn>,
    object: &mut RawObject,
) -> u8 {
    let oid = object.get_object_id();
    isar_try! {
        let data = object.object_as_slice();
        let oid = bank.unwrap().put(txn.unwrap(), oid, data)?;
        object.set_object_id(&oid);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_delete(
    bank: Option<&IsarBank>,
    txn: Option<&Txn>,
    object: &mut RawObject,
) -> u8 {
    let oid = object.get_object_id().unwrap();
    isar_try! {
        bank.unwrap().delete(txn.unwrap(), &oid)?;
    }
}
