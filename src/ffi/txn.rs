use crate::instance::IsarInstance;
use crate::lmdb::txn::Txn;

#[no_mangle]
pub unsafe extern "C" fn isar_txn_begin(
    isar: Option<&IsarInstance>,
    txn: *mut *const Txn,
    write: bool,
) -> u8 {
    isar_try! {
        let new_txn = isar.unwrap().begin_txn(write)?;
        let txn_ptr = Box::into_raw(Box::new(new_txn));
        txn.write(txn_ptr);
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_txn_commit(txn: *mut Txn) -> u8 {
    isar_try! {
        let txn = Box::from_raw(txn);
        txn.commit()?;
    }
}

#[no_mangle]
pub unsafe extern "C" fn isar_txn_abort(txn: *mut Txn) {
    let txn = Box::from_raw(txn);
    txn.abort();
}
