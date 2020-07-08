use crate::error::Result;
use crate::lmdb::error::lmdb_result;
use core::ptr;
use lmdb_sys as ffi;

pub struct Txn {
    pub(crate) txn: *mut ffi::MDB_txn,
}

impl Txn {
    pub fn commit(mut self) -> Result<()> {
        let result = unsafe { lmdb_result(ffi::mdb_txn_commit(self.txn)) };
        self.txn = ptr::null_mut();
        result?;
        Ok(())
    }

    pub fn abort(mut self) {
        unsafe { ffi::mdb_txn_abort(self.txn) };
        self.txn = ptr::null_mut();
    }
}

impl Drop for Txn {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            unsafe { ffi::mdb_txn_abort(self.txn) }
        }
    }
}
