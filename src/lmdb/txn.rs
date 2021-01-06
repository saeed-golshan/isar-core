use crate::error::Result;
use crate::lmdb::env::Env;
use crate::lmdb::error::lmdb_result;
use core::ptr;
use lmdb_sys as ffi;

pub struct Txn<'env> {
    pub(crate) txn: *mut ffi::MDB_txn,
    env: &'env Env,
}

impl<'env> Txn<'env> {
    pub(crate) fn new(txn: *mut ffi::MDB_txn, env: &'env Env) -> Self {
        Txn { txn, env }
    }

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

    pub fn nested_txn(&self, write: bool) -> Result<Self> {
        self.env.txn_internal(write, Some(self))
    }
}

impl<'a> Drop for Txn<'a> {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            unsafe { ffi::mdb_txn_abort(self.txn) }
            self.txn = ptr::null_mut();
        }
    }
}
