use crate::error::{IsarError, Result};
use crate::lmdb::txn::Txn;

pub struct IsarTxn<'env> {
    txn: Txn<'env>,
    write: bool,
}

impl<'env> IsarTxn<'env> {
    pub(crate) fn new(txn: Txn<'env>, write: bool) -> Self {
        IsarTxn { txn, write }
    }

    pub(crate) fn exec_atomic_write<T, F>(&mut self, job: F) -> Result<T>
    where
        F: FnOnce(&Txn) -> Result<T>,
    {
        let nested_txn = self.get_write_txn()?.nested_txn(true)?;
        let result = job(&nested_txn)?;
        nested_txn.commit()?;
        Ok(result)
    }

    pub(crate) fn get_txn(&self) -> &Txn {
        &self.txn
    }

    pub(crate) fn get_write_txn(&self) -> Result<&Txn> {
        if self.write {
            Ok(&self.txn)
        } else {
            Err(IsarError::WriteTxnRequired {})
        }
    }

    pub fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    pub fn abort(self) {
        self.txn.abort();
    }
}
