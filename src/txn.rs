use crate::error::{illegal_state, Result};
use crate::lmdb::txn::Txn;

pub struct IsarTxn<'env> {
    txn: Option<Txn<'env>>,
    write: bool,
}

impl<'env> IsarTxn<'env> {
    pub(crate) fn new(txn: Txn<'env>, write: bool) -> Self {
        IsarTxn {
            txn: Some(txn),
            write,
        }
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

    pub(crate) fn get_txn(&self) -> Result<&Txn> {
        if let Some(txn) = &self.txn {
            Ok(txn)
        } else {
            illegal_state("Transaction is already closed.")
        }
    }

    pub(crate) fn get_write_txn(&self) -> Result<&Txn> {
        if self.write {
            self.get_txn()
        } else {
            illegal_state("Write transaction required.")
        }
    }

    pub fn commit(mut self) -> Result<()> {
        if let Some(txn) = self.txn.take() {
            txn.commit()
        } else {
            illegal_state("Transaction currently in use or closed.")
        }
    }

    pub fn abort(mut self) -> Result<()> {
        if let Some(txn) = self.txn.take() {
            txn.abort();
            Ok(())
        } else {
            illegal_state("Transaction currently in use or closed.")
        }
    }
}
