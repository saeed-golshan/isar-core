use crate::error::{illegal_state, Result};
use crate::lmdb::txn::Txn;

pub struct IsarTxn {
    txn: Option<Txn>,
    write: bool,
}

impl IsarTxn {
    pub(crate) fn new(txn: Txn, write: bool) -> Self {
        IsarTxn {
            txn: Some(txn),
            write,
        }
    }

    pub fn is_usable(&self) -> bool {
        self.txn.is_some()
    }

    pub(crate) fn get_read_txn(&self) -> Result<&Txn> {
        if let Some(txn) = &self.txn {
            Ok(txn)
        } else {
            illegal_state("Transaction currently in use or closed.")
        }
    }

    pub(crate) fn take_write_txn(&mut self) -> Result<Txn> {
        if !self.write {
            illegal_state("Write transaction required")
        } else if let Some(txn) = self.txn.take() {
            Ok(txn)
        } else {
            illegal_state("Transaction currently in use or closed.")
        }
    }

    pub(crate) fn put_write_txn(&mut self, txn: Txn) {
        assert!(self.write);
        self.txn = Some(txn)
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
