use crate::error::{illegal_state, Result};
use crate::lmdb::txn::Txn;

pub struct IsarTxn {
    pub(crate) txn: Txn,
    write: bool,
}

impl IsarTxn {
    pub(crate) fn new(txn: Txn, write: bool) -> Self {
        IsarTxn { txn, write }
    }

    pub fn require_write(&self) -> Result<()> {
        if !self.write {
            illegal_state("Write transaction required")
        } else {
            Ok(())
        }
    }

    pub fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    pub fn abort(self) {
        self.txn.abort();
    }
}
