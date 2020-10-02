use crate::error::Result;
use crate::lmdb::db::Db;
use crate::lmdb::error::{lmdb_result, LmdbError};
use crate::lmdb::txn::Txn;
use crate::lmdb::{from_mdb_val, to_mdb_val, KeyVal, EMPTY_KEY, EMPTY_VAL};
use core::ptr;
use lmdb_sys as ffi;
use lmdb_sys::MDB_val;
use std::marker::PhantomData;

#[derive(Debug)]
pub struct Cursor<'txn> {
    cursor: *mut ffi::MDB_cursor,
    _marker: PhantomData<fn() -> &'txn ()>,
}

impl<'txn> Cursor<'txn> {
    pub(crate) fn open(txn: &Txn, db: &Db) -> Result<Self> {
        let mut cursor: *mut ffi::MDB_cursor = ptr::null_mut();

        unsafe { lmdb_result(ffi::mdb_cursor_open(txn.txn, db.dbi, &mut cursor))? }

        Ok(Cursor {
            cursor,
            _marker: PhantomData,
        })
    }

    fn op_get(&self, op: u32, key: Option<MDB_val>) -> Result<Option<KeyVal<'txn>>> {
        let mut key = key.unwrap_or(EMPTY_KEY);
        let mut data = EMPTY_VAL;

        let result =
            unsafe { lmdb_result(ffi::mdb_cursor_get(self.cursor, &mut key, &mut data, op)) };

        match result {
            Ok(()) => {
                let key = unsafe { from_mdb_val(key) };
                let data = unsafe { from_mdb_val(data) };
                Ok(Some((key, data)))
            }
            Err(LmdbError::NotFound) => Ok(None),
            Err(e) => Err(e)?,
        }
    }

    pub fn move_to_first(&self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_FIRST, None)
    }

    pub fn move_to_last(&self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_LAST, None)
    }

    pub fn move_to(&self, key: &[u8]) -> Result<Option<KeyVal<'txn>>> {
        let key = unsafe { to_mdb_val(&key) };
        self.op_get(ffi::MDB_SET_KEY, Some(key))
    }

    pub fn set(&self, key: &[u8]) -> Result<bool> {
        let mut key = unsafe { to_mdb_val(key) };
        let mut data = EMPTY_VAL;

        let result = unsafe {
            lmdb_result(ffi::mdb_cursor_get(
                self.cursor,
                &mut key,
                &mut data,
                ffi::MDB_SET,
            ))
        };

        match result {
            Ok(()) => Ok(true),
            Err(LmdbError::NotFound) => Ok(false),
            Err(e) => Err(e)?,
        }
    }

    pub fn move_to_key_greater_than_or_equal_to(&self, key: &[u8]) -> Result<Option<KeyVal<'txn>>> {
        let key = unsafe { to_mdb_val(&key) };
        self.op_get(ffi::MDB_SET_RANGE, Some(key))
    }

    pub fn move_to_next(&self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_NEXT, None)
    }

    pub fn move_to_next_dup(&self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_NEXT_DUP, None)
    }

    pub fn delete_current(&self, delete_dup: bool) -> Result<()> {
        let op = if delete_dup { ffi::MDB_NODUPDATA } else { 0 };

        unsafe { lmdb_result(ffi::mdb_cursor_del(self.cursor, op))? };

        Ok(())
    }

    pub fn delete_key_prefix(&self, key_prefix: &[u8]) -> Result<()> {
        self.move_to_key_greater_than_or_equal_to(key_prefix)?;
        for item in self.iter() {
            let (key, _) = item?;
            if key[0..4] != key_prefix[..] {
                break;
            }

            self.delete_current(false)?;
        }
        Ok(())
    }

    pub fn iter<'a>(&'a self) -> CursorIterator<'a, 'txn> {
        CursorIterator::new(self, ffi::MDB_GET_CURRENT, ffi::MDB_NEXT)
    }

    pub fn iter_from_first<'a>(&'a self) -> CursorIterator<'a, 'txn> {
        CursorIterator::new(self, ffi::MDB_FIRST, ffi::MDB_NEXT)
    }
}

impl<'txn> Drop for Cursor<'txn> {
    fn drop(&mut self) {
        unsafe { ffi::mdb_cursor_close(self.cursor) }
    }
}

/// An iterator over the key/value pairs in an LMDB database.
pub struct CursorIterator<'a, 'txn> {
    /// The LMDB cursor with which to iterate.
    cursor: &'a Cursor<'txn>,

    /// The first operation to perform when the consumer calls Iter.next().
    op: u32,

    /// The next and subsequent operations to perform.
    next_op: u32,

    /// A marker to ensure the iterator doesn't outlive the transaction.
    _marker: PhantomData<fn(&'txn ())>,
}

impl<'a, 'txn> CursorIterator<'a, 'txn> {
    /// Creates a new iterator backed by the given cursor.
    fn new(cursor: &'a Cursor<'txn>, op: u32, next_op: u32) -> Self {
        CursorIterator {
            cursor,
            op,
            next_op,
            _marker: PhantomData,
        }
    }
}

impl<'a, 'txn> Iterator for CursorIterator<'a, 'txn> {
    type Item = Result<KeyVal<'txn>>;

    fn next(&mut self) -> Option<Result<KeyVal<'txn>>> {
        let result = self.cursor.op_get(self.op, None);
        self.op = self.next_op;

        match result {
            Ok(result) => match result {
                Some(result) => Some(Ok(result)),
                None => None,
            },
            Err(e) => Some(Err(e)),
        }
    }
}
