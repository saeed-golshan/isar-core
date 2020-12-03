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
    _marker: PhantomData<&'txn ()>,
}

impl<'txn> Cursor<'txn> {
    pub(crate) fn open(txn: &'txn Txn, db: &Db) -> Result<Cursor<'txn>> {
        let mut cursor: *mut ffi::MDB_cursor = ptr::null_mut();

        unsafe { lmdb_result(ffi::mdb_cursor_open(txn.txn, db.dbi, &mut cursor))? }

        Ok(Cursor {
            cursor,
            _marker: PhantomData,
        })
    }

    fn op_get(&mut self, op: u32, key: Option<MDB_val>) -> Result<Option<KeyVal<'txn>>> {
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
            Err(LmdbError::NotFound { backtrace: _ }) => Ok(None),
            Err(e) => Err(e)?,
        }
    }

    pub fn move_to_first(&mut self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_FIRST, None)
    }

    pub fn move_to_last(&mut self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_LAST, None)
    }

    pub fn move_to(&mut self, key: &[u8]) -> Result<Option<KeyVal<'txn>>> {
        let key = unsafe { to_mdb_val(&key) };
        self.op_get(ffi::MDB_SET_KEY, Some(key))
    }

    pub fn set(&mut self, key: &[u8]) -> Result<bool> {
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
            Err(LmdbError::NotFound { backtrace: _ }) => Ok(false),
            Err(e) => Err(e)?,
        }
    }

    pub fn move_to_key_greater_than_or_equal_to(
        &mut self,
        key: &[u8],
    ) -> Result<Option<KeyVal<'txn>>> {
        let key = unsafe { to_mdb_val(&key) };
        self.op_get(ffi::MDB_SET_RANGE, Some(key))
    }

    pub fn move_to_next(&mut self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_NEXT, None)
    }

    pub fn delete_current(&mut self, delete_dup: bool) -> Result<()> {
        let op = if delete_dup { ffi::MDB_NODUPDATA } else { 0 };

        unsafe { lmdb_result(ffi::mdb_cursor_del(self.cursor, op))? };

        Ok(())
    }

    #[allow(clippy::while_let_loop)]
    pub fn delete_while<F>(&mut self, predicate: F, delete_dup: bool) -> Result<()>
    where
        F: Fn(&[u8], &[u8]) -> bool,
    {
        loop {
            if let Some((key, val)) = self.move_to_next()? {
                if !predicate(key, val) {
                    break;
                }
            } else {
                break;
            }
            self.delete_current(delete_dup)?;
        }
        Ok(())
    }

    pub fn iter<'a>(&'a mut self) -> CursorIterator<'a, 'txn> {
        CursorIterator::new(self, ffi::MDB_GET_CURRENT, ffi::MDB_NEXT)
    }

    pub fn iter_no_dup<'a>(&'a mut self) -> CursorIterator<'a, 'txn> {
        CursorIterator::new(self, ffi::MDB_GET_CURRENT, ffi::MDB_NODUPDATA)
    }

    pub fn iter_from_first<'a>(&'a mut self) -> CursorIterator<'a, 'txn> {
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
    cursor: &'a mut Cursor<'txn>,

    /// The first operation to perform when the consumer calls Iter.next().
    op: u32,

    /// The next and subsequent operations to perform.
    next_op: u32,
}

impl<'a, 'txn> CursorIterator<'a, 'txn> {
    /// Creates a new iterator backed by the given cursor.
    fn new(cursor: &'a mut Cursor<'txn>, op: u32, next_op: u32) -> Self {
        CursorIterator {
            cursor,
            op,
            next_op,
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
