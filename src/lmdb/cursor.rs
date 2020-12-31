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
            Err(LmdbError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e)?,
        }
    }

    pub fn get(&self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_GET_CURRENT, None)
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

    /*pub fn set(&mut self, key: &[u8]) -> Result<bool> {
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
    }*/

    pub fn move_to_gte(&mut self, key: &[u8]) -> Result<Option<KeyVal<'txn>>> {
        let key = unsafe { to_mdb_val(&key) };
        self.op_get(ffi::MDB_SET_RANGE, Some(key))
    }

    pub fn move_to_next(&mut self) -> Result<Option<KeyVal<'txn>>> {
        self.op_get(ffi::MDB_NEXT, None)
    }

    /// Requires the cursor to have a valid position
    pub fn delete_current(&mut self, delete_dup: bool) -> Result<()> {
        let op = if delete_dup { ffi::MDB_NODUPDATA } else { 0 };

        unsafe { lmdb_result(ffi::mdb_cursor_del(self.cursor, op))? };

        Ok(())
    }

    /// Requires the cursor to have a valid position
    #[allow(clippy::while_let_loop)]
    pub fn delete_while<F>(&mut self, predicate: F, delete_dup: bool) -> Result<()>
    where
        F: Fn(&[u8], &[u8]) -> bool,
    {
        if let Some((key, val)) = self.get()? {
            if !predicate(key, val) {
                return Ok(());
            }
            self.delete_current(delete_dup)?;
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
        }

        Ok(())
    }

    /// Requires the cursor to have a valid position
    pub fn iter<'a>(&'a mut self) -> CursorIterator<'a, 'txn> {
        CursorIterator::new(self, ffi::MDB_GET_CURRENT, ffi::MDB_NEXT)
    }

    /*/// Requires the cursor to have a valid position
    pub fn iter_no_dup<'a>(&'a mut self) -> CursorIterator<'a, 'txn> {
        CursorIterator::new(self, ffi::MDB_GET_CURRENT, ffi::MDB_NODUPDATA)
    }

    pub fn iter_from_first<'a>(&'a mut self) -> CursorIterator<'a, 'txn> {
        CursorIterator::new(self, ffi::MDB_FIRST, ffi::MDB_NEXT)
    }*/
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

#[cfg(test)]
mod tests {
    use crate::lmdb::db::Db;
    use crate::lmdb::env::tests::get_env;
    use crate::lmdb::env::Env;
    use itertools::Itertools;
    use std::sync::{Arc, Mutex};

    fn get_filled_db() -> (Env, Db) {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", false, false).unwrap();
        db.put(&txn, b"key1", b"val1").unwrap();
        db.put(&txn, b"key2", b"val2").unwrap();
        db.put(&txn, b"key3", b"val3").unwrap();
        db.put(&txn, b"key4", b"val4").unwrap();
        txn.commit().unwrap();
        (env, db)
    }

    fn get_filled_db_dup() -> (Env, Db) {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", true, false).unwrap();
        db.put(&txn, b"key1", b"val1").unwrap();
        db.put(&txn, b"key1", b"val1b").unwrap();
        db.put(&txn, b"key1", b"val1c").unwrap();
        db.put(&txn, b"key2", b"val2").unwrap();
        db.put(&txn, b"key2", b"val2b").unwrap();
        db.put(&txn, b"key2", b"val2c").unwrap();
        txn.commit().unwrap();
        (env, db)
    }

    fn get_empty_db() -> (Env, Db) {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", true, false).unwrap();
        txn.commit().unwrap();
        (env, db)
    }

    #[test]
    fn test_get() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        let entry = cur.get().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1"[..])));

        cur.move_to_next().unwrap();
        let entry = cur.get().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_get_dup() {
        let (env, db) = get_filled_db_dup();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        let entry = cur.get().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1"[..])));

        cur.move_to_next().unwrap();
        let entry = cur.get().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1b"[..])));
    }

    #[test]
    fn test_move_to_first() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let first = cur.move_to_first().unwrap();
        assert_eq!(first, Some((&b"key1"[..], &b"val1"[..])));

        let next = cur.move_to_next().unwrap();
        assert_eq!(next, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_move_to_first_empty() {
        let (env, db) = get_empty_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let first = cur.move_to_first().unwrap();
        assert_eq!(first, None);

        let next = cur.move_to_next().unwrap();
        assert_eq!(next, None);
    }

    #[test]
    fn test_move_to_last() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let last = cur.move_to_last().unwrap();
        assert_eq!(last, Some((&b"key4"[..], &b"val4"[..])));

        let next = cur.move_to_next().unwrap();
        assert_eq!(next, None);
    }

    #[test]
    fn test_move_to_last_dup() {
        let (env, db) = get_filled_db_dup();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let last = cur.move_to_last().unwrap();
        assert_eq!(last, Some((&b"key2"[..], &b"val2c"[..])));
    }

    #[test]
    fn test_move_to_last_empty() {
        let (env, db) = get_empty_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to_last().unwrap();
        assert!(entry.is_none());

        let entry = cur.move_to_next().unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn test_move_to() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to(b"key2").unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));

        let entry = cur.move_to(b"key1").unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1"[..])));

        let next = cur.move_to_next().unwrap();
        assert_eq!(next, Some((&b"key2"[..], &b"val2"[..])));

        let entry = cur.move_to(b"key5").unwrap();
        assert_eq!(entry, None);
    }

    #[test]
    fn test_move_to_empty() {
        let (env, db) = get_empty_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to(b"key1").unwrap();
        assert!(entry.is_none());
        let entry = cur.move_to_next().unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn test_move_to_gte() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to_gte(b"key2").unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));

        let entry = cur.move_to_gte(b"k").unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1"[..])));

        let next = cur.move_to_next().unwrap();
        assert_eq!(next, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn move_to_gte_empty() {
        let (env, db) = get_empty_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to_gte(b"key1").unwrap();
        assert!(entry.is_none());

        let entry = cur.move_to_next().unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn test_move_to_next() {
        let (env, db) = get_filled_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1"[..])));

        let entry = cur.move_to_next().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_move_to_next_dup() {
        let (env, db) = get_filled_db_dup();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        let entry = cur.move_to_next().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1b"[..])));

        let entry = cur.move_to_next().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1c"[..])));

        let entry = cur.move_to_next().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_move_to_next_empty() {
        let (env, db) = get_empty_db();

        let txn = env.txn(false).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entry = cur.move_to_next().unwrap();
        assert!(entry.is_none());

        let entry = cur.move_to_next().unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn test_delete_current() {
        let (env, db) = get_filled_db();

        let txn = env.txn(true).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        cur.delete_current(false).unwrap();

        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_delete_current_dup() {
        let (env, db) = get_filled_db_dup();

        let txn = env.txn(true).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        cur.delete_current(false).unwrap();

        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1b"[..])));

        cur.delete_current(true).unwrap();
        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_delete_while() {
        let (env, db) = get_filled_db();

        let txn = env.txn(true).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        let entries = Arc::new(Mutex::new(vec![(b"key1", b"val1"), (b"key2", b"val2")]));

        cur.move_to_first().unwrap();
        cur.delete_while(
            |k, v| {
                let mut entries = entries.lock().unwrap();
                if entries.is_empty() {
                    return false;
                }
                let (rk, rv) = entries.remove(0);
                assert_eq!((&rk[..], &rv[..]), (k, v));
                true
            },
            false,
        )
        .unwrap();

        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key3"[..], &b"val3"[..])));
    }

    #[test]
    fn test_delete_while_dup() {
        let (env, db) = get_filled_db_dup();

        let txn = env.txn(true).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        cur.delete_current(false).unwrap();

        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key1"[..], &b"val1b"[..])));

        cur.delete_current(true).unwrap();
        let entry = cur.move_to_first().unwrap();
        assert_eq!(entry, Some((&b"key2"[..], &b"val2"[..])));
    }

    #[test]
    fn test_iter() {
        let (env, db) = get_filled_db();

        let txn = env.txn(true).unwrap();
        let mut cur = db.cursor(&txn).unwrap();

        cur.move_to_first().unwrap();
        cur.move_to_next().unwrap();
        let keys = cur
            .iter()
            .map(|r| {
                let (k, _) = r.unwrap();
                k
            })
            .collect_vec();
        assert_eq!(vec![b"key2", b"key3", b"key4"], keys);
    }
}
