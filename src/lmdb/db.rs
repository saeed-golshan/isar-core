use crate::error::{IsarError, Result};
use crate::lmdb::cursor::Cursor;
use crate::lmdb::error::{lmdb_result, LmdbError};
use crate::lmdb::txn::Txn;
use crate::lmdb::{from_mdb_val, to_mdb_val, EMPTY_VAL};
use crate::utils::to_c_str;
use lmdb_sys as ffi;
use std::ptr;

#[derive(Copy, Clone)]
pub struct Db {
    pub(crate) dbi: ffi::MDB_dbi,
}

impl Db {
    pub fn open(
        txn: &Txn,
        name: &str,
        int_keys: bool,
        dup: bool,
        fixed_vals: bool,
    ) -> Result<Self> {
        let name = to_c_str(name)?;
        let mut flags = ffi::MDB_CREATE;
        if dup {
            flags |= ffi::MDB_DUPSORT;
            if int_keys {
                flags |= ffi::MDB_INTEGERDUP;
            }
            if fixed_vals {
                flags |= ffi::MDB_DUPFIXED;
            }
        } else if int_keys {
            flags |= ffi::MDB_INTEGERKEY;
        }

        let mut dbi: ffi::MDB_dbi = 0;
        unsafe {
            lmdb_result(ffi::mdb_dbi_open(txn.txn, name.as_ptr(), flags, &mut dbi))?;
        }
        Ok(Self { dbi })
    }

    pub fn get<'txn>(&self, txn: &'txn Txn, key: &[u8]) -> Result<Option<&'txn [u8]>> {
        let mut data = EMPTY_VAL;
        let result = unsafe {
            let mut key = to_mdb_val(key);
            lmdb_result(ffi::mdb_get(txn.txn, self.dbi, &mut key, &mut data))
        };

        match result {
            Ok(()) => {
                let data = unsafe { from_mdb_val(data) };
                Ok(Some(data))
            }
            Err(LmdbError::NotFound) => Ok(None),
            Err(e) => Err(e)?,
        }
    }

    pub fn put(&self, txn: &Txn, key: &[u8], data: &[u8]) -> Result<()> {
        self.put_internal(txn, key, data, 0)?;
        Ok(())
    }

    pub fn put_no_override(&self, txn: &Txn, key: &[u8], data: &[u8]) -> Result<bool> {
        let result = self.put_internal(txn, key, data, ffi::MDB_NOOVERWRITE);
        return match result {
            Ok(()) => Ok(true),
            Err(LmdbError::KeyExist) => Ok(false),
            Err(e) => Err(e)?,
        };
    }

    fn put_internal(
        &self,
        txn: &Txn,
        key: &[u8],
        data: &[u8],
        flags: u32,
    ) -> std::result::Result<(), LmdbError> {
        unsafe {
            let mut key = to_mdb_val(key);
            let mut data = to_mdb_val(data);
            lmdb_result(ffi::mdb_put(txn.txn, self.dbi, &mut key, &mut data, flags))?;
        }
        Ok(())
    }

    pub fn delete(&self, txn: &Txn, key: &[u8], data: Option<&[u8]>) -> Result<()> {
        unsafe {
            let mut key = to_mdb_val(key);
            let data = if let Some(data) = data {
                &mut to_mdb_val(data)
            } else {
                ptr::null_mut()
            };
            lmdb_result(ffi::mdb_del(txn.txn, self.dbi, &mut key, data))?;
        }
        Ok(())
    }

    pub fn clear(&self, txn: &Txn) -> Result<()> {
        unsafe {
            lmdb_result(ffi::mdb_drop(txn.txn, self.dbi, 0))?;
        }
        Ok(())
    }

    pub fn cursor<'txn>(&self, txn: &'txn Txn) -> Result<Cursor<'txn>> {
        Cursor::open(txn, &self)
    }
}

#[cfg(test)]
mod tests {
    use crate::lmdb::env::tests::get_env;

    use super::*;
    use itertools::Itertools;

    #[test]
    fn test_open() {
        let env = get_env();

        let read_txn = env.txn(false).unwrap();
        assert!(Db::open(&read_txn, "test", false, false, false).is_err());
        read_txn.abort();

        let flags = vec![
            (false, false, false, 0),
            (false, false, true, 0),
            (false, true, false, ffi::MDB_DUPSORT),
            (false, true, true, ffi::MDB_DUPSORT | ffi::MDB_DUPFIXED),
            (true, false, false, ffi::MDB_INTEGERKEY),
            (true, false, true, ffi::MDB_INTEGERKEY),
            (true, true, false, ffi::MDB_DUPSORT | ffi::MDB_INTEGERDUP),
            (
                true,
                true,
                true,
                ffi::MDB_DUPSORT | ffi::MDB_INTEGERDUP | ffi::MDB_DUPFIXED,
            ),
        ];

        for (i, (int_keys, dup, fixed_vals, flags)) in flags.iter().enumerate() {
            let txn = env.txn(true).unwrap();
            let db = Db::open(
                &txn,
                format!("test{}", i).as_str(),
                *int_keys,
                *dup,
                *fixed_vals,
            )
            .unwrap();
            txn.commit().unwrap();

            let mut actual_flags: u32 = 0;
            let txn = env.txn(false).unwrap();
            unsafe {
                ffi::mdb_dbi_flags(txn.txn, db.dbi, &mut actual_flags);
            }
            txn.abort();
            assert_eq!(*flags, actual_flags);
        }
    }

    #[test]
    fn test_get_put_delete() {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", false, false, false).unwrap();
        db.put(&txn, b"key1", b"val1").unwrap();
        db.put(&txn, b"key2", b"val2").unwrap();
        db.put(&txn, b"key3", b"val3").unwrap();
        db.put(&txn, b"key2", b"val4").unwrap();
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        assert_eq!(b"val1", db.get(&txn, b"key1").unwrap().unwrap());
        assert_eq!(b"val4", db.get(&txn, b"key2").unwrap().unwrap());
        assert_eq!(b"val3", db.get(&txn, b"key3").unwrap().unwrap());
        assert_eq!(db.get(&txn, b"key").unwrap(), None);

        db.delete(&txn, b"key1", None).unwrap();
        assert_eq!(db.get(&txn, b"key1").unwrap(), None);
        txn.abort();
    }

    #[test]
    fn test_put_get_del_multi() {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", false, true, false).unwrap();

        db.put(&txn, b"key1", b"val1").unwrap();
        db.put(&txn, b"key1", b"val2").unwrap();
        db.put(&txn, b"key1", b"val3").unwrap();
        db.put(&txn, b"key2", b"val4").unwrap();
        db.put(&txn, b"key2", b"val5").unwrap();
        db.put(&txn, b"key2", b"val6").unwrap();
        db.put(&txn, b"key3", b"val7").unwrap();
        db.put(&txn, b"key3", b"val8").unwrap();
        db.put(&txn, b"key3", b"val9").unwrap();
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        {
            let mut cur = db.cursor(&txn).unwrap();
            assert_eq!(cur.set(b"key2").unwrap(), true);
            let iter = cur.iter_dup();
            //let vals = iter.map(|x| x.1).collect_vec();
            //assert!(iter.error.is_none());
            //assert_eq!(vals, vec![b"val4", b"val5", b"val6"]);
        }
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        db.delete(&txn, b"key1", Some(b"val2")).unwrap();
        db.delete(&txn, b"key2", None).unwrap();
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        {
            let mut cur = db.cursor(&txn).unwrap();
            cur.move_to_first().unwrap();
            let iter = cur.iter();
            let vals: Result<Vec<&[u8]>> = iter.map_results(|x| x.1).collect();
            assert_eq!(
                vals.unwrap(),
                vec![b"val1", b"val3", b"val7", b"val8", b"val9"]
            );
        }
        txn.commit().unwrap();
    }

    #[test]
    fn test_clear_db() {
        let env = get_env();
        let txn = env.txn(true).unwrap();
        let db = Db::open(&txn, "test", false, false, false).unwrap();
        db.put(&txn, b"key1", b"val1").unwrap();
        db.put(&txn, b"key2", b"val2").unwrap();
        db.put(&txn, b"keye", b"val3").unwrap();
        txn.commit().unwrap();

        let txn = env.txn(true).unwrap();
        db.clear(&txn).unwrap();
        txn.commit().unwrap();

        let txn = env.txn(false).unwrap();
        {
            let cursor = db.cursor(&txn).unwrap();
            assert!(cursor.move_to_first().unwrap().is_none());
        }
        txn.abort();
    }
}
