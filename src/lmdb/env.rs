use crate::error::Result;
use crate::lmdb::error::{lmdb_result, LmdbError};
use crate::lmdb::txn::Txn;
use core::ptr;
use lmdb_sys as ffi;
use std::ffi::CString;

pub struct Env {
    env: *mut ffi::MDB_env,
}

unsafe impl Sync for Env {}
unsafe impl Send for Env {}

#[macro_export]
macro_rules! lmdb_try_with_cleanup {
    ($expr:expr, $cleanup:expr) => {{
        match $expr {
            ffi::MDB_SUCCESS => (),
            err_code => {
                let _ = $cleanup;
                return Err(LmdbError::from_err_code(err_code))?;
            }
        }
    }};
}

impl Env {
    pub fn create(path: &str, max_dbs: u32, max_size: usize) -> Result<Env> {
        let path = CString::new(path.as_bytes()).unwrap();
        let mut env: *mut ffi::MDB_env = ptr::null_mut();
        unsafe {
            lmdb_result(ffi::mdb_env_create(&mut env))?;
            lmdb_try_with_cleanup!(
                ffi::mdb_env_set_mapsize(env, max_size),
                ffi::mdb_env_close(env)
            );
            lmdb_try_with_cleanup!(
                ffi::mdb_env_set_maxdbs(env, max_dbs),
                ffi::mdb_env_close(env)
            );
            lmdb_try_with_cleanup!(
                ffi::mdb_env_open(env, path.as_ptr(), 0, 0o600),
                ffi::mdb_env_close(env)
            );
        }
        Ok(Env { env })
    }

    pub fn txn(&self, write: bool) -> Result<Txn> {
        self.txn_internal(write, None)
    }

    pub(crate) fn txn_internal(&self, write: bool, parent: Option<&Txn>) -> Result<Txn> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        let parent = if let Some(parent) = parent {
            parent.txn
        } else {
            ptr::null_mut()
        };

        let flags = if write { 0 } else { ffi::MDB_RDONLY };

        unsafe { lmdb_result(ffi::mdb_txn_begin(self.env, parent, flags, &mut txn))? }

        Ok(Txn::new(txn, self))
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        if !self.env.is_null() {
            unsafe { ffi::mdb_env_close(self.env) }
            self.env = ptr::null_mut();
        }
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_create() {
        get_env();
    }

    pub fn get_env() -> Env {
        let dir = tempdir().unwrap();
        Env::create(dir.path().to_str().unwrap(), 50, 100000).unwrap()
    }
}
