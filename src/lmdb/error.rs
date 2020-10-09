use std::ffi::CStr;
use std::os::raw::c_char;
use std::{fmt, str};
use thiserror::Error;

use libc::c_int;
use lmdb_sys as ffi;
use std::result::Result;

#[derive(Debug, Error)]
pub enum LmdbError {
    /// key/data pair already exists.
    KeyExist,
    /// key/data pair not found (EOF).
    NotFound,
    /// Requested page not found - this usually indicates corruption.
    PageNotFound,
    /// Located page was wrong type.
    Corrupted,
    /// Update of meta page failed or environment had fatal IsarError.
    Panic,
    /// Environment version mismatch.
    VersionMismatch,
    /// File is not a valid LMDB file.
    Invalid,
    /// Environment mapsize reached.
    MapFull,
    /// Environment maxdbs reached.
    DbsFull,
    /// Environment maxreaders reached.
    ReadersFull,
    /// Too many TLS keys in use - Windows only.
    TlsFull,
    /// Txn has too many dirty pages.
    TxnFull,
    /// Cursor stack too deep - internal IsarError.
    CursorFull,
    /// Page has not enough space - internal IsarError.
    PageFull,
    /// Database contents grew beyond environment mapsize.
    MapResized,
    /// Operation and DB incompatible, or DB type changed. This can mean:
    ///   - The operation expects an MDB_DUPSORT / MDB_DUPFIXED database.
    ///   - Opening a named DB when the unnamed DB has MDB_DUPSORT / MDB_INTEGERKEY.
    ///   - Accessing a data record as a database, or vice versa.
    ///   - The database was dropped and recreated with different flags.
    Incompatible,
    /// Invalid reuse of reader locktable slot.
    BadRslot,
    /// Transaction cannot recover - it must be aborted.
    BadTxn,
    /// Unsupported size of key/DB name/data, or wrong DUP_FIXED size.
    BadValSize,
    /// The specified DBI was changed unexpectedly.
    BadDbi,
    /// Other IsarError.
    Other(c_int),
}

impl LmdbError {
    /// Converts a raw LmdbError code to an `Error`.
    pub fn from_err_code(err_code: c_int) -> LmdbError {
        match err_code {
            ffi::MDB_KEYEXIST => LmdbError::KeyExist,
            ffi::MDB_NOTFOUND => LmdbError::NotFound,
            ffi::MDB_PAGE_NOTFOUND => LmdbError::PageNotFound,
            ffi::MDB_CORRUPTED => LmdbError::Corrupted,
            ffi::MDB_PANIC => LmdbError::Panic,
            ffi::MDB_VERSION_MISMATCH => LmdbError::VersionMismatch,
            ffi::MDB_INVALID => LmdbError::Invalid,
            ffi::MDB_MAP_FULL => LmdbError::MapFull,
            ffi::MDB_DBS_FULL => LmdbError::DbsFull,
            ffi::MDB_READERS_FULL => LmdbError::ReadersFull,
            ffi::MDB_TLS_FULL => LmdbError::TlsFull,
            ffi::MDB_TXN_FULL => LmdbError::TxnFull,
            ffi::MDB_CURSOR_FULL => LmdbError::CursorFull,
            ffi::MDB_PAGE_FULL => LmdbError::PageFull,
            ffi::MDB_MAP_RESIZED => LmdbError::MapResized,
            ffi::MDB_INCOMPATIBLE => LmdbError::Incompatible,
            ffi::MDB_BAD_RSLOT => LmdbError::BadRslot,
            ffi::MDB_BAD_TXN => LmdbError::BadTxn,
            ffi::MDB_BAD_VALSIZE => LmdbError::BadValSize,
            ffi::MDB_BAD_DBI => LmdbError::BadDbi,
            other => LmdbError::Other(other),
        }
    }

    // Converts an `Error` to the raw IsarError code.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn to_err_code(&self) -> i32 {
        match *self {
            LmdbError::KeyExist => ffi::MDB_KEYEXIST,
            LmdbError::NotFound => ffi::MDB_NOTFOUND,
            LmdbError::PageNotFound => ffi::MDB_PAGE_NOTFOUND,
            LmdbError::Corrupted => ffi::MDB_CORRUPTED,
            LmdbError::Panic => ffi::MDB_PANIC,
            LmdbError::VersionMismatch => ffi::MDB_VERSION_MISMATCH,
            LmdbError::Invalid => ffi::MDB_INVALID,
            LmdbError::MapFull => ffi::MDB_MAP_FULL,
            LmdbError::DbsFull => ffi::MDB_DBS_FULL,
            LmdbError::ReadersFull => ffi::MDB_READERS_FULL,
            LmdbError::TlsFull => ffi::MDB_TLS_FULL,
            LmdbError::TxnFull => ffi::MDB_TXN_FULL,
            LmdbError::CursorFull => ffi::MDB_CURSOR_FULL,
            LmdbError::PageFull => ffi::MDB_PAGE_FULL,
            LmdbError::MapResized => ffi::MDB_MAP_RESIZED,
            LmdbError::Incompatible => ffi::MDB_INCOMPATIBLE,
            LmdbError::BadRslot => ffi::MDB_BAD_RSLOT,
            LmdbError::BadTxn => ffi::MDB_BAD_TXN,
            LmdbError::BadValSize => ffi::MDB_BAD_VALSIZE,
            LmdbError::BadDbi => ffi::MDB_BAD_DBI,
            LmdbError::Other(err_code) => err_code,
        }
    }
}

impl fmt::Display for LmdbError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let description = unsafe {
            // This is safe since the IsarError messages returned from mdb_strerror are static.
            let err: *const c_char = ffi::mdb_strerror(self.to_err_code()) as *const c_char;
            str::from_utf8_unchecked(CStr::from_ptr(err).to_bytes())
        };

        fmt.write_str(description)
    }
}

#[inline]
pub fn lmdb_result(err_code: c_int) -> Result<(), LmdbError> {
    if err_code == ffi::MDB_SUCCESS {
        Ok(())
    } else {
        Err(LmdbError::from_err_code(err_code))
    }
}

/*#[macro_export]
macro_rules! lmdb_try_with_cleanup {
    ($expr:expr, $cleanup:expr) => {{
        match $expr {
            ffi::MDB_SUCCESS => (),
            err_code => {
                let _ = $cleanup;
                return Err(LmdbError::from_err_code(err_code));
            }
        }
    }};
}*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_description() {
        assert_eq!(
            "Permission denied",
            LmdbError::from_err_code(13).to_string()
        );
        assert_eq!(
            "MDB_NOTFOUND: No matching key/data pair found",
            LmdbError::NotFound.to_string()
        );
    }
}
