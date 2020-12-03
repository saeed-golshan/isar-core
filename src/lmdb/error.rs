use std::ffi::CStr;
use std::os::raw::c_char;
use std::{fmt, str};
use thiserror::Error;

use libc::c_int;
use lmdb_sys as ffi;
use std::backtrace::Backtrace;
use std::result::Result;

#[derive(Debug, Error)]
pub enum LmdbError {
    /// key/data pair already exists.
    KeyExist { backtrace: Backtrace },
    /// key/data pair not found (EOF).
    NotFound { backtrace: Backtrace },
    /// Requested page not found - this usually indicates corruption.
    PageNotFound { backtrace: Backtrace },
    /// Located page was wrong type.
    Corrupted { backtrace: Backtrace },
    /// Update of meta page failed or environment had fatal IsarError.
    Panic { backtrace: Backtrace },
    /// Environment version mismatch.
    VersionMismatch { backtrace: Backtrace },
    /// File is not a valid LMDB file.
    Invalid { backtrace: Backtrace },
    /// Environment mapsize reached.
    MapFull { backtrace: Backtrace },
    /// Environment maxdbs reached.
    DbsFull { backtrace: Backtrace },
    /// Environment maxreaders reached.
    ReadersFull { backtrace: Backtrace },
    /// Too many TLS keys in use - Windows only.
    TlsFull { backtrace: Backtrace },
    /// Txn has too many dirty pages.
    TxnFull { backtrace: Backtrace },
    /// Cursor stack too deep - internal IsarError.
    CursorFull { backtrace: Backtrace },
    /// Page has not enough space - internal IsarError.
    PageFull { backtrace: Backtrace },
    /// Database contents grew beyond environment mapsize.
    MapResized { backtrace: Backtrace },
    /// Operation and DB incompatible, or DB type changed. This can mean:
    ///   - The operation expects an MDB_DUPSORT / MDB_DUPFIXED database.
    ///   - Opening a named DB when the unnamed DB has MDB_DUPSORT / MDB_INTEGERKEY.
    ///   - Accessing a data record as a database, or vice versa.
    ///   - The database was dropped and recreated with different flags.
    Incompatible { backtrace: Backtrace },
    /// Invalid reuse of reader locktable slot.
    BadRslot { backtrace: Backtrace },
    /// Transaction cannot recover - it must be aborted.
    BadTxn { backtrace: Backtrace },
    /// Unsupported size of key/DB name/data, or wrong DUP_FIXED size.
    BadValSize { backtrace: Backtrace },
    /// The specified DBI was changed unexpectedly.
    BadDbi { backtrace: Backtrace },
    /// Other IsarError.
    Other { code: c_int, backtrace: Backtrace },
}

impl LmdbError {
    /// Converts a raw LmdbError code to an `Error`.
    pub fn from_err_code(err_code: c_int) -> LmdbError {
        match err_code {
            ffi::MDB_KEYEXIST => LmdbError::KeyExist {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_NOTFOUND => LmdbError::NotFound {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_PAGE_NOTFOUND => LmdbError::PageNotFound {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_CORRUPTED => LmdbError::Corrupted {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_PANIC => LmdbError::Panic {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_VERSION_MISMATCH => LmdbError::VersionMismatch {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_INVALID => LmdbError::Invalid {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_MAP_FULL => LmdbError::MapFull {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_DBS_FULL => LmdbError::DbsFull {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_READERS_FULL => LmdbError::ReadersFull {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_TLS_FULL => LmdbError::TlsFull {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_TXN_FULL => LmdbError::TxnFull {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_CURSOR_FULL => LmdbError::CursorFull {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_PAGE_FULL => LmdbError::PageFull {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_MAP_RESIZED => LmdbError::MapResized {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_INCOMPATIBLE => LmdbError::Incompatible {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_BAD_RSLOT => LmdbError::BadRslot {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_BAD_TXN => LmdbError::BadTxn {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_BAD_VALSIZE => LmdbError::BadValSize {
                backtrace: Backtrace::force_capture(),
            },
            ffi::MDB_BAD_DBI => LmdbError::BadDbi {
                backtrace: Backtrace::force_capture(),
            },
            other => LmdbError::Other {
                code: other,
                backtrace: Backtrace::force_capture(),
            },
        }
    }

    // Converts an `Error` to the raw IsarError code.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn to_err_code(&self) -> i32 {
        match *self {
            LmdbError::KeyExist { backtrace: _ } => ffi::MDB_KEYEXIST,
            LmdbError::NotFound { backtrace: _ } => ffi::MDB_NOTFOUND,
            LmdbError::PageNotFound { backtrace: _ } => ffi::MDB_PAGE_NOTFOUND,
            LmdbError::Corrupted { backtrace: _ } => ffi::MDB_CORRUPTED,
            LmdbError::Panic { backtrace: _ } => ffi::MDB_PANIC,
            LmdbError::VersionMismatch { backtrace: _ } => ffi::MDB_VERSION_MISMATCH,
            LmdbError::Invalid { backtrace: _ } => ffi::MDB_INVALID,
            LmdbError::MapFull { backtrace: _ } => ffi::MDB_MAP_FULL,
            LmdbError::DbsFull { backtrace: _ } => ffi::MDB_DBS_FULL,
            LmdbError::ReadersFull { backtrace: _ } => ffi::MDB_READERS_FULL,
            LmdbError::TlsFull { backtrace: _ } => ffi::MDB_TLS_FULL,
            LmdbError::TxnFull { backtrace: _ } => ffi::MDB_TXN_FULL,
            LmdbError::CursorFull { backtrace: _ } => ffi::MDB_CURSOR_FULL,
            LmdbError::PageFull { backtrace: _ } => ffi::MDB_PAGE_FULL,
            LmdbError::MapResized { backtrace: _ } => ffi::MDB_MAP_RESIZED,
            LmdbError::Incompatible { backtrace: _ } => ffi::MDB_INCOMPATIBLE,
            LmdbError::BadRslot { backtrace: _ } => ffi::MDB_BAD_RSLOT,
            LmdbError::BadTxn { backtrace: _ } => ffi::MDB_BAD_TXN,
            LmdbError::BadValSize { backtrace: _ } => ffi::MDB_BAD_VALSIZE,
            LmdbError::BadDbi { backtrace: _ } => ffi::MDB_BAD_DBI,
            LmdbError::Other {
                code: err_code,
                backtrace: _,
            } => err_code,
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
    //use super::*;
}
