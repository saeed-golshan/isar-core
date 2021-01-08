use crate::lmdb::error::LmdbError;
use std::error::Error;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, IsarError>;

#[derive(Debug, Error)]
pub enum IsarError {
    #[error("VersionError: {message:?}")]
    VersionError { message: String },

    #[error("No such file or directory. Please make sure that the provided path is valid.")]
    PathError {},

    #[error("The database is full.")]
    DbFull {},

    #[error("Unique index violated.")]
    UniqueViolated {},

    #[error("Write transaction required.")]
    WriteTxnRequired {},

    #[error("The ObjectId is not valid for this collection.")]
    InvalidObjectId {},

    #[error("The provided object is invalid.")]
    InvalidObject {},

    #[error("Transaction closed.")]
    TransactionClosed {},

    #[error("IllegalArg: {message:?}.")]
    IllegalArg { message: String },

    #[error("DbCorrupted: {message:?}")]
    DbCorrupted {
        source: Option<Box<dyn Error>>,
        message: String,
    },

    #[error("LmdbError: {code:?}")]
    LmdbError { code: i32 },
}

impl IsarError {}

impl From<LmdbError> for IsarError {
    fn from(e: LmdbError) -> Self {
        match e {
            LmdbError::MapFull {} => IsarError::DbFull {},
            _ => IsarError::LmdbError {
                code: e.to_err_code(),
            },
        }
    }
}

pub fn illegal_arg<T>(msg: &str) -> Result<T> {
    Err(IsarError::IllegalArg {
        message: msg.to_string(),
    })
}
