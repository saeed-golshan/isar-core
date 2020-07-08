use crate::lmdb::error::LmdbError;
use std::error::Error;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, IsarError>;

#[derive(Debug, Error)]
pub enum IsarError {
    #[error("VersionError: {message:?}")]
    VersionError { message: String },

    #[error("The database is full.")]
    DbFull { source: LmdbError },

    #[error("UniqueViolated: {message:?}")]
    UniqueViolated {
        source: Option<LmdbError>,
        message: String,
    },

    #[error("IllegalState: {message:?}")]
    IllegalState {
        source: Option<Box<dyn Error>>,
        message: String,
    },

    #[error("IllegalArgument: {message:?}")]
    IllegalArgument {
        source: Option<Box<dyn Error>>,
        message: String,
    },

    #[error("DbCorrupted: {message:?}")]
    DbCorrupted {
        source: Option<Box<dyn Error>>,
        message: String,
    },

    #[error("LmdbError: {source:?}")]
    LmdbError { source: LmdbError },

    #[error("Error: {source:?} {message:?}")]
    Error {
        source: Option<Box<dyn Error>>,
        message: String,
    },
}

impl From<LmdbError> for IsarError {
    fn from(e: LmdbError) -> Self {
        match e {
            LmdbError::MapFull => IsarError::DbFull { source: e },
            LmdbError::Other(2) => IsarError::IllegalArgument {
                source: Some(Box::new(e)),
                message:
                    "No such file or directory. Please make sure that the provided path is valid."
                        .to_string(),
            },
            _ => IsarError::LmdbError { source: e },
        }
    }
}

pub fn illegal_state(msg: &str) -> Result<()> {
    Err(IsarError::IllegalState {
        source: None,
        message: msg.to_string(),
    })
}

pub fn illegal_arg(msg: &str) -> Result<()> {
    Err(IsarError::IllegalArgument {
        source: None,
        message: msg.to_string(),
    })
}

pub fn corrupted(msg: &str) -> Result<()> {
    Err(IsarError::DbCorrupted {
        source: None,
        message: msg.to_string(),
    })
}
