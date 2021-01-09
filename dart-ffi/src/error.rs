use isar_core::error::IsarError;

pub trait ErrCode {
    fn err_code(&self) -> i32;
}

impl ErrCode for IsarError {
    fn err_code(&self) -> i32 {
        match self {
            IsarError::VersionError { .. } => 100,
            IsarError::PathError { .. } => 101,
            IsarError::DbFull { .. } => 102,
            IsarError::UniqueViolated { .. } => 103,
            IsarError::WriteTxnRequired { .. } => 104,
            IsarError::InvalidObjectId { .. } => 105,
            IsarError::InvalidObject { .. } => 106,
            IsarError::TransactionClosed { .. } => 107,
            IsarError::IllegalArg { .. } => 108,
            IsarError::DbCorrupted { .. } => 109,
            IsarError::MigrationError { .. } => 110,
            IsarError::LmdbError { code } => *code,
        }
    }
}

#[macro_export]
macro_rules! isar_try {
    { $($token:tt)* } => {{
        use crate::error::ErrCode;
        #[allow(unused_mut)] {
            let mut l = || -> isar_core::error::Result<()> {
                $($token)*
                Ok(())
            };
            match l() {
                Ok(_) => 0,
                Err(e) => {
                    eprintln!("{}",e);
                    e.err_code()
                },
            }
        }
    }}
}
