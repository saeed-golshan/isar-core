#[macro_use]
pub mod debug;

use crate::error::{IsarError, Result};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use time::OffsetDateTime;

pub unsafe fn from_c_str<'a>(str: *const c_char) -> Result<&'a str> {
    let result = CStr::from_ptr(str).to_str();

    match result {
        Ok(str) => Ok(str),
        Err(e) => Err(IsarError::IllegalArgument {
            source: Some(Box::new(e)),
            message: "The provided String is not valid.".to_string(),
        }),
    }
}

pub fn to_c_str(str: &str) -> Result<CString> {
    match CString::new(str.as_bytes()) {
        Ok(str) => Ok(str),
        Err(e) => Err(IsarError::IllegalArgument {
            source: Some(Box::new(e)),
            message: "The provided String is not valid.".to_string(),
        }),
    }
}

pub fn seconds_since_epoch() -> u64 {
    OffsetDateTime::now_utc().timestamp() as u64
}
