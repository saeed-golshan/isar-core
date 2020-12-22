#![allow(clippy::missing_safety_doc)]

#[macro_use]
pub mod debug;

use core::mem;
use time::OffsetDateTime;

pub fn seconds_since_epoch() -> u64 {
    OffsetDateTime::now_utc().unix_timestamp() as u64
}

#[macro_export]
macro_rules! option (
    ($option:expr, $value:expr) => {
        if $option {
            Some($value)
        } else {
            None
        }
    };
);

#[macro_export]
macro_rules! map_option (
    ($option:expr, $var:ident, $map:expr) => {
        if let Some($var) = $option {
            Some($map)
        } else {
            None
        }
    };
);

#[repr(C, align(8))]
struct Align8([u8; 8]);

pub fn aligned_vec(size: usize) -> Vec<u8> {
    assert_eq!(size % 8, 0);
    let n_units = size / mem::size_of::<Align8>();

    let mut aligned: Vec<Align8> = Vec::with_capacity(n_units);
    let ptr = aligned.as_mut_ptr();
    mem::forget(aligned);

    unsafe { Vec::from_raw_parts(ptr as *mut u8, 0, size) }
}
