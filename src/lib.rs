#![feature(backtrace)]
#![feature(is_sorted)]
#![feature(trait_alias)]
#![allow(clippy::try_err)]

#[cfg(not(target_endian = "little"))]
compile_error!("Only little endian systems are supported at this time.");

pub mod collection;
pub mod error;
pub mod utils;
//pub mod collection_manager;
pub mod data_dbs;
pub mod ffi;
mod index;
pub mod instance;
//mod link;
pub mod lmdb;
pub mod query;
pub mod schema;
//pub mod schema_diff;
pub mod object;
