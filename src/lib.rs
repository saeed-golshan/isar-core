#![allow(clippy::new_without_default)]

#[cfg(not(target_endian = "little"))]
compile_error!("Only little endian systems are supported at this time.");

pub mod collection;
pub mod data_dbs;
pub mod error;
pub mod index;
pub mod instance;
pub mod lmdb;
pub mod object;
pub mod query;
pub mod schema;
pub mod txn;
pub mod utils;
