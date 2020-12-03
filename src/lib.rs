#![feature(backtrace)]
#![feature(stmt_expr_attributes)]
#![recursion_limit = "256"]

#[cfg(not(target_endian = "little"))]
compile_error!("Only little endian systems are supported at this time.");

pub mod collection;
pub mod error;
pub mod utils;
//pub mod collection_manager;
pub mod data_dbs;
pub mod ffi;
pub mod index;
pub mod instance;
//mod link;
pub mod lmdb;
pub mod object;
pub mod query;
pub mod schema;
