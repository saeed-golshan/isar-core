#![feature(backtrace)]
#![feature(is_sorted)]
#![feature(trait_alias)]

pub mod collection;
#[macro_use]
pub mod error;
//pub mod collection_manager;
pub mod data_dbs;
pub mod ffi;
pub mod field;
pub mod index;
pub mod instance;
pub mod link;
pub mod lmdb;
pub mod object_id;
pub mod object_set;
pub mod query;
pub mod schema;
//pub mod schema_diff;
pub mod utils;
