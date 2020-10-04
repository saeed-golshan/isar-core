#![feature(backtrace)]
#![feature(is_sorted)]
#![feature(trait_alias)]
#![allow(clippy::try_err)]

pub mod collection;
#[macro_use]
pub mod error;
//pub mod collection_manager;
pub mod data_dbs;
pub mod ffi;
mod field;
mod index;
pub mod instance;
mod link;
pub mod lmdb;
pub mod query;
pub mod schema;
//pub mod schema_diff;
mod object_id;
mod object_set;
pub mod utils;
