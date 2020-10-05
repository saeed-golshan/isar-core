mod common;

use crate::common::*;
use isar_core::collection::IsarCollection;
use isar_core::instance::IsarInstance;
use isar_core::object::property::{DataType, Property};
use isar_core::schema::collection_schema::CollectionSchema;
use isar_core::schema::index_schema::IndexSchema;
use isar_core::schema::property_schema::PropertySchema;
use isar_core::schema::Schema;
use serde_json::error::Category::Data;
use tempfile::tempdir;

#[test]
fn test_get() {
    let col = get_isar(get_person_schema()).get_collection(0).unwrap();
}
