use isar_core::collection::IsarCollection;
use isar_core::instance::IsarInstance;
use isar_core::lmdb::db::Db;
use isar_core::lmdb::env::Env;
use isar_core::object::property::DataType;
use isar_core::schema::collection_schema::CollectionSchema;
use isar_core::schema::Schema;
use tempfile::tempdir;

pub fn get_person_schema() -> Schema {
    let mut schema = Schema::new();
    let mut collection = CollectionSchema::new("persons");
    collection.add_property("age", DataType::Int);
    collection.add_property("name", DataType::String);
    collection.add_property("friends", DataType::StringList);
    collection.add_index(&vec!["age"], false, false);

    schema.add_collection(collection);
    schema
}

pub fn get_isar(schema: Schema) -> IsarInstance {
    let temp = tempdir().unwrap();
    IsarInstance::create(temp.path().to_str().unwrap(), 1000000, schema).unwrap()
}
