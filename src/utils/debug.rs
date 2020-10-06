use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use std::collections::HashMap;

#[macro_export]
macro_rules! map (
    ($($key:expr => $value:expr),+) => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(m.insert($key, $value);)+
            m
        }
     };
);

#[macro_export]
macro_rules! create_col (
    ($isar:ident, $col:ident, $($name:expr => $type:ident),+) => {
        create_col!($isar, $col, $($name => $type),+ index);
    };

    ($isar:ident, $col:ident, $($name:expr => $type:ident),+ index $($($index:expr),+);*) => {
        let mut schema = crate::schema::Schema::new();
        let mut collection = crate::schema::collection_schema::CollectionSchema::new("test");

        $(collection.add_property(stringify!($name), crate::object::property::DataType::$type).unwrap();)+
        $(collection.add_index(&[$(stringify!($index)),+], false, false).unwrap();)*

        schema.add_collection(collection).unwrap();

        let temp = tempfile::tempdir().unwrap();
        let $isar = crate::instance::IsarInstance::create(temp.path().to_str().unwrap(), 10000000, schema).unwrap();
        let $col = $isar.get_collection(0).unwrap();
    };
);

pub fn dump_db(db: Db, txn: &Txn) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut map = HashMap::new();
    for kv in db.cursor(&txn).unwrap().iter_from_first() {
        let (key, val) = kv.unwrap();
        map.insert(key.to_vec(), val.to_vec());
    }
    map
}
