#![cfg(test)]

use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use std::collections::HashMap;

#[macro_export]
macro_rules! map (
    ($($key:expr => $value:expr),+) => {
        #[allow(clippy::useless_vec)]
        {
            let mut m = ::std::collections::HashMap::new();
            $(m.insert($key.to_vec(), $value.to_vec());)+
            m
        }
    };
);

#[macro_export]
macro_rules! isar (
    ($isar:ident, $($col:ident => $schema:expr),+) => {
        let mut schema = crate::schema::Schema::new();
        $(
        let col = $schema;
        schema.add_collection(col).unwrap();
        )+
        let temp = tempfile::tempdir().unwrap();
        let $isar = crate::instance::IsarInstance::create(temp.path().to_str().unwrap(), 10000000, schema).unwrap();
        isar!(x $isar, 0, $($col),+);
    };

    (x $isar:ident, $index:expr, $col:ident, $($other:ident),+) => {
        let $col = $isar.get_collection($index).unwrap();
        isar!(x $isar, $index + 1, $($other),*)
    };

    (x $isar:ident, $index:expr, $col:ident) => {
        let $col = $isar.get_collection($index).unwrap();
    };
);

#[macro_export]
macro_rules! col (
    ($($name:expr => $type:ident),+) => {
        col!($($name => $type),+ index);
    };

    ($($name:expr => $type:ident),+ index $($($index:expr),+);*) => {
        {
            let mut collection = crate::schema::collection_schema::CollectionSchema::new(stringify!($($name)+));
            $(collection.add_property(stringify!($name), crate::object::property::DataType::$type).unwrap();)+
            $(collection.add_index(&[$(stringify!($index)),+], false, false).unwrap();)*
            collection
        }
    };
);

pub fn dump_db(db: Db, txn: &Txn, prefix: Option<&[u8]>) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut map = HashMap::new();
    let cursor = db.cursor(&txn).unwrap();

    if let Some(prefix) = prefix {
        cursor.move_to_key_greater_than_or_equal_to(prefix).unwrap();
    } else {
        cursor.move_to_first().unwrap();
    }

    for kv in cursor.iter() {
        let (key, val) = kv.unwrap();
        if prefix.is_some() && !key.starts_with(prefix.unwrap()) {
            println!("ERROR!");
            break;
        }
        map.insert(key.to_vec(), val.to_vec());
    }
    map
}
