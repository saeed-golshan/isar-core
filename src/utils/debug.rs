#![cfg(test)]

use crate::collection::IsarCollection;
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::object::object_id::ObjectId;
use hashbrown::HashMap;
use std::hash::Hash;

#[macro_export]
macro_rules! map (
    ($($key:expr => $value:expr),+) => {
        #[allow(clippy::useless_vec)]
        {
            let mut m = ::hashbrown::HashMap::new();
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
            $(collection.add_property(stringify!($name), crate::object::data_type::DataType::$type).unwrap();)+
            $(collection.add_index(&[$(stringify!($index)),+], false, false).unwrap();)*
            collection
        }
    };
);

pub fn fill_db<'a>(
    col: &IsarCollection,
    txn: &'a Txn,
    data: &'a [(Option<ObjectId>, Vec<u8>)],
) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut result = HashMap::new();
    for (oid, object) in data {
        let oid = col.put(&txn, *oid, object).unwrap();
        result.insert(oid.as_bytes().to_vec(), object.to_vec());
    }
    result
}

pub fn ref_map<K: Eq + Hash, V>(map: &HashMap<K, V>) -> HashMap<&K, &V> {
    map.iter().map(|(k, v)| (k, v)).collect()
}

pub fn dump_db(db: Db, txn: &Txn, prefix: Option<&[u8]>) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut map = HashMap::new();
    let mut cursor = db.cursor(&txn).unwrap();

    if let Some(prefix) = prefix {
        cursor.move_to_key_greater_than_or_equal_to(prefix).unwrap();
    } else {
        cursor.move_to_first().unwrap();
    }

    for kv in cursor.iter() {
        let (key, val) = kv.unwrap();
        if prefix.is_some() && !key.starts_with(prefix.unwrap()) {
            break;
        }
        map.insert(key.to_vec(), val.to_vec());
    }
    map
}
