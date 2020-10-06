use crate::error::Result;
use crate::index::{Index, IndexType};
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::object::object_builder::ObjectBuilder;
use crate::object::object_id::ObjectId;
use crate::object::object_id_generator::ObjectIdGenerator;
use crate::object::object_info::ObjectInfo;
use crate::query::where_clause::WhereClause;
use rand::random;

#[cfg(test)]
use {crate::utils::debug::dump_db, std::collections::HashMap};

pub struct IsarCollection {
    prefix: [u8; 2],
    object_info: ObjectInfo,
    indexes: Vec<Index>,
    db: Db,
    oidg: ObjectIdGenerator,
}

impl IsarCollection {
    pub(crate) fn new(id: u16, object_info: ObjectInfo, indexes: Vec<Index>, db: Db) -> Self {
        IsarCollection {
            prefix: u16::to_le_bytes(id),
            object_info,
            indexes,
            db,
            oidg: ObjectIdGenerator::new(random()),
        }
    }

    pub fn get_object_builder(&self) -> ObjectBuilder {
        ObjectBuilder::new(&self.object_info)
    }

    pub fn get<'txn>(&self, txn: &'txn Txn, oid: ObjectId) -> Result<Option<&'txn [u8]>> {
        let oid_bytes = oid.to_bytes_with_prefix(&self.prefix);
        self.db.get(txn, &oid_bytes)
    }

    pub fn put(&self, txn: &Txn, oid: Option<ObjectId>, object: &[u8]) -> Result<ObjectId> {
        let oid = if let Some(oid) = oid {
            self.delete_from_indexes(txn, oid)?;
            oid
        } else {
            self.oidg.generate()
        };

        /*if !self.verify_object(object) {
            illegal_arg("Provided object is invalid.")?;
        }*/

        let oid_bytes = oid.to_bytes_with_prefix(&self.prefix);

        for index in &self.indexes {
            index.create_for_object(txn, oid, object)?;
        }

        self.db.put(txn, &oid_bytes, object)?;
        Ok(oid)
    }

    pub fn delete(&self, txn: &Txn, oid: ObjectId) -> Result<()> {
        if self.delete_from_indexes(txn, oid)? {
            let oid_bytes = oid.to_bytes_with_prefix(&self.prefix);
            self.db.delete(txn, &oid_bytes, None)?;
        }
        Ok(())
    }

    pub fn clear(&self, txn: &Txn) -> Result<()> {
        for index in &self.indexes {
            index.clear(txn)?;
        }
        let cursor = self.db.cursor(txn)?;
        cursor.delete_key_prefix(&self.prefix)?;

        Ok(())
    }

    pub fn create_where_clause(&self, index_index: usize) -> WhereClause {
        if let Some(index) = self.indexes.get(index_index) {
            index.create_where_clause()
        } else {
            WhereClause::new(&self.prefix, IndexType::Primary)
        }
    }

    fn delete_from_indexes(&self, txn: &Txn, oid: ObjectId) -> Result<bool> {
        let existing_object = self.get(txn, oid)?;
        if let Some(existing_object) = existing_object {
            for index in &self.indexes {
                index.delete_for_object(txn, oid, existing_object)?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[cfg(test)]
    fn debug_dump(&self, txn: &Txn) -> HashMap<Vec<u8>, Vec<u8>> {
        dump_db(self.db, txn)
    }
}

#[cfg(test)]

mod test {
    use crate::{create_col, map};

    #[test]
    fn test_put_single() {
        create_col!(isar, col, field1 => Int);

        let txn = isar.begin_txn(true).unwrap();
        let object = 12345i32.to_le_bytes().to_vec();
        let oid = col.put(&txn, None, &object).unwrap();

        let oid_with_prefix = oid.to_bytes_with_prefix(&col.prefix);
        assert_eq!(col.debug_dump(&txn), map!(oid_with_prefix => object));
    }

    #[test]
    fn test_put_existing() {
        create_col!(isar, col, field1 => Int);

        let txn = isar.begin_txn(true).unwrap();

        let object = 12345i32.to_le_bytes().to_vec();
        let oid = col.put(&txn, None, &object).unwrap();

        let object2 = 54321i32.to_le_bytes().to_vec();
        let oid2 = col.put(&txn, Some(oid), &object2).unwrap();
        assert_eq!(oid, oid2);

        let oid_with_prefix = oid.to_bytes_with_prefix(&col.prefix);
        assert_eq!(col.debug_dump(&txn), map!(oid_with_prefix => object2));
    }

    #[test]
    fn test_put_creates_index() {
        create_col!(isar, col, field1 => Int index field1);

        let txn = isar.begin_txn(true).unwrap();

        let object = 12345i32.to_le_bytes().to_vec();
        let oid = col.put(&txn, None, &object).unwrap();

        let index = &col.indexes[0];
        let id = index.debug_dump(&txn);
        println!("{:?}", id);
        assert_eq!(
            index.debug_dump(&txn),
            map!(index.debug_create_key(&object) => oid.to_bytes().to_vec())
        );
    }
}
