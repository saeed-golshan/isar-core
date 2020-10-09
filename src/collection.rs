use crate::error::{illegal_arg, Result};
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
use {crate::utils::debug::dump_db, hashbrown::HashMap};

pub struct IsarCollection {
    id: u16,
    object_info: ObjectInfo,
    indexes: Vec<Index>,
    db: Db,
    oidg: ObjectIdGenerator,
}

impl IsarCollection {
    pub(crate) fn new(id: u16, object_info: ObjectInfo, indexes: Vec<Index>, db: Db) -> Self {
        IsarCollection {
            id,
            object_info,
            indexes,
            db,
            oidg: ObjectIdGenerator::new(random()),
        }
    }

    pub fn get_object_builder(&self) -> ObjectBuilder {
        ObjectBuilder::new(&self.object_info)
    }

    pub fn get<'txn>(&self, txn: &'txn Txn, mut oid: ObjectId) -> Result<Option<&'txn [u8]>> {
        let oid_bytes = oid.as_bytes_with_prefix_padding(self.id);
        self.db.get(txn, &oid_bytes)
    }

    pub fn put(&self, txn: &Txn, oid: Option<ObjectId>, object: &[u8]) -> Result<ObjectId> {
        let mut oid = if let Some(oid) = oid {
            self.delete_from_indexes(txn, oid)?;
            oid
        } else {
            self.oidg.generate()
        };

        /*if !self.verify_object(object) {
            illegal_arg("Provided object is invalid.")?;
        }*/

        for index in &self.indexes {
            index.create_for_object(txn, oid, object)?;
        }

        let oid_bytes = oid.as_bytes_with_prefix_padding(self.id);
        self.db.put(txn, &oid_bytes, object)?;
        Ok(oid)
    }

    pub fn delete(&self, txn: &Txn, mut oid: ObjectId) -> Result<()> {
        if self.delete_from_indexes(txn, oid)? {
            let oid_bytes = oid.as_bytes_with_prefix_padding(self.id);
            self.db.delete(txn, &oid_bytes, None)?;
        }
        Ok(())
    }

    pub fn clear(&self, txn: &Txn) -> Result<()> {
        for index in &self.indexes {
            index.clear(txn)?;
        }
        self.db.delete_key_prefix(txn, &self.id.to_le_bytes())?;

        Ok(())
    }

    pub fn create_where_clause(&self, index_index: Option<usize>) -> Result<WhereClause> {
        if let Some(index_index) = index_index {
            if let Some(index) = self.indexes.get(index_index) {
                Ok(index.create_where_clause())
            } else {
                illegal_arg("Unknown index")
            }
        } else {
            Ok(WhereClause::new(&self.id.to_le_bytes(), IndexType::Primary))
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
    pub fn debug_dump(&self, txn: &Txn) -> HashMap<Vec<u8>, Vec<u8>> {
        dump_db(self.db, txn, Some(&self.id.to_le_bytes()))
            .into_iter()
            .map(|(key, val)| (key[2..key.len() - 2].to_vec(), val))
            .collect()
    }

    #[cfg(test)]
    pub fn debug_get_index(&self, index: usize) -> &Index {
        self.indexes.get(index).unwrap()
    }

    #[cfg(test)]
    pub fn debug_get_db(&self) -> Db {
        self.db
    }
}

#[cfg(test)]
mod tests {
    use crate::{col, isar, map};

    #[test]
    fn test_put_new() {
        isar!(isar, col => col!(field1 => Int));

        let txn = isar.begin_txn(true).unwrap();
        let object1 = 1111111i32.to_le_bytes();
        let oid1 = col.put(&txn, None, &object1).unwrap();

        let object2 = 22222i32.to_le_bytes();
        let oid2 = col.put(&txn, None, &object2).unwrap();

        let object3 = 333333333i32.to_le_bytes();
        let oid3 = col.put(&txn, None, &object3).unwrap();

        assert_eq!(
            col.debug_dump(&txn),
            map!(oid1.as_bytes() => object1, oid2.as_bytes() => object2, oid3.as_bytes() => object3)
        );
    }

    #[test]
    fn test_put_existing() {
        isar!(isar, col => col!(field1 => Int));

        let txn = isar.begin_txn(true).unwrap();

        let object = 12345i32.to_le_bytes();
        let oid = col.put(&txn, None, &object).unwrap();

        let object2 = 54321i32.to_le_bytes();
        let oid2 = col.put(&txn, Some(oid), &object2).unwrap();
        assert_eq!(oid, oid2);

        let new_oid = col.oidg.generate();
        let object3 = 99999i32.to_le_bytes();
        let oid3 = col.put(&txn, Some(new_oid), &object3).unwrap();
        assert_eq!(new_oid, oid3);

        assert_eq!(
            col.debug_dump(&txn),
            map!(oid.as_bytes() => object2, new_oid.as_bytes() => object3)
        );
    }

    #[test]
    fn test_put_creates_index() {
        isar!(isar, col => col!(field1 => Int index field1));

        let txn = isar.begin_txn(true).unwrap();

        let object = 12345i32.to_le_bytes();
        let oid = col.put(&txn, None, &object).unwrap();

        let index = &col.indexes[0];
        assert_eq!(
            index.debug_dump(&txn),
            map!(index.debug_create_key(&object) => oid.as_bytes())
        );
    }

    #[test]
    fn test_put_clears_old_index() {
        isar!(isar, col => col!(field1 => Int index field1));

        let txn = isar.begin_txn(true).unwrap();

        let object = 12345i32.to_le_bytes();
        let oid = col.put(&txn, None, &object).unwrap();

        let object2 = 54321i32.to_le_bytes();
        col.put(&txn, Some(oid), &object2).unwrap();

        let index = &col.indexes[0];
        assert_eq!(
            index.debug_dump(&txn),
            map!(index.debug_create_key(&object2) => oid.as_bytes())
        );
    }

    #[test]
    fn test_delete() {
        isar!(isar, col => col!(field1 => Int index field1));

        let txn = isar.begin_txn(true).unwrap();

        let object = 12345i32.to_le_bytes();
        let oid = col.put(&txn, None, &object).unwrap();

        let object2 = 54321i32.to_le_bytes();
        let oid2 = col.put(&txn, None, &object2).unwrap();

        col.delete(&txn, oid).unwrap();

        assert_eq!(col.debug_dump(&txn), map!(oid2.as_bytes() => object2));

        let index = &col.indexes[0];
        assert_eq!(
            index.debug_dump(&txn),
            map!(index.debug_create_key(&object2) => oid2.as_bytes())
        );
    }

    #[test]
    fn test_clear() {
        isar!(isar, col1 => col!(f1 => Int index f1), col2 => col!(f2 => Int index f2));

        let txn = isar.begin_txn(true).unwrap();

        let object1 = 12345i32.to_le_bytes();
        let object2 = 54321i32.to_le_bytes();
        col1.put(&txn, None, &object1).unwrap();
        col1.put(&txn, None, &object2).unwrap();
        let oid1 = col2.put(&txn, None, &object1).unwrap();
        let oid2 = col2.put(&txn, None, &object2).unwrap();

        col1.clear(&txn).unwrap();

        assert!(col1.debug_dump(&txn).is_empty());
        assert!(&col1.indexes[0].debug_dump(&txn).is_empty());

        assert_eq!(
            col2.debug_dump(&txn),
            map!(oid1.as_bytes() => &object1, oid2.as_bytes() => &object2)
        );

        let index2 = &col2.indexes[0];
        assert_eq!(
            index2.debug_dump(&txn),
            map!(
                index2.debug_create_key(&object1) => oid1.as_bytes(),
                index2.debug_create_key(&object2) => oid2.as_bytes()
            )
        );
    }
}
