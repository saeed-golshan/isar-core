use crate::error::{illegal_state, Result};
use crate::index::{Index, IndexType};
use crate::link::Link;
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::object::object_id::{ObjectId, ObjectIdGenerator};
use crate::object::object_info::ObjectInfo;
use crate::query::where_clause::WhereClause;
use rand::random;

pub struct IsarCollection {
    prefix: [u8; 2],
    object_info: ObjectInfo,
    links: Vec<Link>,
    indexes: Vec<Index>,
    db: Db,
    oidg: ObjectIdGenerator,
}

impl IsarCollection {
    pub fn new(
        id: u16,
        object_info: ObjectInfo,
        links: Vec<Link>,
        indexes: Vec<Index>,
        db: Db,
    ) -> Self {
        IsarCollection {
            prefix: u16::to_le_bytes(id),
            object_info,
            links,
            indexes,
            db,
            oidg: ObjectIdGenerator::new(random()),
        }
    }

    pub fn get_object_info(&self) -> &ObjectInfo {
        &self.object_info
    }

    pub fn get<'txn>(&self, txn: &'txn Txn, oid: ObjectId) -> Result<Option<&'txn [u8]>> {
        let oid_bytes = oid.to_bytes_with_prefix(&self.prefix);
        self.db.get(txn, &oid_bytes)
    }

    pub fn put(&mut self, txn: &Txn, oid: Option<ObjectId>, object: &[u8]) -> Result<ObjectId> {
        let oid = if let Some(oid) = oid {
            if !self.delete_from_indexes(txn, oid)? {
                illegal_state("ObjectId provided but no entry found.")?;
            }
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
}
