use crate::collection::IsarCollection;
use crate::data_dbs::DataDbs;
use crate::error::*;
use crate::lmdb::db::Db;
use crate::lmdb::env::Env;
use crate::query::query_builder::QueryBuilder;
use crate::schema::schema_manager::SchemaManger;
use crate::schema::Schema;
use crate::txn::IsarTxn;

pub struct IsarInstance {
    env: Env,
    dbs: DataDbs,
    collections: Vec<IsarCollection>,
}

impl IsarInstance {
    pub fn create(path: &str, max_size: usize, schema: Schema) -> Result<Self> {
        let env = Env::create(path, 4, max_size)?;
        let dbs = IsarInstance::open_databases(&env)?;

        let manager = SchemaManger::new(&env, dbs);
        manager.check_isar_version()?;
        let collections = manager.get_collections(schema)?;

        Ok(IsarInstance {
            env,
            dbs,
            collections,
        })
    }

    fn open_databases(env: &Env) -> Result<DataDbs> {
        let txn = env.txn(true)?;
        let info = Db::open(&txn, "info", false, false)?;
        let primary = Db::open(&txn, "data", false, false)?;
        let secondary = Db::open(&txn, "index", false, true)?;
        let secondary_dup = Db::open(&txn, "index_dup", true, true)?;
        txn.commit()?;
        Ok(DataDbs {
            info,
            primary,
            secondary,
            secondary_dup,
        })
    }

    #[inline]
    pub fn begin_txn(&self, write: bool) -> Result<IsarTxn> {
        Ok(IsarTxn::new(self.env.txn(write)?, write))
    }

    pub fn get_collection(&self, collection_index: usize) -> Option<&IsarCollection> {
        self.collections.get(collection_index)
    }

    pub fn get_collection_by_name(&self, collection_name: &str) -> Option<&IsarCollection> {
        self.collections
            .iter()
            .find(|c| c.get_name() == collection_name)
    }

    pub fn create_query_builder<'col>(
        &self,
        collection: &'col IsarCollection,
    ) -> QueryBuilder<'col> {
        QueryBuilder::new(
            collection,
            self.dbs.primary,
            self.dbs.secondary,
            self.dbs.secondary_dup,
        )
    }

    pub fn close(self) {}

    #[cfg(test)]
    pub fn debug_get_primary_db(&self) -> Db {
        self.dbs.primary
    }

    #[cfg(test)]
    pub fn debug_get_secondary_db(&self) -> Db {
        self.dbs.secondary
    }

    #[cfg(test)]
    pub fn debug_get_secondary_dup_db(&self) -> Db {
        self.dbs.secondary_dup
    }
}

#[cfg(test)]
mod tests {
    use crate::{col, isar};
    use tempfile::tempdir;

    #[test]
    fn test_open_new_instance() {
        isar!(isar, col => col!(f1 => Int));

        let mut ob = col.get_object_builder();
        ob.write_int(123);
        let o = ob.finish();

        let txn = isar.begin_txn(true).unwrap();
        let oid = col.put(&txn, None, o.as_bytes()).unwrap();
        txn.commit().unwrap();

        let txn = isar.begin_txn(false).unwrap();
        assert_eq!(col.get(&txn, oid).unwrap().unwrap(), o.as_bytes());
        txn.abort();
    }

    #[test]
    fn test_open_instance_added_collection() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let (oid, object) = {
            isar!(path: path, isar, col1 => col!("col1", f1 => Int));

            let mut ob = col1.get_object_builder();
            ob.write_int(123);
            let o = ob.finish();

            let txn = isar.begin_txn(true).unwrap();
            let oid = col1.put(&txn, None, o.as_bytes()).unwrap();
            txn.commit().unwrap();

            (oid, o.as_bytes().to_vec())
        };

        isar!(path: path, isar, col1 => col!("col1", f1 => Int), col2 => col!("col2", f1 => Int));

        let txn = isar.begin_txn(false).unwrap();
        assert_eq!(col1.get(&txn, oid).unwrap().unwrap().to_vec(), object);
        assert_eq!(
            isar.create_query_builder(col2).build().count(&txn).unwrap(),
            0
        );
        txn.abort();
    }

    #[test]
    fn test_open_instance_removed_collection() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        {
            isar!(path: path, isar, col1 => col!("col1", f1 => Int), _col2 => col!("col2", f1 => Int));

            let mut ob = col1.get_object_builder();
            ob.write_int(123);
            let o = ob.finish();

            let txn = isar.begin_txn(true).unwrap();
            //col1.put(&txn, None, o.as_bytes()).unwrap();
            col1.put(&txn, None, o.as_bytes()).unwrap();
            txn.commit().unwrap();
        };

        {
            isar!(path: path, isar, _col2 => col!("col2", f1 => Int));
        }

        isar!(path: path, isar, col1 => col!("col1", f1 => Int), _col2 => col!("col2", f1 => Int));

        let txn = isar.begin_txn(false).unwrap();
        assert_eq!(
            isar.create_query_builder(col1).build().count(&txn).unwrap(),
            0
        );
        txn.abort();
    }
}
