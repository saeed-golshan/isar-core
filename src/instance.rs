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

    pub fn create_query_builder(&self, collection: &IsarCollection) -> QueryBuilder {
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
