use crate::collection::IsarCollection;
use crate::data_dbs::DataDbs;
use crate::error::IsarError::VersionError;
use crate::error::*;
use crate::lmdb::db::Db;
use crate::lmdb::env::Env;
use crate::lmdb::txn::Txn;
use std::convert::TryInto;

pub const ISAR_VERSION: u32 = 1;

pub struct IsarInstance {
    env: Env,
    dbs: DataDbs,
    collections: Vec<IsarCollection>,
    path: String,
}

impl IsarInstance {
    pub fn create(path: &str, max_size: u32, _schema_json: &str) -> Result<Self> {
        //let schema = Schema::schema_from_json(schema_json)?;

        let env = Env::create(path, 5, max_size)?;
        let dbs = IsarInstance::open_databases(&env)?;

        let txn = env.txn(true)?;
        Self::migrate_isar_database(&txn, dbs)?;
        txn.commit()?;

        let collections = vec![];

        Ok(IsarInstance {
            env,
            dbs,
            collections,
            path: path.to_string(),
        })
    }

    fn open_databases(env: &Env) -> Result<DataDbs> {
        let txn = env.txn(true)?;
        let info = Db::open(&txn, "info", false, false, false)?;
        let primary = Db::open(&txn, "data", true, false, false)?;
        let secondary = Db::open(&txn, "index", false, false, true)?;
        let secondary_dup = Db::open(&txn, "index_dup", false, true, true)?;
        let links = Db::open(&txn, "links", true, true, true)?;
        txn.commit()?;
        Ok(DataDbs {
            info,
            primary,
            secondary,
            secondary_dup,
            links,
        })
    }

    //#[cfg(test)]
    pub fn open_databases_debug(env: &Env) -> Result<DataDbs> {
        Self::open_databases(env)
    }

    fn migrate_isar_database(txn: &Txn, dbs: DataDbs) -> Result<()> {
        let version = dbs.info.get(&txn, b"version")?;
        if let Some(version) = version {
            let version_number = u32::from_le_bytes(version.try_into().unwrap());
            if version_number != ISAR_VERSION {
                return Err(VersionError {
                    message: "Database has an illegal version number.".to_string(),
                });
            }
        } else {
            dbs.info
                .put(&txn, b"version", &u32::to_le_bytes(ISAR_VERSION))?;
        }
        Ok(())
    }

    #[inline]
    pub fn begin_txn(&self, write: bool) -> Result<Txn> {
        self.env.txn(write)
    }

    pub fn get_collection(&self, collection_index: usize) -> Option<&IsarCollection> {
        self.collections.get(collection_index)
    }
}
