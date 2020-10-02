use crate::collection::IsarCollection;
use crate::data_dbs::DataDbs;
use crate::error::IsarError::VersionError;
use crate::error::*;
use crate::lmdb::db::Db;
use crate::lmdb::env::Env;
use crate::lmdb::txn::Txn;
use crate::schema::schema::Schema;
use std::convert::TryInto;

pub const ISAR_VERSION: u32 = 1;

pub struct IsarInstance {
    env: Env,
    info_db: Db,
    collections: Vec<IsarCollection>,
    path: String,
}

impl IsarInstance {
    pub fn create(path: &str, max_size: u32, schema_json: &str) -> Result<Self> {
        let schema = Schema::schema_from_json(schema_json)?;

        let env = Env::create(path, 5, max_size)?;
        let (info_db, data_dbs) = IsarInstance::open_databases(&env)?;

        let txn = env.txn(true)?;
        Self::migrate_isar_database(&txn, info_db, data_dbs)?;
        txn.commit()?;

        let collections = vec![];

        Ok(IsarInstance {
            env,
            info_db,
            collections,
            path: path.to_string(),
        })
    }

    fn open_databases(env: &Env) -> Result<(Db, DataDbs)> {
        let txn = env.txn(true)?;
        let info = Db::open(&txn, "info", false, false, false)?;
        let primary = Db::open(&txn, "data", true, false, false)?;
        let secondary = Db::open(&txn, "index", false, false, true)?;
        let secondary_dup = Db::open(&txn, "index_dup", false, true, true)?;
        let links = Db::open(&txn, "links", true, true, true)?;
        txn.commit()?;
        Ok((
            info,
            DataDbs {
                primary,
                secondary,
                secondary_dup,
                links,
            },
        ))
    }

    fn migrate_isar_database(txn: &Txn, info_db: Db, data_dbs: DataDbs) -> Result<()> {
        let version = info_db.get(&txn, b"version")?;
        if let Some(version) = version {
            let version_number = u32::from_le_bytes(version.try_into().unwrap());
            if version_number != ISAR_VERSION {
                return Err(VersionError {
                    message: "Database has an illegal version number.".to_string(),
                });
            }
        } else {
            info_db.put(&txn, b"version", &u32::to_le_bytes(ISAR_VERSION))?;
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
