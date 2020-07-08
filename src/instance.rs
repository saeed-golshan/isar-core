use crate::bank::IsarBank;
use crate::bank_manager::BankManager;
use crate::error::IsarError::VersionError;
use crate::error::*;
use crate::lmdb::db::Db;
use crate::lmdb::env::Env;
use crate::lmdb::txn::Txn;
use crate::schema::Schema;
use std::convert::TryInto;

pub const ISAR_VERSION: u32 = 1;

struct LmdbDbs {
    info: Db,
    schema: Db,
    data: Db,
    index: Db,
    index_dup: Db,
}

pub struct IsarInstance {
    env: Env,
    dbs: LmdbDbs,
    bank_manager: BankManager,
    path: String,
}

impl IsarInstance {
    pub fn create(path: &str, max_size: u32, schemas_json: &str) -> Result<Self> {
        let schemas = Schema::schemas_from_json(schemas_json)?;

        let env = Env::create(path, 5, max_size)?;
        let dbs = IsarInstance::open_databases(&env)?;

        let txn = env.txn(true)?;
        Self::migrate_isar_database(&txn, &dbs)?;
        txn.commit()?;

        let mut bank_manager = BankManager::new(dbs.schema, dbs.data, dbs.index, dbs.index_dup);

        let txn = env.txn(true)?;
        bank_manager.init(&txn, schemas)?;
        txn.commit()?;

        Ok(IsarInstance {
            env,
            dbs,
            bank_manager,
            path: path.to_string(),
        })
    }

    fn open_databases(env: &Env) -> Result<LmdbDbs> {
        let txn = env.txn(true)?;
        let info = Db::open(&txn, "info", false, false, false)?;
        let schema = Db::open(&txn, "schema", false, false, false)?;
        let data = Db::open(&txn, "data", true, false, false)?;
        let index = Db::open(&txn, "index", false, false, true)?;
        let index_dup = Db::open(&txn, "index_dup", false, true, true)?;
        txn.commit()?;
        Ok(LmdbDbs {
            info,
            schema,
            data,
            index,
            index_dup,
        })
    }

    fn migrate_isar_database(txn: &Txn, dbs: &LmdbDbs) -> Result<()> {
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

    pub fn get_bank(&self, bank_index: usize) -> Option<&IsarBank> {
        self.bank_manager.get_bank(bank_index)
    }
}
