use crate::bank::IsarBank;
use crate::bank_manager::BankManager;
use crate::data_dbs::DataDbs;
use crate::error::IsarError::VersionError;
use crate::error::*;
use crate::lmdb::db::Db;
use crate::lmdb::env::Env;
use crate::lmdb::txn::Txn;
use crate::schema::Schema;
use std::convert::TryInto;

pub const ISAR_VERSION: u32 = 1;

pub struct IsarInstance {
    env: Env,
    info_db: Db,
    bank_manager: BankManager,
    path: String,
}

impl IsarInstance {
    pub fn create(path: &str, max_size: u32, schemas_json: &str) -> Result<Self> {
        let schemas = Schema::schemas_from_json(schemas_json)?;

        let env = Env::create(path, 5, max_size)?;
        let (info_db, schema_db, data_dbs) = IsarInstance::open_databases(&env)?;

        let txn = env.txn(true)?;
        Self::migrate_isar_database(&txn, info_db, schema_db, data_dbs)?;
        txn.commit()?;

        let mut bank_manager = BankManager::new(schema_db, data_dbs);

        let txn = env.txn(true)?;
        bank_manager.init(&txn, schemas)?;
        txn.commit()?;

        Ok(IsarInstance {
            env,
            info_db,
            bank_manager,
            path: path.to_string(),
        })
    }

    fn open_databases(env: &Env) -> Result<(Db, Db, DataDbs)> {
        let txn = env.txn(true)?;
        let info = Db::open(&txn, "info", false, false, false)?;
        let schema = Db::open(&txn, "schema", false, false, false)?;
        let primary = Db::open(&txn, "data", true, false, false)?;
        let secondary = Db::open(&txn, "index", false, false, true)?;
        let secondary_dup = Db::open(&txn, "index_dup", false, true, true)?;
        txn.commit()?;
        Ok((
            info,
            schema,
            DataDbs {
                primary,
                secondary,
                secondary_dup,
            },
        ))
    }

    fn migrate_isar_database(
        txn: &Txn,
        info_db: Db,
        schema_db: Db,
        data_dbs: DataDbs,
    ) -> Result<()> {
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

    pub fn get_bank(&self, bank_index: usize) -> Option<&IsarBank> {
        self.bank_manager.get_bank(bank_index)
    }
}
