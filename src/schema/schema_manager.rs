use crate::collection::IsarCollection;
use crate::data_dbs::DataDbs;
use crate::error::{IsarError, Result};
use crate::lmdb::env::Env;
use crate::lmdb::txn::Txn;
use crate::schema::collection_migrator::CollectionMigrator;
use crate::schema::Schema;
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Serializer};
use std::convert::TryInto;

const ISAR_VERSION: u64 = 1;
const INFO_VERSION_KEY: &[u8] = b"version";
const INFO_SCHEMA_KEY: &[u8] = b"schema";

pub struct SchemaManger<'env> {
    env: &'env Env,
    dbs: DataDbs,
}

impl<'env> SchemaManger<'env> {
    pub fn new(env: &'env Env, dbs: DataDbs) -> Self {
        SchemaManger { env, dbs }
    }

    pub fn check_isar_version(&self) -> Result<()> {
        let txn = self.env.txn(true)?;
        let version = self.dbs.info.get(&txn, INFO_VERSION_KEY)?;
        if let Some(version) = version {
            let version_num = u64::from_le_bytes(version.try_into().unwrap());
            if version_num != ISAR_VERSION {
                return Err(IsarError::VersionError {});
            }
        } else {
            let version_bytes = &ISAR_VERSION.to_le_bytes();
            self.dbs.info.put(&txn, INFO_VERSION_KEY, version_bytes)?;
        }
        txn.abort();
        Ok(())
    }

    pub fn get_collections(&self, mut schema: Schema) -> Result<Vec<IsarCollection>> {
        let txn = self.env.txn(true)?;
        let existing_schema_bytes = self.dbs.info.get(&txn, INFO_SCHEMA_KEY)?;

        let existing_collections = if let Some(existing_schema_bytes) = existing_schema_bytes {
            let mut deser = Deserializer::from_slice(existing_schema_bytes);
            let existing_schema =
                Schema::deserialize(&mut deser).map_err(|e| IsarError::DbCorrupted {
                    source: Some(Box::new(e)),
                    message: "Could not deserialize existing schema.".to_string(),
                })?;
            schema.update_with_existing_schema(Some(&existing_schema));
            existing_schema.build_collections(self.dbs)
        } else {
            schema.update_with_existing_schema(None);
            vec![]
        };

        self.save_schema(&txn, &schema)?;
        let collections = schema.build_collections(self.dbs);
        self.perform_migration(&txn, &collections, &existing_collections)?;

        txn.commit()?;

        Ok(collections)
    }

    fn save_schema(&self, txn: &Txn, schema: &Schema) -> Result<()> {
        let mut bytes = vec![];
        let mut ser = Serializer::new(&mut bytes);
        schema
            .serialize(&mut ser)
            .map_err(|e| IsarError::MigrationError {
                source: Some(Box::new(e)),
                message: "Could not serialize schema.".to_string(),
            })?;
        self.dbs.info.put(txn, INFO_SCHEMA_KEY, &bytes)?;
        Ok(())
    }

    fn perform_migration(
        &self,
        txn: &Txn,
        collections: &[IsarCollection],
        existing_collections: &[IsarCollection],
    ) -> Result<()> {
        let removed_collections = existing_collections
            .iter()
            .filter(|existing| !collections.iter().any(|c| existing.get_id() == c.get_id()));

        for col in removed_collections {
            col.delete_all_internal(txn)?;
        }

        for col in collections {
            let existing = existing_collections
                .iter()
                .find(|existing| existing.get_id() == col.get_id());

            if let Some(existing) = existing {
                let migrator = CollectionMigrator::create(col, existing);
                migrator.migrate(txn, self.dbs.primary)?;
            }
        }

        Ok(())
    }
}
