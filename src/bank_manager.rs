use crate::bank::IsarBank;
use crate::error::IsarError::DbCorrupted;
use crate::error::{corrupted, Result};
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::schema::Schema;
use rand::random;
use std::collections::HashMap;
use std::convert::TryInto;

pub struct BankManager {
    schema_db: Db,
    data_db: Db,
    index_db: Db,
    index_dup_db: Db,
    banks: Vec<IsarBank>,
}

impl BankManager {
    pub fn new(schema_db: Db, data_db: Db, index_db: Db, index_dup_db: Db) -> Self {
        BankManager {
            schema_db,
            data_db,
            index_db,
            index_dup_db,
            banks: vec![],
        }
    }

    pub fn init(&mut self, txn: &Txn, mut schemas: Vec<Schema>) -> Result<()> {
        let existing_schemas = self.load_schemas(txn)?;
        for new_schema in &mut schemas {
            let existing = existing_schemas
                .iter()
                .find(|(_, s)| s.bank_name == new_schema.bank_name);

            if let Some((bank_id, existing_schema)) = existing {
                eprintln!(
                    "Bank {} ({}) already exists.",
                    new_schema.bank_name, bank_id
                );

                if existing_schema != new_schema {
                    eprintln!("Bank {} needs migration.", new_schema.bank_name);
                    //migrate
                }

                let bank =
                    new_schema.to_bank(*bank_id, self.data_db, self.index_db, self.index_dup_db);
                self.banks.push(bank);
            } else {
                new_schema.update_index_ids(None);
                self.put_schema(txn, new_schema)?;
            }
        }

        let unneeded_schemas = existing_schemas
            .iter()
            .filter(|(_, s)| !schemas.iter().any(|new| new.bank_name == s.bank_name));
        for (bank_id, unneeded_schema) in unneeded_schemas {
            let unneeded_bank =
                unneeded_schema.to_bank(*bank_id, self.data_db, self.index_db, self.index_dup_db);
            unneeded_bank.clear(txn)?;
            eprintln!(
                "Bank {} is no longer needed and has been deleted.",
                unneeded_bank.name
            );
        }
        Ok(())
    }

    fn load_schemas(&self, txn: &Txn) -> Result<Vec<(u16, Schema)>> {
        let cursor = self.schema_db.cursor(txn)?;
        let mut schemas = vec![];
        for item in cursor.iter_from_first() {
            let (id_bytes, schema_bytes) = item?;
            eprintln!("{:?}", id_bytes);
            let id = u16::from_le_bytes(id_bytes.try_into().unwrap());
            let schema_str = std::str::from_utf8(schema_bytes).map_err(|e| DbCorrupted {
                source: Some(Box::new(e)),
                message: "Could not load schemas from db.".to_string(),
            })?;
            let schema = serde_json::from_str(schema_str).map_err(|e| DbCorrupted {
                source: Some(Box::new(e)),
                message: "Could not parse schema.".to_string(),
            })?;
            schemas.push((id, schema))
        }
        Ok(schemas)
    }

    fn put_schema(&mut self, txn: &Txn, schema: &Schema) -> Result<()> {
        let bank_id = self.find_free_bank_id();

        let id_bytes = u16::to_le_bytes(bank_id);
        let schema_str = serde_json::to_string(schema).unwrap();
        self.schema_db.put(txn, &id_bytes, schema_str.as_bytes())?;

        let bank = schema.to_bank(bank_id, self.data_db, self.index_db, self.index_dup_db);
        eprintln!("Bank {} {} has been created.", bank.name, bank.id);
        self.banks.push(bank);

        Ok(())
    }

    fn find_free_bank_id(&self) -> u16 {
        let mut id = 0u16;
        loop {
            id = random();
            if !self.banks.iter().any(|b| b.id == id) {
                break;
            }
        }
        id
    }

    pub fn get_bank(&self, bank_index: usize) -> Option<&IsarBank> {
        self.banks.get(bank_index)
    }
}
