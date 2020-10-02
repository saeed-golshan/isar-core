use crate::collection::IsarCollection;
use crate::data_dbs::DataDbs;
use crate::error::IsarError::DbCorrupted;
use crate::error::Result;
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::schema::Schema;
use rand::random;
use std::convert::TryInto;

pub struct CollectionManager {
    schema_db: Db,
    data_dbs: DataDbs,
    collections: Vec<IsarCollection>,
}

impl CollectionManager {
    pub fn new(schema_db: Db, data_dbs: DataDbs) -> Self {
        CollectionManager {
            schema_db,
            data_dbs,
            collections: vec![],
        }
    }

    pub fn init(&mut self, txn: &Txn, mut schemas: Vec<Schema>) -> Result<()> {
        let existing_schemas = self.load_schemas(txn)?;
        for new_schema in &mut schemas {
            let existing = existing_schemas
                .iter()
                .find(|(_, s)| s.collection_name == new_schema.collection_name);

            if let Some((collection_id, existing_schema)) = existing {
                eprintln!(
                    "Collection {} ({}) already exists.",
                    new_schema.collection_name, collection_id
                );

                if existing_schema != new_schema {
                    eprintln!("Collection {} needs migration.", new_schema.collection_name);
                    //migrate
                }

                let collection = new_schema.to_collection(*collection_id, self.data_dbs);
                self.collections.push(collection);
            } else {
                new_schema.update_index_ids(None);
                self.put_schema(txn, new_schema)?;
            }
        }

        let unneeded_schemas = existing_schemas.iter().filter(|(_, s)| {
            !schemas
                .iter()
                .any(|new| new.collection_name == s.collection_name)
        });
        for (collection_id, unneeded_schema) in unneeded_schemas {
            let unneeded_collection = unneeded_schema.to_collection(*collection_id, self.data_dbs);
            unneeded_collection.clear(txn)?;
            eprintln!(
                "Collection {} is no longer needed and has been deleted.",
                unneeded_collection.name
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
        let collection_id = self.find_free_collection_id();

        let id_bytes = u16::to_le_bytes(collection_id);
        let schema_str = serde_json::to_string(schema).unwrap();
        self.schema_db.put(txn, &id_bytes, schema_str.as_bytes())?;

        let collection = schema.to_collection(collection_id, self.data_dbs);
        eprintln!(
            "Collection {} {} has been created.",
            collection.name, collection.id
        );
        self.collections.push(collection);

        Ok(())
    }

    fn find_free_collection_id(&self) -> u16 {
        loop {
            let id = random();
            if !self.collections.iter().any(|b| b.id == id) {
                return id;
            }
        }
    }

    pub fn get_collection(&self, collection_index: usize) -> Option<&IsarCollection> {
        self.collections.get(collection_index)
    }
}
