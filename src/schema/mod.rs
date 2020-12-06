pub mod collection_schema;
pub mod index_schema;
pub mod property_schema;

use crate::collection::IsarCollection;
use crate::data_dbs::DataDbs;
use crate::error::{illegal_arg, Result};
use crate::schema::collection_schema::CollectionSchema;
use hashbrown::HashSet;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Schema {
    collections: Vec<CollectionSchema>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema {
            collections: vec![],
        }
    }

    pub fn add_collection(&mut self, collection: CollectionSchema) -> Result<()> {
        if self.collections.iter().any(|c| c.name == collection.name) {
            illegal_arg("Schema already contains this collection.")?;
        }
        self.collections.push(collection);
        Ok(())
    }

    pub(crate) fn build_collections(
        mut self,
        dbs: DataDbs,
        existing_schema: Option<&Schema>,
    ) -> Vec<IsarCollection> {
        self.update_ids(existing_schema);
        self.collections
            .iter()
            .map(|c| c.get_isar_collection(dbs))
            .collect()
    }

    fn update_ids(&mut self, existing_schema: Option<&Schema>) {
        let mut ids = HashSet::<u16>::new();
        if let Some(existing_schema) = existing_schema {
            for collection in &existing_schema.collections {
                collection.collect_ids(&mut ids);
            }
        }

        let empty = vec![];
        let existing_collections = existing_schema.map_or(&empty, |c| &c.collections);
        for collection in &mut self.collections {
            collection.update_with_existing_collection(existing_collections, &mut ids)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_collection() {
        let mut schema = Schema::new();

        let col1 = CollectionSchema::new("col");
        schema.add_collection(col1).unwrap();

        let col2 = CollectionSchema::new("other");
        schema.add_collection(col2).unwrap();

        let duplicate = CollectionSchema::new("col");
        assert!(schema.add_collection(duplicate).is_err());
    }
}
