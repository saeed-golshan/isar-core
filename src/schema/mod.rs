pub mod collection_schema;
pub mod field_schema;
pub mod index_schema;
pub mod link_schema;
//pub mod schema_diff;

use crate::collection::IsarCollection;
use crate::data_dbs::DataDbs;
use crate::error::IsarError::IllegalArgument;
use crate::error::{illegal_arg, Result};
use crate::field::{DataType, Field};
use crate::index::{Index, IndexType};
use crate::schema::collection_schema::CollectionSchema;
use itertools::Itertools;
use rand::random;
use serde::{Deserialize, Serialize};

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct Schema {
    collections: Vec<CollectionSchema>,
}

impl Schema {
    pub fn new(collections: Vec<CollectionSchema>) -> Schema {
        Schema { collections }
    }

    pub fn from_json(json: &str) -> Result<Schema> {
        let schema: Schema = serde_json::from_str(json).map_err(|e| {
            eprintln!("{:?}", e);
            IllegalArgument {
                source: Some(Box::new(e)),
                message: "Could not parse schema json.".to_string(),
            }
        })?;
        schema.validate()?;
        Ok(schema)
    }

    pub fn validate(&self) -> Result<()> {
        if self.collections.iter().unique_by(|c| &c.name).count() != self.collections.len() {
            illegal_arg("Schema contains duplicate collections.")?;
        }
        for collection in &self.collections {
            collection.validate(self)?;
        }

        Ok(())
    }

    pub fn update_ids(&mut self, existing_schema: Option<&Schema>) {
        if let Some(existing_schema) = existing_schema {
            for old_collection in &existing_schema.collections {
                let collection = self
                    .collections
                    .iter_mut()
                    .find(|c| c.name == old_collection.name);
                if let Some(collection) = collection {
                    collection.id = old_collection.id;

                    for link in &mut collection.links {
                        let old_link = old_collection.links.iter().find(|old_link| {
                            link.name == old_link.name
                                && link.foreign_collection_name == old_link.foreign_collection_name
                                && link.foreign_link_name == old_link.foreign_link_name
                        });
                        if let Some(old_link) = old_link {
                            link.id = old_link.id;
                        }
                    }

                    for index in &mut collection.indexes {
                        let old_index = old_collection.indexes.iter().find(|old_index| {
                            index.field_names == old_index.field_names
                                && index.unique == old_index.unique
                                && index.hash_value == old_index.hash_value
                        });
                        if let Some(old_index) = old_index {
                            index.id = old_index.id;
                        }
                    }
                }
            }
        }

        self.update_collection_ids();
        //self.update_link_ids();
        self.update_index_ids();
    }

    pub fn update_collection_ids(&mut self) {
        let mut collection_ids = self
            .collections
            .iter()
            .filter_map(|index| index.id)
            .collect_vec();
        for mut collection in &mut self.collections {
            if collection.id.is_none() {
                loop {
                    let id = random();
                    if !collection_ids.contains(&id) {
                        collection.id = Some(id);
                        collection_ids.push(id);
                        break;
                    }
                }
            }
        }
    }

    pub fn update_index_ids(&mut self) {
        let mut index_ids = self
            .collections
            .iter()
            .map(|c| &c.indexes)
            .flatten()
            .filter_map(|index| index.id)
            .collect_vec();
        for collection in &mut self.collections {
            for index in &mut collection.indexes {
                if index.id.is_none() {
                    loop {
                        let id = random();
                        if !index_ids.contains(&id) {
                            index.id = Some(id);
                            index_ids.push(id);
                            break;
                        }
                    }
                }
            }
        }
    }

    /*pub fn update_link_ids(&mut self) {
        let mut link_ids = self
            .collections
            .iter()
            .map(|c| c.links)
            .flatten()
            .filter_map(|link| link.id)
            .collect_vec();
        for mut collection in &mut self.collections {
            for mut link in &mut collection.links {
                if link.id.is_none() {
                    loop {
                        let id = random();
                        if !link_ids.contains(&id) {
                            link.id = Some(id);
                            link_ids.push(id);
                            break;
                        }
                    }
                }
            }
        }
    }*/

    fn get_collection(&self, collection: &CollectionSchema, dbs: DataDbs) -> IsarCollection {
        let fields = Schema::get_fields(collection);
        let indexes = self.get_indexes(collection, &fields, dbs);
        IsarCollection::new(collection.id.unwrap(), fields, vec![], indexes, dbs.primary)
    }

    fn get_fields(collection: &CollectionSchema) -> Vec<Field> {
        let mut offset = 0;

        collection
            .fields
            .iter()
            .map(|f| {
                let field = Field::new(f.data_type, offset);

                let size = match f.data_type {
                    DataType::Bool => 1,
                    _ => 8,
                };

                offset += size;

                field
            })
            .collect()
    }

    fn get_indexes(
        &self,
        collection: &CollectionSchema,
        fields: &[Field],
        dbs: DataDbs,
    ) -> Vec<Index> {
        collection
            .indexes
            .iter()
            .map(|index| {
                let fields = index
                    .field_names
                    .iter()
                    .map(|name| {
                        let pos = collection
                            .fields
                            .iter()
                            .position(|field| &field.name == name)
                            .unwrap();
                        fields.get(pos).unwrap()
                    })
                    .copied()
                    .collect();
                let (index_type, db) = if index.unique {
                    (IndexType::Secondary, dbs.secondary)
                } else {
                    (IndexType::SecondaryDup, dbs.secondary_dup)
                };
                Index::new(index.id.unwrap(), fields, index_type, index.hash_value, db)
            })
            .collect()
    }
}
