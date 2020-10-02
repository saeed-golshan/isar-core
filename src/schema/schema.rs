use crate::collection::IsarCollection;
use crate::data_dbs::{DataDbs, IndexType};
use crate::error::IsarError::IllegalArgument;
use crate::error::{illegal_arg, Result};
use crate::field::{DataType, Field};
use crate::index::Index;
use itertools::Itertools;
use rand::random;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct Schema {
    collections: Vec<CollectionSchema>,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct CollectionSchema {
    id: Option<u16>,
    name: String,
    pub(crate) fields: Vec<FieldSchema>,
    pub(crate) links: Vec<LinkSchema>,
    pub(crate) indexes: Vec<IndexSchema>,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct FieldSchema {
    name: String,
    #[serde(rename = "type")]
    data_type: DataType,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct LinkSchema {
    id: Option<u16>,
    name: String,
    #[serde(rename = "foreignCollection")]
    foreign_collection_name: String,
    #[serde(rename = "foreignLink")]
    foreign_link_name: Option<String>,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct IndexSchema {
    id: Option<u16>,
    #[serde(rename = "fields")]
    field_names: Vec<String>,
    unique: bool,
    #[serde(rename = "hashValue")]
    hash_value: Option<bool>,
}

impl Schema {
    pub fn schema_from_json(json: &str) -> Result<Schema> {
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
            self.validate_collection(collection)?
        }

        Ok(())
    }

    fn validate_collection(&self, collection: &CollectionSchema) -> Result<()> {
        if collection.fields.is_empty() {
            illegal_arg("Schema needs to have at least one field.")?;
        }

        let field_link_names = collection
            .fields
            .iter()
            .map(|f| &f.name)
            .merge(collection.links.iter().map(|l| &l.name))
            .collect_vec();

        if field_link_names.len() != field_link_names.iter().unique().count() {
            illegal_arg("Schema contains duplicate fields or links.")?;
        }

        let is_sorted = collection.fields.is_sorted_by(|field1, field2| {
            let ord = match field1.data_type.cmp(&field2.data_type) {
                Ordering::Equal => field1.name.cmp(&field2.name),
                cmp => cmp,
            };
            Some(ord)
        });
        if !is_sorted {
            illegal_arg("Fields need to be sorted by data type and by name.")?;
        }

        for link in &collection.links {
            let collection_exists = self
                .collections
                .iter()
                .any(|c| c.name == link.foreign_collection_name);
            if !collection_exists {
                illegal_arg("Illegal relation: Foreign collection does not exist.")?;
            }

            if let Some(foreign_link_name) = &link.foreign_link_name {
                let foreign_collection = self
                    .collections
                    .iter()
                    .find(|c| c.name == link.foreign_collection_name)
                    .unwrap();

                let foreign_link = foreign_collection
                    .links
                    .iter()
                    .find(|f| &f.name == foreign_link_name);

                if let Some(foreign_link) = foreign_link {
                    if foreign_link.foreign_link_name.is_some() {
                        illegal_arg("Two backlinks point to each other.")?;
                    }
                } else {
                    illegal_arg("Backlink points to non existing link.")?;
                }
            }
        }

        for index in &collection.indexes {
            if index.field_names.is_empty() {
                illegal_arg("At least one field needs to be added to a valid index.")?;
            } else if index.field_names.len() > 3 {
                illegal_arg("No more than three fields may be used as a composite index.")?;
            }

            let index_exists = collection
                .indexes
                .iter()
                .any(|i| i != index && i.field_names == index.field_names);
            if index_exists {
                illegal_arg("Duplicate index.")?;
            }

            let unknown_field = index
                .field_names
                .iter()
                .any(|index_field| !collection.fields.iter().any(|f| f.name == *index_field));

            if unknown_field {
                illegal_arg("Field specified in index is not part of the schema.")?;
            }

            let has_string_fields = index.field_names.iter().any(|name| {
                collection.fields.iter().any(|f| {
                    f.name == *name && f.data_type == DataType::String
                        || f.data_type == DataType::StringList
                })
            });

            if let Some(hash_value) = index.hash_value {
                if !has_string_fields {
                    illegal_arg("Only String indexes may use the 'hashValue' parameter.")?;
                }
                if !hash_value && index.field_names.len() > 1 {
                    illegal_arg("Composite indexes need to use String hashes.")?;
                }
            } else if has_string_fields {
                illegal_arg(
                    "Index contains Strings and must therefore contain the 'hashValue' field.",
                )?;
            }
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
        for mut collection in &mut self.collections {
            for mut index in &mut collection.indexes {
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
        let indexes = self.get_indexes(collection, &fields);
        IsarCollection::new(
            collection.name.clone(),
            collection.id.unwrap(),
            fields,
            vec![],
            indexes,
            dbs.primary,
        )
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

    fn get_indexes(&self, collection: &CollectionSchema, fields: &[Field]) -> Vec<Index> {
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
                let index_type = if index.unique {
                    IndexType::Secondary
                } else {
                    IndexType::SecondaryDup
                };
                Index::new(index.id.unwrap(), fields, index_type, index.hash_value)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {}
