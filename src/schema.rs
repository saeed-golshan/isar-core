use crate::bank::IsarBank;
use crate::data_dbs::{DataDbs, IndexType};
use crate::error::IsarError::IllegalArgument;
use crate::error::{illegal_arg, Result};
use crate::field::{DataType, Field};
use crate::index::Index;
use crate::lmdb::db::Db;
use itertools::Itertools;
use rand::random;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct Schema {
    #[serde(rename = "name")]
    pub bank_name: String,
    pub fields: Vec<SchemaField>,
    pub indexes: Vec<SchemaIndex>,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct SchemaField {
    name: String,
    #[serde(rename = "type")]
    data_type: DataType,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct SchemaIndex {
    id: Option<u16>,
    #[serde(rename = "fields")]
    field_names: Vec<String>,
    unique: bool,
    #[serde(rename = "hashValue")]
    hash_value: Option<bool>,
}

impl Schema {
    pub fn schemas_from_json(json: &str) -> Result<Vec<Schema>> {
        let schemas: Vec<Schema> = serde_json::from_str(json).map_err(|e| {
            eprintln!("{:?}", e);
            IllegalArgument {
                source: Some(Box::new(e)),
                message: "Could not parse schema json.".to_string(),
            }
        })?;
        for schema in &schemas {
            schema.validate()?;
        }
        Ok(schemas)
    }

    pub fn validate(&self) -> Result<()> {
        if self.fields.is_empty() {
            illegal_arg("Schema needs to have at least one field.")?;
        }

        for field in &self.fields {
            let duplicate_field = self
                .fields
                .iter()
                .any(|f| f != field && f.name == field.name);
            if duplicate_field {
                illegal_arg("Schema contains duplicate fields.")?;
            }
        }

        let is_sorted = self.fields.is_sorted_by(|field1, field2| {
            let ord = match field1.data_type.cmp(&field2.data_type) {
                Ordering::Equal => field1.name.cmp(&field2.name),
                cmp => cmp,
            };
            Some(ord)
        });
        if !is_sorted {
            illegal_arg("Fields need to be sorted by data type and by name.")?;
        }

        for index in &self.indexes {
            if index.field_names.is_empty() {
                illegal_arg("At least one field needs to be added to a valid index.")?;
            } else if index.field_names.len() > 3 {
                illegal_arg("No more than three fields may be used as a composite index.")?;
            }

            let index_exists = self
                .indexes
                .iter()
                .any(|i| i != index && i.field_names == index.field_names);
            if index_exists {
                illegal_arg("Index already exists.")?;
            }

            let unknown_field = index
                .field_names
                .iter()
                .any(|index_field| !self.fields.iter().any(|f| f.name == *index_field));

            if unknown_field {
                illegal_arg("Field specified in index is not part of the schema.")?;
            }

            let has_string_fields = index.field_names.iter().any(|name| {
                self.fields.iter().any(|f| {
                    f.name == *name && f.data_type == DataType::String
                        || f.data_type == DataType::StringList
                })
            });

            if let Some(hash_value) = index.hash_value {
                if !has_string_fields {
                    illegal_arg("Only String indexes may use the 'hashValue' parameter.")?;
                }
                if !hash_value && index.field_names.len() > 1 {
                    illegal_arg("composite indexes need to use String hashes.")?;
                }
            } else if has_string_fields {
                illegal_arg(
                    "Index contains Strings and must therefore contain the 'hashValue' field.",
                )?;
            }
        }

        Ok(())
    }

    pub fn update_index_ids(&mut self, existing_schema: Option<&Schema>) {
        if let Some(existing_schema) = existing_schema {
            for old_index in &existing_schema.indexes {
                let index = self.indexes.iter_mut().find(|index| {
                    index.field_names == old_index.field_names
                        && index.unique == old_index.unique
                        && index.hash_value == old_index.hash_value
                });
                if let Some(index) = index {
                    index.id = old_index.id;
                }
            }
        }

        let mut index_ids = self
            .indexes
            .iter()
            .filter_map(|index| index.id)
            .collect_vec();
        for mut index in &mut self.indexes {
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

    pub fn to_bank(&self, bank_id: u16, dbs: DataDbs) -> IsarBank {
        let fields = self.get_fields();
        let indexes = self.get_indexes(bank_id, &fields);
        IsarBank::new(self.bank_name.clone(), bank_id, fields, indexes, dbs)
    }

    fn get_fields(&self) -> Vec<Field> {
        let mut offset = 0;

        self.fields
            .iter()
            .map(|f| {
                let field = Field::new(f.name.to_string(), f.data_type, offset);

                let size = match f.data_type {
                    DataType::Bool => 1,
                    _ => 8,
                };

                offset += size;

                field
            })
            .collect()
    }

    fn get_indexes(&self, bank_id: u16, fields: &[Field]) -> Vec<Index> {
        self.indexes
            .iter()
            .map(|index| {
                let fields = index
                    .field_names
                    .iter()
                    .map(|name| fields.iter().find(|f| f.name == *name).unwrap())
                    .cloned()
                    .collect();
                let index_type = if index.unique {
                    IndexType::Secondary
                } else {
                    IndexType::SecondaryDup
                };
                Index::new(
                    bank_id,
                    index.id.unwrap(),
                    fields,
                    index_type,
                    index.hash_value,
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {}
