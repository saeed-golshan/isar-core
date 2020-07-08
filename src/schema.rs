use crate::bank::IsarBank;
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
    pub indices: Vec<SchemaIndex>,
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

        for index in &self.indices {
            if index.field_names.is_empty() {
                illegal_arg("At least one field needs to be added to a valid index.")?;
            } else if index.field_names.len() > 3 {
                illegal_arg("No more than three fields may be used as a compound index.")?;
            }

            let index_exists = self
                .indices
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
                    illegal_arg("Only String indices may use the 'hashValue' parameter.")?;
                }
                if !hash_value && index.field_names.len() > 1 {
                    illegal_arg("Compound indices need to use String hashes.")?;
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
            for old_index in &existing_schema.indices {
                let index = self.indices.iter_mut().find(|index| {
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
            .indices
            .iter()
            .filter_map(|index| index.id)
            .collect_vec();
        for mut index in &mut self.indices {
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

    pub fn to_bank(&self, bank_id: u16, data_db: Db, index_db: Db, index_dup_db: Db) -> IsarBank {
        let mut fields = self.get_fields();

        let indices = self.get_indices(bank_id, index_db, index_dup_db, &fields);

        IsarBank::new(self.bank_name.clone(), bank_id, fields, indices, data_db)
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

    fn get_indices(
        &self,
        bank_id: u16,
        index_db: Db,
        index_dup_db: Db,
        fields: &[Field],
    ) -> Vec<Index> {
        self.indices
            .iter()
            .map(|index| {
                let fields = index
                    .field_names
                    .iter()
                    .map(|name| fields.iter().find(|f| f.name == *name).unwrap())
                    .cloned()
                    .collect();
                let db = if index.unique { index_db } else { index_dup_db };
                Index::new(
                    bank_id,
                    index.id.unwrap(),
                    fields,
                    index.unique,
                    index.hash_value,
                    db,
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {}
