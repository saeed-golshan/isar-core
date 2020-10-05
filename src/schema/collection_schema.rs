use crate::collection::IsarCollection;
use crate::data_dbs::DataDbs;
use crate::error::{illegal_arg, Result};
use crate::field::{DataType, Field};
use crate::index::{Index, IndexType};
use crate::schema::field_schema::FieldSchema;
use crate::schema::index_schema::IndexSchema;
use itertools::Itertools;
use rand::random;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Clone)]
pub struct CollectionSchema {
    pub(super) id: Option<u16>,
    pub(super) name: String,
    pub(super) fields: Vec<FieldSchema>,
    pub(super) indexes: Vec<IndexSchema>,
}

impl CollectionSchema {
    pub fn new(name: &str) -> CollectionSchema {
        CollectionSchema {
            id: None,
            name: name.to_string(),
            fields: vec![],
            indexes: vec![],
        }
    }

    pub fn add_field(&mut self, name: &str, data_type: DataType) -> Result<()> {
        if name.is_empty() {
            illegal_arg("Empty fields are not allowed")?;
        }

        if self.fields.iter().any(|f| f.name == name) {
            illegal_arg("Field already exists")?;
        }

        if let Some(previous) = self.fields.last() {
            if data_type == previous.data_type {
                if name > &previous.name {
                    illegal_arg("Fields with same type need to be ordered alphabetically")?;
                }
            } else if data_type > previous.data_type {
                illegal_arg("Fields need to be ordered by type")?;
            }
        }

        self.fields.push(FieldSchema {
            name: name.to_string(),
            data_type,
        });

        Ok(())
    }

    pub fn add_index(
        &mut self,
        field_names: &[&str],
        unique: bool,
        hash_value: bool,
    ) -> Result<()> {
        if field_names.is_empty() {
            illegal_arg("At least one field needs to be added to a valid index.")?;
        }

        if field_names.len() > 3 {
            illegal_arg("No more than three fields may be used as a composite index.")?;
        }

        let duplicate = self.indexes.iter().any(|i| {
            i.field_names == field_names
                && i.unique == unique
                && (i.hash_value.is_none() || i.hash_value == Some(hash_value))
        });
        if duplicate {
            illegal_arg("Index already exists")?;
        }

        let unknown_field = field_names
            .iter()
            .any(|index_field| !self.fields.iter().any(|f| f.name == *index_field));
        if unknown_field {
            illegal_arg("Index field does not exist")?;
        }

        let has_string_fields = field_names.iter().any(|name| {
            self.fields.iter().any(|f| {
                f.name == *name && f.data_type == DataType::String
                    || f.data_type == DataType::StringList
            })
        });

        let index = if has_string_fields {
            IndexSchema::new(field_names, unique, Some(hash_value))
        } else {
            IndexSchema::new(field_names, unique, None)
        };

        self.indexes.push(index);

        Ok(())
    }

    pub(super) fn get_isar_collection(&self, dbs: DataDbs) -> IsarCollection {
        let fields = self.get_fields();
        let indexes = self.get_indexes(&fields, dbs);
        IsarCollection::new(self.id.unwrap(), fields, vec![], indexes, dbs.primary)
    }

    fn get_fields(&self) -> Vec<Field> {
        let mut offset = 0;

        self.fields
            .iter()
            .map(|f| {
                let field = Field::new(&f.name, f.data_type, offset);

                let size = match f.data_type {
                    DataType::Bool => 1,
                    _ => 8,
                };

                offset += size;

                field
            })
            .collect()
    }

    fn get_indexes(&self, fields: &[Field], dbs: DataDbs) -> Vec<Index> {
        self.indexes
            .iter()
            .map(|index| {
                let fields = index
                    .field_names
                    .iter()
                    .map(|name| {
                        let pos = self
                            .fields
                            .iter()
                            .position(|field| &field.name == name)
                            .unwrap();
                        fields.get(pos).unwrap()
                    })
                    .cloned()
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

    pub(super) fn update_with_existing_collection(
        &mut self,
        existing_collections: &[CollectionSchema],
        used_ids: &mut HashSet<u16>,
    ) {
        if let Some(existing_collection) = existing_collections.iter().find(|c| c.name == self.name)
        {
            let fields = &self.fields;

            for index in &mut self.indexes {
                let existing_index = existing_collection.indexes.iter().find(|i| &index == i);
                if let Some(existing_index) = existing_index {
                    let can_reuse_index = !index.field_names.iter().any(|field_name| {
                        let field = fields.iter().find(|f| &f.name == field_name).unwrap();
                        let existing_field = existing_collection
                            .fields
                            .iter()
                            .find(|f| &f.name == field_name)
                            .unwrap();
                        field.data_type != existing_field.data_type
                    });

                    if can_reuse_index {
                        index.id = existing_index.id;
                    }
                }
            }

            if self.fields == existing_collection.fields
                && self.indexes == existing_collection.indexes
            {
                self.id = existing_collection.id;
            }
        }

        if self.id.is_none() {
            self.id = Some(Self::find_id(used_ids));
        }
        for index in &mut self.indexes {
            if index.id.is_none() {
                index.id = Some(Self::find_id(used_ids));
            }
        }
    }

    fn find_id(used_ids: &mut HashSet<u16>) -> u16 {
        loop {
            let id = random();
            if used_ids.insert(id) {
                return id;
            }
        }
    }

    pub(super) fn collect_ids(&self, ids: &mut HashSet<u16>) {
        if let Some(id) = self.id {
            ids.insert(id);
        }
        for index in &self.indexes {
            if let Some(id) = index.id {
                ids.insert(id);
            }
        }
    }
}
