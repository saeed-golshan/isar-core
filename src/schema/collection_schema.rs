use crate::collection::IsarCollection;
use crate::data_dbs::DataDbs;
use crate::error::{illegal_arg, Result};
use crate::index::{Index, IndexType};
use crate::object::object_info::ObjectInfo;
use crate::object::property::{DataType, Property};
use crate::schema::index_schema::IndexSchema;
use crate::schema::property_schema::PropertySchema;
use itertools::Itertools;
use rand::random;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Clone)]
pub struct CollectionSchema {
    pub(super) id: Option<u16>,
    pub(super) name: String,
    pub(super) properties: Vec<PropertySchema>,
    pub(super) indexes: Vec<IndexSchema>,
}

impl CollectionSchema {
    pub fn new(name: &str) -> CollectionSchema {
        CollectionSchema {
            id: None,
            name: name.to_string(),
            properties: vec![],
            indexes: vec![],
        }
    }

    pub fn add_property(&mut self, name: &str, data_type: DataType) -> Result<()> {
        if name.is_empty() {
            illegal_arg("Empty properties are not allowed")?;
        }

        if self.properties.iter().any(|f| f.name == name) {
            illegal_arg("Property already exists")?;
        }

        if let Some(previous) = self.properties.last() {
            match data_type.cmp(&previous.data_type) {
                Ordering::Equal => {
                    if name < &previous.name {
                        illegal_arg("Propertys with same type need to be ordered alphabetically")?;
                    }
                }
                Ordering::Less => illegal_arg("Propertys need to be ordered by type")?,
                Ordering::Greater => {}
            }
        }

        self.properties.push(PropertySchema {
            name: name.to_string(),
            data_type,
        });

        Ok(())
    }

    pub fn add_index(
        &mut self,
        property_names: &[&str],
        unique: bool,
        hash_value: bool,
    ) -> Result<()> {
        if property_names.is_empty() {
            illegal_arg("At least one property needs to be added to a valid index.")?;
        }

        if property_names.len() > 3 {
            illegal_arg("No more than three properties may be used as a composite index.")?;
        }

        let duplicate = self.indexes.iter().any(|i| {
            i.property_names == property_names && i.unique == unique && i.hash_value == hash_value
        });
        if duplicate {
            illegal_arg("Index already exists")?;
        }

        let unknown_property = property_names
            .iter()
            .any(|index_property| !self.properties.iter().any(|f| f.name == *index_property));
        if unknown_property {
            illegal_arg("Index property does not exist")?;
        }

        let has_string_properties = property_names.iter().any(|name| {
            self.properties.iter().any(|f| {
                f.name == *name && f.data_type == DataType::String
                    || f.data_type == DataType::StringList
            })
        });

        if has_string_properties && hash_value {
            illegal_arg("Only string indexes can be hashed")?;
        }

        self.indexes
            .push(IndexSchema::new(property_names, unique, hash_value));

        Ok(())
    }

    pub(super) fn get_isar_collection(&self, dbs: DataDbs) -> IsarCollection {
        let properties = self.get_properties();
        let indexes = self.get_indexes(&properties, dbs);
        let object_info = ObjectInfo::new(properties);
        IsarCollection::new(self.id.unwrap(), object_info, indexes, dbs.primary)
    }

    fn get_properties(&self) -> Vec<Property> {
        let mut offset = 0;

        self.properties
            .iter()
            .map(|f| {
                let property = Property::new(&f.name, f.data_type, offset);

                let size = match f.data_type {
                    DataType::Bool => 1,
                    _ => 8,
                };

                offset += size;

                property
            })
            .collect()
    }

    fn get_indexes(&self, properties: &[Property], dbs: DataDbs) -> Vec<Index> {
        self.indexes
            .iter()
            .map(|index| {
                let properties = index
                    .property_names
                    .iter()
                    .map(|name| {
                        let pos = self
                            .properties
                            .iter()
                            .position(|property| &property.name == name)
                            .unwrap();
                        properties.get(pos).unwrap()
                    })
                    .cloned()
                    .collect_vec();
                let (index_type, db) = if index.unique {
                    (IndexType::Secondary, dbs.secondary)
                } else {
                    (IndexType::SecondaryDup, dbs.secondary_dup)
                };
                Index::new(
                    index.id.unwrap(),
                    properties,
                    index_type,
                    index.hash_value,
                    db,
                )
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
            let properties = &self.properties;

            for index in &mut self.indexes {
                let existing_index = existing_collection.indexes.iter().find(|i| &index == i);
                if let Some(existing_index) = existing_index {
                    let can_reuse_index = !index.property_names.iter().any(|property_name| {
                        let property = properties
                            .iter()
                            .find(|f| &f.name == property_name)
                            .unwrap();
                        let existing_property = existing_collection
                            .properties
                            .iter()
                            .find(|f| &f.name == property_name)
                            .unwrap();
                        property.data_type != existing_property.data_type
                    });

                    if can_reuse_index {
                        index.id = existing_index.id;
                    }
                }
            }

            if self.properties == existing_collection.properties
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
