use crate::error::{illegal_arg, Result};
use crate::schema::field_schema::FieldSchema;
use crate::schema::index_schema::IndexSchema;
use crate::schema::link_schema::LinkSchema;
use crate::schema::Schema;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct CollectionSchema {
    pub(super) id: Option<u16>,
    pub(super) name: String,
    pub(super) fields: Vec<FieldSchema>,
    pub(super) links: Vec<LinkSchema>,
    pub(super) indexes: Vec<IndexSchema>,
}

impl CollectionSchema {
    pub fn new(
        name: &str,
        fields: &[FieldSchema],
        links: &[LinkSchema],
        indexes: &[IndexSchema],
    ) -> CollectionSchema {
        CollectionSchema {
            id: None,
            name: name.to_string(),
            fields: fields.to_vec(),
            links: links.to_vec(),
            indexes: indexes.to_vec(),
        }
    }

    pub fn validate(&self, schema: &Schema) -> Result<()> {
        if self.fields.is_empty() {
            illegal_arg("Schema needs to have at least one field.")?;
        }

        let field_link_names = self
            .fields
            .iter()
            .map(|f| &f.name)
            .merge(self.links.iter().map(|l| &l.name))
            .collect_vec();

        if field_link_names.len() != field_link_names.iter().unique().count() {
            illegal_arg("Schema contains duplicate fields or links.")?;
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

        for link in &self.links {
            link.validate(schema)?;
        }

        for index in &self.indexes {
            index.validate(self)?;
        }

        Ok(())
    }
}
