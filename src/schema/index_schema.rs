use crate::error::{illegal_arg, Result};
use crate::field::DataType;
use crate::schema::collection_schema::CollectionSchema;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct IndexSchema {
    pub(super) id: Option<u16>,
    #[serde(rename = "fields")]
    pub(super) field_names: Vec<String>,
    pub(super) unique: bool,
    #[serde(rename = "hashValue")]
    pub(super) hash_value: Option<bool>,
}

impl IndexSchema {
    pub fn new(field_names: &[&str], unique: bool, hash_value: Option<bool>) -> IndexSchema {
        IndexSchema {
            id: None,
            field_names: field_names.iter().map(|f| f.to_string()).collect_vec(),
            unique,
            hash_value,
        }
    }

    pub fn validate(&self, collection: &CollectionSchema) -> Result<()> {
        if self.field_names.is_empty() {
            illegal_arg("At least one field needs to be added to a valid index.")?;
        } else if self.field_names.len() > 3 {
            illegal_arg("No more than three fields may be used as a composite index.")?;
        }

        let index_exists = collection
            .indexes
            .iter()
            .any(|i| i != self && i.field_names == self.field_names);
        if index_exists {
            illegal_arg("Duplicate index.")?;
        }

        let unknown_field = self
            .field_names
            .iter()
            .any(|index_field| !collection.fields.iter().any(|f| f.name == *index_field));

        if unknown_field {
            illegal_arg("Field specified in index is not part of the schema.")?;
        }

        let has_string_fields = self.field_names.iter().any(|name| {
            collection.fields.iter().any(|f| {
                f.name == *name && f.data_type == DataType::String
                    || f.data_type == DataType::StringList
            })
        });

        if let Some(hash_value) = self.hash_value {
            if !has_string_fields {
                illegal_arg("Only String indexes may use the 'hashValue' parameter.")?;
            }
            if !hash_value && self.field_names.len() > 1 {
                illegal_arg("Composite indexes need to use String hashes.")?;
            }
        } else if has_string_fields {
            illegal_arg(
                "Index contains Strings and must therefore contain the 'hashValue' field.",
            )?;
        }

        Ok(())
    }
}
