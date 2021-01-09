use crate::schema::property_schema::PropertySchema;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct IndexSchema {
    pub(super) id: Option<u16>,
    #[serde(rename = "properties")]
    pub(super) properties: Vec<PropertySchema>,
    pub(super) unique: bool,
    #[serde(rename = "hashValue")]
    pub(super) hash_value: bool,
}

impl IndexSchema {
    pub fn new(properties: Vec<PropertySchema>, unique: bool, hash_value: bool) -> IndexSchema {
        IndexSchema {
            id: None,
            properties,
            unique,
            hash_value,
        }
    }

    pub(crate) fn update_with_existing_indexes<F>(
        &mut self,
        existing_indexes: &[IndexSchema],
        get_id: &mut F,
    ) where
        F: FnMut() -> u16,
    {
        let existing_index = existing_indexes.iter().find(|i| {
            i.properties == self.properties
                && i.unique == self.unique
                && i.hash_value == self.hash_value
        });
        if let Some(existing_index) = existing_index {
            self.id = existing_index.id;
        } else {
            self.id = Some(get_id());
        }
    }
}
