use crate::error::illegal_arg;
use crate::error::Result;
use crate::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct LinkSchema {
    pub(super) id: Option<u16>,
    pub(super) name: String,
    #[serde(rename = "foreignCollection")]
    pub(super) foreign_collection_name: String,
    #[serde(rename = "foreignLink")]
    pub(super) foreign_link_name: Option<String>,
}

impl LinkSchema {
    pub(super) fn validate(&self, schema: &Schema) -> Result<()> {
        let collection_exists = schema
            .collections
            .iter()
            .any(|c| c.name == self.foreign_collection_name);
        if !collection_exists {
            illegal_arg("Illegal relation: Foreign collection does not exist.")?;
        }

        if let Some(foreign_link_name) = &self.foreign_link_name {
            let foreign_collection = schema
                .collections
                .iter()
                .find(|c| c.name == self.foreign_collection_name)
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

        Ok(())
    }
}
