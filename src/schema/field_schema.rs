use crate::field::DataType;
use serde::{Deserialize, Serialize};

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct FieldSchema {
    pub(super) name: String,
    #[serde(rename = "type")]
    pub(super) data_type: DataType,
}

impl FieldSchema {
    pub fn new(name: &str, data_type: DataType) -> FieldSchema {
        FieldSchema {
            name: name.to_string(),
            data_type,
        }
    }
}
