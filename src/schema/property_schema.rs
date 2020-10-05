use crate::object::property::DataType;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Serialize, Deserialize, Clone)]
pub struct PropertySchema {
    pub(super) name: String,
    #[serde(rename = "type")]
    pub(super) data_type: DataType,
}

impl PropertySchema {
    pub fn new(name: &str, data_type: DataType) -> PropertySchema {
        PropertySchema {
            name: name.to_string(),
            data_type,
        }
    }
}
