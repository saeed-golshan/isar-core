use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Serialize, Deserialize, Clone)]
pub struct IndexSchema {
    pub(super) id: Option<u16>,
    #[serde(rename = "properties")]
    pub(super) property_names: Vec<String>,
    pub(super) unique: bool,
    #[serde(rename = "hashValue")]
    pub(super) hash_value: bool,
}

impl IndexSchema {
    pub fn new(property_names: &[&str], unique: bool, hash_value: bool) -> IndexSchema {
        IndexSchema {
            id: None,
            property_names: property_names.iter().map(|f| f.to_string()).collect_vec(),
            unique,
            hash_value,
        }
    }
}
