use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Serialize, Deserialize, Clone)]
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
}
