use enum_ordinalize::Ordinalize;
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(
    Ord, PartialOrd, PartialEq, Eq, Clone, Copy, Serialize_repr, Deserialize_repr, Debug, Ordinalize,
)]
#[repr(u8)]
pub enum DataType {
    Bool = 0,

    Int = 1,
    Float = 2,

    Long = 3,
    Double = 4,

    String = 5,
    Bytes = 6,
    BoolList = 7,
    StringList = 8,
    BytesList = 9,

    IntList = 10,
    FloatList = 11,

    LongList = 12,
    DoubleList = 13,
}

impl DataType {
    pub fn is_dynamic(&self) -> bool {
        !matches!(
            &self,
            DataType::Int | DataType::Long | DataType::Float | DataType::Double | DataType::Bool
        )
    }

    pub fn get_static_size(&self) -> usize {
        match *self {
            DataType::Bool => 1,
            DataType::Int | DataType::Float => 4,
            _ => 8,
        }
    }

    pub fn get_element_size(&self) -> usize {
        match *self {
            DataType::String
            | DataType::Bytes
            | DataType::BoolList
            | DataType::StringList
            | DataType::BytesList => 1,
            DataType::IntList | DataType::FloatList => 4,
            DataType::LongList | DataType::DoubleList => 8,
            _ => 0,
        }
    }
}
