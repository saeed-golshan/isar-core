use enum_ordinalize::Ordinalize;
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(
    Ord, PartialOrd, PartialEq, Eq, Clone, Copy, Serialize_repr, Deserialize_repr, Debug, Ordinalize,
)]
#[repr(u8)]
pub enum DataType {
    // Alignment 1
    Byte = 0,

    // Alignment 4
    Int = 1,
    Float = 2,

    // Alignment 8
    Long = 3,
    Double = 4,

    // Element Alignment 1
    String = 5,
    ByteList = 6,

    // Element Alignment 4
    IntList = 7,
    FloatList = 8,

    // Element Alignment 8
    LongList = 9,
    DoubleList = 10,

    // Offset List alignment 8
    // Element Alignment 1
    StringList = 11,
}

impl DataType {
    pub fn is_dynamic(&self) -> bool {
        !matches!(
            &self,
            DataType::Int | DataType::Long | DataType::Float | DataType::Double | DataType::Byte
        )
    }

    pub fn get_static_size(&self) -> usize {
        match *self {
            DataType::Byte => 1,
            DataType::Int | DataType::Float => 4,
            _ => 8,
        }
    }

    pub fn get_element_size(&self) -> usize {
        match *self {
            DataType::String | DataType::ByteList | DataType::StringList => 1,
            DataType::IntList | DataType::FloatList => 4,
            DataType::LongList | DataType::DoubleList => 8,
            _ => 0,
        }
    }
}
