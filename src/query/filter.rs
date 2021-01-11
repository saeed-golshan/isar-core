use crate::error::{illegal_arg, Result};
use crate::object::property::Property;
use enum_dispatch::enum_dispatch;

#[derive(Eq, PartialEq)]
pub enum Case {
    Sensitive,
    Insensitive,
}

#[enum_dispatch]
pub enum Filter<'col> {
    IsNull(IsNull<'col>),
    ByteBetween(ByteBetween<'col>),
    ByteNotEqual(ByteNotEqual<'col>),
    IntBetween(IntBetween<'col>),
    IntNotEqual(IntNotEqual<'col>),
    LongBetween(LongBetween<'col>),
    LongNotEqual(LongNotEqual<'col>),
    FloatBetween(FloatBetween<'col>),
    DoubleBetween(DoubleBetween<'col>),
    /*StrAnyOf(StrAnyOf),
    StrStartsWith(),
    StrEndsWith(),
    StrContains(),*/
    And(And<'col>),
    Or(Or<'col>),
    Not(Not<'col>),
}

#[enum_dispatch(Filter)]
pub trait Condition {
    fn evaluate(&self, object: &[u8]) -> bool;
}

pub struct IsNull<'col> {
    property: &'col Property,
    is_null: bool,
}

impl<'col> Condition for IsNull<'col> {
    fn evaluate(&self, object: &[u8]) -> bool {
        self.property.is_null(object) == self.is_null
    }
}

impl<'col> IsNull<'col> {
    pub fn filter(property: &'col Property, is_null: bool) -> Filter<'col> {
        Filter::IsNull(Self { property, is_null })
    }
}

#[macro_export]
macro_rules! filter_between {
    ($name:ident, $data_type:ident, $type:ty) => {
        pub struct $name<'col> {
            upper: $type,
            lower: $type,
            property: &'col Property,
        }

        impl<'col> $name<'col> {
            pub fn filter(
                property: &'col Property,
                lower: $type,
                upper: $type,
            ) -> Result<Filter<'col>> {
                if property.data_type == crate::object::data_type::DataType::$data_type {
                    Ok(Filter::$name(Self {
                        property,
                        lower,
                        upper,
                    }))
                } else {
                    illegal_arg("Property does not support this filter.")
                }
            }
        }
    };
}

#[macro_export]
macro_rules! primitive_filter_between {
    ($name:ident, $data_type:ident, $type:ty, $prop_accessor:ident) => {
        filter_between!($name, $data_type, $type);

        impl<'col> Condition for $name<'col> {
            fn evaluate(&self, object: &[u8]) -> bool {
                let val = self.property.$prop_accessor(object);
                self.lower <= val && self.upper >= val
            }
        }
    };
}

#[macro_export]
macro_rules! float_filter_between {
    ($name:ident, $data_type:ident, $type:ty, $prop_accessor:ident) => {
        filter_between!($name, $data_type, $type);

        impl<'col> Condition for $name<'col> {
            fn evaluate(&self, object: &[u8]) -> bool {
                let val = self.property.$prop_accessor(object);
                if self.upper.is_nan() {
                    self.lower.is_nan() && val.is_nan()
                } else if self.lower.is_nan() {
                    self.upper >= val
                } else {
                    self.lower <= val && self.upper >= val
                }
            }
        }
    };
}

primitive_filter_between!(ByteBetween, Byte, u8, get_byte);
primitive_filter_between!(IntBetween, Int, i32, get_int);
primitive_filter_between!(LongBetween, Long, i64, get_long);
float_filter_between!(FloatBetween, Float, f32, get_float);
float_filter_between!(DoubleBetween, Double, f64, get_double);

#[macro_export]
macro_rules! filter_not_equal {
    ($name:ident, $data_type:ident, $type:ty) => {
        pub struct $name<'col> {
            value: $type,
            property: &'col Property,
        }

        impl<'col> $name<'col> {
            pub fn filter(property: &'col Property, value: $type) -> Result<Filter<'col>> {
                if property.data_type == crate::object::data_type::DataType::$data_type {
                    Ok(Filter::$name(Self { property, value }))
                } else {
                    illegal_arg("Property does not support this filter.")
                }
            }
        }
    };
}

#[macro_export]
macro_rules! primitive_filter_not_equal {
    ($not_equal_name:ident, $data_type:ident, $type:ty, $prop_accessor:ident) => {
        filter_not_equal!($not_equal_name, $data_type, $type);

        impl<'col> Condition for $not_equal_name<'col> {
            fn evaluate(&self, object: &[u8]) -> bool {
                let val = self.property.$prop_accessor(object);
                self.value != val
            }
        }
    };
}

primitive_filter_not_equal!(ByteNotEqual, Byte, u8, get_byte);
primitive_filter_not_equal!(IntNotEqual, Int, i32, get_int);
primitive_filter_not_equal!(LongNotEqual, Long, i64, get_long);

/*pub struct StrAnyOf {
    property: Property,
    values: Vec<Option<Vec<u8>>>,
    case: Case,
}

impl StrAnyOf {
    pub fn new(property: Property, values: &[Option<&str>], case: Case) -> StrAnyOf {
        let values = if case == Case::Insensitive {
            values
                .iter()
                .map(|s| s.to_lowercase().into_bytes())
                .collect_vec()
        } else {
            values.iter().map(|s| s.as_bytes().to_vec()).collect_vec()
        };
        StrAnyOf {
            property,
            values,
            case,
        }
    }
}

impl Condition for StrAnyOf {
    fn evaluate(&self, object: &[u8]) -> bool {
        let string_bytes = self.property.get_bytes(object);
        match self.case {
            Case::Sensitive => self
                .values
                .iter()
                .any(|item| item.as_slice() == string_bytes),
            Case::Insensitive => unsafe {
                let lowercase_string = std::str::from_utf8_unchecked(object).to_lowercase();
                let lowercase_bytes = lowercase_string.as_bytes();
                self.values
                    .iter()
                    .any(|item| item.as_slice() == lowercase_bytes)
            },
        }
    }
}

impl StrAnyOf {
    pub fn filter(property: Property, values: Vec<Vec<u8>>, case: Case) -> Filter {
        Filter::StrAnyOf(StrAnyOf {
            property,
            values,
            case,
        })
    }
}*/

pub struct And<'col> {
    filters: Vec<Filter<'col>>,
}

impl<'col> Condition for And<'col> {
    fn evaluate(&self, object: &[u8]) -> bool {
        for filter in &self.filters {
            if !filter.evaluate(object) {
                return false;
            }
        }
        true
    }
}

impl<'col> And<'col> {
    pub fn filter(filters: Vec<Filter<'col>>) -> Filter<'col> {
        Filter::And(And { filters })
    }
}

pub struct Or<'col> {
    filters: Vec<Filter<'col>>,
}

impl<'col> Condition for Or<'col> {
    fn evaluate(&self, object: &[u8]) -> bool {
        for filter in &self.filters {
            if filter.evaluate(object) {
                return true;
            }
        }
        false
    }
}

impl<'col> Or<'col> {
    pub fn filter(filters: Vec<Filter<'col>>) -> Filter<'col> {
        Filter::Or(Or { filters })
    }
}

pub struct Not<'col> {
    filter: Box<Filter<'col>>,
}

impl<'col> Condition for Not<'col> {
    fn evaluate(&self, object: &[u8]) -> bool {
        self.filter.evaluate(object)
    }
}

impl<'col> Not<'col> {
    pub fn filter(filter: Filter<'col>) -> Filter<'col> {
        Filter::Not(Not {
            filter: Box::new(filter),
        })
    }
}
