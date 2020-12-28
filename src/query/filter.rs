use crate::error::{illegal_arg, Result};
use crate::object::data_type::DataType;
use crate::object::property::Property;
use enum_dispatch::enum_dispatch;

#[derive(Eq, PartialEq)]
pub enum Case {
    Sensitive,
    Insensitive,
}

#[enum_dispatch]
pub enum Filter {
    IsNull(IsNull),
    Bool(BoolEqualTo),
    Int(IntBetween),
    Long(LongBetween),
    Float(FloatBetween),
    Double(DoubleBetween),
    //StrAnyOf(StrAnyOf),
    /*StrStartsWith(),
    StrEndsWith(),
    StrContains(),*/
    And(And),
    Or(Or),
    Not(Not),
}

#[enum_dispatch(Filter)]
pub trait Condition {
    fn evaluate(&self, object: &[u8]) -> bool;
}

/*impl Filter {
    fn null_safe(self) -> Filter {
        Filter::NonNullFilter(NonNullFilter {
            property: self.get_property().unwrap(),
            filter: Box::new(self),
        })
    }
}*/

pub struct IsNull {
    property: Property,
    is_null: bool,
}

impl Condition for IsNull {
    fn evaluate(&self, object: &[u8]) -> bool {
        self.property.is_null(object) == self.is_null
    }
}

impl IsNull {
    pub fn filter(property: Property, is_null: bool) -> Filter {
        Filter::IsNull(Self { property, is_null })
    }
}

pub struct BoolEqualTo {
    value: bool,
    property: Property,
}

impl Condition for BoolEqualTo {
    fn evaluate(&self, object: &[u8]) -> bool {
        let val = self.property.get_bool(object) == Property::TRUE_BOOL;
        self.value == val
    }
}

impl BoolEqualTo {
    pub fn filter(property: Property, value: bool) -> Result<Filter> {
        if property.data_type == DataType::Bool {
            Ok(Filter::Bool(Self { property, value }))
        } else {
            illegal_arg("Property does not support this filter.")
        }
    }
}

#[macro_export]
macro_rules! primitive_filter (
    ($between_name:ident, $data_type:ident, $type:ty, $prop_accessor:ident) => {
        pub struct $between_name {
            upper: $type,
            lower: $type,
            property: Property,
        }

        impl Condition for $between_name {
            fn evaluate(&self, object: &[u8]) -> bool {
                let val = self.property.$prop_accessor(object);
                self.lower <= val && self.upper >= val
            }
        }

        impl $between_name {
            pub fn filter(property: Property, lower: $type, upper: $type) -> Result<Filter> {
                if property.data_type == crate::object::data_type::DataType::$data_type {
                    Ok(Filter::$data_type(Self {
                        property,
                        lower,
                        upper,
                    }))
                } else {
                    illegal_arg("Property does not support this filter.")
                }
            }
        }
    }
);

primitive_filter!(IntBetween, Int, i32, get_int);
primitive_filter!(LongBetween, Long, i64, get_long);
primitive_filter!(FloatBetween, Float, f32, get_float);
primitive_filter!(DoubleBetween, Double, f64, get_double);

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

pub struct And {
    filters: Vec<Filter>,
}

impl Condition for And {
    fn evaluate(&self, object: &[u8]) -> bool {
        for filter in &self.filters {
            if !filter.evaluate(object) {
                return false;
            }
        }
        true
    }
}

impl And {
    pub fn filter(filters: Vec<Filter>) -> Filter {
        Filter::And(And { filters })
    }
}

pub struct Or {
    filters: Vec<Filter>,
}

impl Condition for Or {
    fn evaluate(&self, object: &[u8]) -> bool {
        for filter in &self.filters {
            if filter.evaluate(object) {
                return true;
            }
        }
        false
    }
}

impl Or {
    pub fn filter(filters: Vec<Filter>) -> Filter {
        Filter::Or(Or { filters })
    }
}

pub struct Not {
    filter: Box<Filter>,
}

impl Condition for Not {
    fn evaluate(&self, object: &[u8]) -> bool {
        self.filter.evaluate(object)
    }
}

impl Not {
    pub fn filter(filter: Filter) -> Filter {
        Filter::Not(Not {
            filter: Box::new(filter),
        })
    }
}

/*pub struct Not {
    filter: Box<Filter>,
}

impl Condition for Not {
    fn evaluate(&self, object: &[u8]) -> bool {
        !self.filter.evaluate(object)
    }
}

impl Not {
    pub fn filter(filter: Filter) -> Filter {
        Filter::Not(Not {
            filter: Box::new(filter),
        })
    }
}

pub struct LinkFilter {
    property: Property,
    filter: Box<Filter>,
}

impl Condition for LinkFilter {
    fn evaluate(&self, object: &[u8]) -> bool {
        !self.filter.evaluate(object)
    }
}

impl LinkFilter {
    pub fn filter(filter: Filter) -> Filter {
        Filter::Not(Not {
            filter: Box::new(filter),
        })
    }
}
*/
