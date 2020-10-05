use crate::object::property::Property;
use enum_dispatch::enum_dispatch;
use itertools::Itertools;
use unicase::UniCase;

#[derive(Eq, PartialEq)]
pub enum Case {
    Sensitive,
    Insensitive,
}

#[enum_dispatch]
pub enum Filter {
    EqualsNull(EqualsNull),
    NonNullFilter(NonNullFilter),
    IntBetween(IntBetween),
    IntAnyOf(IntAnyOf),
    DoubleBetween(DoubleBetween),
    DoubleAnyOf(DoubleAnyOf),
    //StrAnyOf(StrAnyOf),
    /*StrStartsWith(),
    StrEndsWith(),
    StrContains(),*/
    And(And),
    Or(Or),
    Not(Not),
}

/*impl Filter {
    fn null_safe(self) -> Filter {
        Filter::NonNullFilter(NonNullFilter {
            property: self.get_property().unwrap(),
            filter: Box::new(self),
        })
    }
}*/

#[enum_dispatch(Filter)]
trait Condition {
    fn evaluate(&self, object: &[u8]) -> bool;
}

pub struct EqualsNull {
    property: Property,
    is_null: bool,
}

impl Condition for EqualsNull {
    fn evaluate(&self, object: &[u8]) -> bool {
        let null = self.property.is_null(object);
        self.is_null == null
    }
}

impl EqualsNull {
    pub fn filter(property: Property, is_null: bool) -> Filter {
        Filter::EqualsNull(EqualsNull { property, is_null })
    }
}

pub struct NonNullFilter {
    property: Property,
    filter: Box<Filter>,
}

impl Condition for NonNullFilter {
    fn evaluate(&self, object: &[u8]) -> bool {
        if !self.property.is_null(object) {
            self.filter.evaluate(object)
        } else {
            false
        }
    }
}

pub struct IntBetween {
    upper: i64,
    lower: i64,
    property: Property,
}

impl Condition for IntBetween {
    fn evaluate(&self, object: &[u8]) -> bool {
        let int = self.property.get_int(object);
        self.lower <= int && self.upper >= int
    }
}

impl IntBetween {
    pub fn filter(property: Property, lower: i64, upper: i64) -> Filter {
        Filter::IntBetween(IntBetween {
            property,
            lower,
            upper,
        })
    }
}

pub struct IntAnyOf {
    property: Property,
    values: Vec<i64>,
}

impl Condition for IntAnyOf {
    fn evaluate(&self, object: &[u8]) -> bool {
        let int = self.property.get_int(object);
        self.values.iter().any(|v| *v == int)
    }
}

impl IntAnyOf {
    pub fn filter(property: Property, values: Vec<i64>) -> Filter {
        Filter::IntAnyOf(IntAnyOf { property, values })
    }
}

pub struct DoubleBetween {
    upper: f64,
    lower: f64,
    property: Property,
}

impl Condition for DoubleBetween {
    fn evaluate(&self, object: &[u8]) -> bool {
        let double = self.property.get_double(object);
        self.lower <= double && self.upper >= double
    }
}

impl DoubleBetween {
    pub fn filter(property: Property, lower: f64, upper: f64) -> Filter {
        Filter::DoubleBetween(DoubleBetween {
            property,
            lower,
            upper,
        })
    }
}

pub struct DoubleAnyOf {
    property: Property,
    values: Vec<f64>,
    epsilon: f64,
}

impl Condition for DoubleAnyOf {
    fn evaluate(&self, object: &[u8]) -> bool {
        let int = self.property.get_double(object);
        self.values.iter().any(|v| (*v - int).abs() < self.epsilon)
    }
}

impl DoubleAnyOf {
    pub fn filter(property: Property, values: Vec<f64>, epsilon: f64) -> Filter {
        Filter::DoubleAnyOf(DoubleAnyOf {
            property,
            values,
            epsilon,
        })
    }
}

pub struct StrAnyOf {
    property: Property,
    values: Vec<Option<Vec<u8>>>,
    case: Case,
}

/*impl StrAnyOf {
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
