use crate::field::Field;
use enum_dispatch::enum_dispatch;

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
    StrAnyOf(StrAnyOf),
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
            field: self.get_field().unwrap(),
            filter: Box::new(self),
        })
    }
}*/

#[enum_dispatch(Filter)]
trait Condition {
    fn evaluate(&self, object: &[u8]) -> bool;
}

pub struct EqualsNull {
    field: Field,
    is_null: bool,
}

impl Condition for EqualsNull {
    fn evaluate(&self, object: &[u8]) -> bool {
        let null = self.field.is_null(object);
        self.is_null == null
    }
}

impl EqualsNull {
    pub fn filter(field: Field, is_null: bool) -> Filter {
        Filter::EqualsNull(EqualsNull { field, is_null })
    }
}

pub struct NonNullFilter {
    field: Field,
    filter: Box<Filter>,
}

impl Condition for NonNullFilter {
    fn evaluate(&self, object: &[u8]) -> bool {
        if !self.field.is_null(object) {
            self.filter.evaluate(object)
        } else {
            false
        }
    }
}

pub struct IntBetween {
    upper: i64,
    lower: i64,
    field: Field,
}

impl Condition for IntBetween {
    fn evaluate(&self, object: &[u8]) -> bool {
        let int = self.field.get_int(object);
        self.lower <= int && self.upper >= int
    }
}

impl IntBetween {
    pub fn filter(field: Field, lower: i64, upper: i64) -> Filter {
        Filter::IntBetween(IntBetween {
            field,
            lower,
            upper,
        })
    }
}

pub struct IntAnyOf {
    field: Field,
    values: Vec<i64>,
}

impl Condition for IntAnyOf {
    fn evaluate(&self, object: &[u8]) -> bool {
        let int = self.field.get_int(object);
        self.values.iter().any(|v| *v == int)
    }
}

impl IntAnyOf {
    pub fn filter(field: Field, values: Vec<i64>) -> Filter {
        Filter::IntAnyOf(IntAnyOf { field, values })
    }
}

pub struct DoubleBetween {
    upper: f64,
    lower: f64,
    field: Field,
}

impl Condition for DoubleBetween {
    fn evaluate(&self, object: &[u8]) -> bool {
        let double = self.field.get_double(object);
        self.lower <= double && self.upper >= double
    }
}

impl DoubleBetween {
    pub fn filter(field: Field, lower: f64, upper: f64) -> Filter {
        Filter::DoubleBetween(DoubleBetween {
            field,
            lower,
            upper,
        })
    }
}

pub struct DoubleAnyOf {
    field: Field,
    values: Vec<f64>,
}

impl Condition for DoubleAnyOf {
    fn evaluate(&self, object: &[u8]) -> bool {
        let int = self.field.get_double(object);
        self.values.iter().any(|v| *v == int)
    }
}

impl DoubleAnyOf {
    pub fn filter(field: Field, values: Vec<f64>) -> Filter {
        Filter::DoubleAnyOf(DoubleAnyOf { field, values })
    }
}

pub struct StrAnyOf {
    field: Field,
    values: Vec<Vec<u8>>,
    case: Case,
}

impl Condition for StrAnyOf {
    fn evaluate(&self, object: &[u8]) -> bool {
        let str = self.field.get_bytes(object);
        match self.case {
            Case::Sensitive => self
                .values
                .iter()
                .any(|str_item| str_item.as_slice() == str),
            Case::Insensitive => self.values.iter().any(|str_item| true),
        }
    }
}

impl StrAnyOf {
    pub fn filter(field: Field, values: Vec<Vec<u8>>, case: Case) -> Filter {
        Filter::StrAnyOf(StrAnyOf {
            field,
            values,
            case,
        })
    }
}

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
    field: Field,
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
