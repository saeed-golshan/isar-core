use crate::field::Field;
use enum_dispatch::enum_dispatch;

pub enum Case {
    Sensitive,
    Insensitive,
}

#[enum_dispatch]
pub enum Filter<'a> {
    EqualsNull(EqualsNull<'a>),
    NonNullFilter(NonNullFilter<'a>),
    IntBetween(IntBetween<'a>),
    IntAnyOf(IntAnyOf<'a>),
    DoubleBetween(DoubleBetween<'a>),
    DoubleAnyOf(DoubleAnyOf<'a>),
    StrAnyOf(StrAnyOf<'a>),
    /*StrStartsWith(),
    StrEndsWith(),
    StrContains(),*/
    And(And<'a>),
    Or(Or<'a>),
    Not(Not<'a>),
}

/*impl<'a> Filter<'a> {
    fn null_safe(self) -> Filter<'a> {
        Filter::NonNullFilter(NonNullFilter {
            field: self.get_field().unwrap(),
            filter: Box::new(self),
        })
    }
}*/

#[enum_dispatch(Filter)]
trait Condition {
    fn evaluate(&self, buf: &[u8]) -> bool;

    fn get_field(&self) -> Option<&Field>;
}

pub struct EqualsNull<'a> {
    field: &'a Field,
    is_null: bool,
}

impl<'a> Condition for EqualsNull<'a> {
    fn evaluate(&self, buf: &[u8]) -> bool {
        let null = self.field.is_null(buf);
        self.is_null == null
    }

    fn get_field(&self) -> Option<&Field> {
        Some(self.field)
    }
}

impl<'a> EqualsNull<'a> {
    pub fn filter(field: &'a Field, is_null: bool) -> Filter {
        Filter::EqualsNull(EqualsNull { field, is_null })
    }
}

pub struct NonNullFilter<'a> {
    field: &'a Field,
    filter: Box<Filter<'a>>,
}

impl<'a> Condition for NonNullFilter<'a> {
    fn evaluate(&self, buf: &[u8]) -> bool {
        if !self.field.is_null(buf) {
            self.filter.evaluate(buf)
        } else {
            false
        }
    }

    fn get_field(&self) -> Option<&Field> {
        Some(self.field)
    }
}

pub struct IntBetween<'a> {
    upper: i64,
    lower: i64,
    field: &'a Field,
}

impl<'a> Condition for IntBetween<'a> {
    fn evaluate(&self, buf: &[u8]) -> bool {
        let int = self.field.get_int(buf);
        self.lower <= int && self.upper >= int
    }

    fn get_field(&self) -> Option<&Field> {
        Some(self.field)
    }
}

impl<'a> IntBetween<'a> {
    pub fn filter(field: &'a Field, lower: i64, upper: i64) -> Filter {
        Filter::IntBetween(IntBetween {
            field,
            lower,
            upper,
        })
    }
}

pub struct IntAnyOf<'a> {
    field: &'a Field,
    values: Vec<i64>,
}

impl<'a> Condition for IntAnyOf<'a> {
    fn evaluate(&self, buf: &[u8]) -> bool {
        let int = self.field.get_int(buf);
        self.values.iter().any(|v| *v == int)
    }

    fn get_field(&self) -> Option<&Field> {
        Some(self.field)
    }
}

impl<'a> IntAnyOf<'a> {
    pub fn filter(field: &'a Field, values: Vec<i64>) -> Filter {
        Filter::IntAnyOf(IntAnyOf { field, values })
    }
}

pub struct DoubleBetween<'a> {
    upper: f64,
    lower: f64,
    field: &'a Field,
}

impl<'a> Condition for DoubleBetween<'a> {
    fn evaluate(&self, buf: &[u8]) -> bool {
        let double = self.field.get_double(buf);
        self.lower <= double && self.upper >= double
    }

    fn get_field(&self) -> Option<&Field> {
        Some(self.field)
    }
}

impl<'a> DoubleBetween<'a> {
    pub fn filter(field: &'a Field, lower: f64, upper: f64) -> Filter {
        Filter::DoubleBetween(DoubleBetween {
            field,
            lower,
            upper,
        })
    }
}

pub struct DoubleAnyOf<'a> {
    field: &'a Field,
    values: Vec<f64>,
}

impl<'a> Condition for DoubleAnyOf<'a> {
    fn evaluate(&self, buf: &[u8]) -> bool {
        let int = self.field.get_double(buf);
        self.values.iter().any(|v| *v == int)
    }

    fn get_field(&self) -> Option<&Field> {
        Some(self.field)
    }
}

impl<'a> DoubleAnyOf<'a> {
    pub fn filter(field: &'a Field, values: Vec<f64>) -> Filter {
        Filter::DoubleAnyOf(DoubleAnyOf { field, values })
    }
}

pub struct StrAnyOf<'a> {
    field: &'a Field,
    values: Vec<&'a [u8]>,
    case: Case,
}

impl<'a> Condition for StrAnyOf<'a> {
    fn evaluate(&self, buf: &[u8]) -> bool {
        let str = self.field.get_bytes(buf);
        match self.case {
            Case::Sensitive => self.values.iter().any(|str_item| *str_item == str),
            Case::Insensitive => self.values.iter().any(|str_item| true),
        }
    }

    fn get_field(&self) -> Option<&Field> {
        Some(self.field)
    }
}

impl<'a> StrAnyOf<'a> {
    pub fn filter(field: &'a Field, values: Vec<&'a [u8]>, case: Case) -> Filter<'a> {
        Filter::StrAnyOf(StrAnyOf {
            field,
            values,
            case,
        })
    }
}

pub struct And<'a> {
    filters: Vec<Filter<'a>>,
}

impl<'a> Condition for And<'a> {
    fn evaluate(&self, buf: &[u8]) -> bool {
        for filter in &self.filters {
            if !filter.evaluate(buf) {
                return false;
            }
        }
        true
    }

    fn get_field(&self) -> Option<&Field> {
        None
    }
}

impl<'a> And<'a> {
    pub fn filter(filters: Vec<Filter<'a>>) -> Filter {
        Filter::And(And { filters })
    }
}

pub struct Or<'a> {
    filters: Vec<Filter<'a>>,
}

impl<'a> Condition for Or<'a> {
    fn evaluate(&self, buf: &[u8]) -> bool {
        for filter in &self.filters {
            if filter.evaluate(buf) {
                return true;
            }
        }
        false
    }

    fn get_field(&self) -> Option<&Field> {
        None
    }
}

impl<'a> Or<'a> {
    pub fn filter(filters: Vec<Filter<'a>>) -> Filter {
        Filter::Or(Or { filters })
    }
}

pub struct Not<'a> {
    filter: Box<Filter<'a>>,
}

impl<'a> Condition for Not<'a> {
    fn evaluate(&self, buf: &[u8]) -> bool {
        !self.filter.evaluate(buf)
    }

    fn get_field(&self) -> Option<&Field> {
        None
    }
}

impl<'a> Not<'a> {
    pub fn filter(filter: Filter<'a>) -> Filter {
        Filter::Not(Not {
            filter: Box::new(filter),
        })
    }
}
