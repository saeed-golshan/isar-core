use once_cell::sync::OnceCell;

pub static DART_POST_C_OBJECT: OnceCell<DartPostCObjectFnType> = OnceCell::new();

pub type DartPort = i64;

pub type DartPostCObjectFnType =
    extern "C" fn(port_id: DartPort, message: *mut DartCObject) -> bool;

#[repr(C)]
pub struct DartCObject {
    pub ty: i32,
    pub value: DartCObjectValue,
}

impl DartCObject {
    pub fn from_int_i32(value: i32) -> Self {
        DartCObject {
            ty: 2,
            value: DartCObjectValue { as_int32: value },
        }
    }
}

#[repr(C)]
pub union DartCObjectValue {
    pub as_bool: bool,
    pub as_int32: i32,
    pub as_int64: i64,
    pub as_double: f64,
    _union_align: [u64; 5usize],
}
