use once_cell::sync::OnceCell;

static DART_POST_C_OBJECT: OnceCell<DartPostCObjectFnType> = OnceCell::new();

pub fn dart_post_int(port: DartPort, value: i32) {
    let dart_post = DART_POST_C_OBJECT.get().unwrap();
    dart_post(port, &mut DartCObject::from_int_i32(value));
}

pub type DartPort = i64;

pub type DartPostCObjectFnType =
    extern "C" fn(port_id: DartPort, message: *mut DartCObject) -> bool;

#[repr(C)]
pub struct DartCObject {
    ty: i32,
    value: DartCObjectValue,
}

impl DartCObject {
    fn from_int_i32(value: i32) -> Self {
        DartCObject {
            ty: 2,
            value: DartCObjectValue { as_int32: value },
        }
    }
}

#[repr(C)]
union DartCObjectValue {
    pub as_bool: bool,
    pub as_int32: i32,
    pub as_int64: i64,
    pub as_double: f64,
    _union_align: [u64; 5usize],
}

#[no_mangle]
unsafe extern "C" fn isar_connect_dart_api(ptr: DartPostCObjectFnType) {
    let _ = DART_POST_C_OBJECT.set(ptr);
}
