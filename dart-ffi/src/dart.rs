use once_cell::sync::OnceCell;

static DART_POST_C_OBJECT: OnceCell<DartPostCObjectFnType> = OnceCell::new();

pub fn dart_post_int(port: DartPort, value: i32) {
    let dart_post = DART_POST_C_OBJECT.get().unwrap();
    dart_post(port, &mut Dart_CObject::from_int_i32(value));
}

pub type DartPort = i64;

pub type DartPostCObjectFnType = extern "C" fn(port_id: DartPort, message: *mut Dart_CObject) -> i8;

#[repr(C)]
pub struct Dart_CObject {
    ty: i32,
    value: DartCObjectValue,
}

impl Dart_CObject {
    fn from_int_i32(value: i32) -> Self {
        Dart_CObject {
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
pub unsafe extern "C" fn isar_connect_dart_api(ptr: DartPostCObjectFnType) {
    let _ = DART_POST_C_OBJECT.set(ptr);
}
