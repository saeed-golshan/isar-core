use crate::object_id::ObjectId;
use std::{ptr, slice};

#[repr(C)]
pub struct RawOid(u32, u64);

#[repr(C)]
pub struct RawObject {
    oid: RawOid,
    data: *const u8,
    data_length: u32,
}

impl RawObject {
    pub fn new(oid: &ObjectId, object: &[u8]) -> Self {
        let mut obj = RawObject {
            oid: RawOid(oid.get_time(), oid.get_rand_counter()),
            data: ptr::null(),
            data_length: 0,
        };
        obj.set_object(object);
        obj
    }

    pub fn set_object(&mut self, object: &[u8]) {
        let data_length = object.len() as u32;
        let data = object as *const _ as *const u8;
        self.data = data;
        self.data_length = data_length;
    }

    pub fn set_object_id(&mut self, oid: &ObjectId) {
        self.oid = RawOid(oid.get_time(), oid.get_rand_counter());
    }

    pub fn set_empty(&mut self) {
        self.oid = RawOid(0, 0);
        self.data = ptr::null();
        self.data_length = 0;
    }

    pub fn object_as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data, self.data_length as usize) }
    }

    pub fn get_object_id(&self) -> Option<ObjectId> {
        if self.oid.0 != 0 {
            Some(ObjectId::new(self.oid.0, self.oid.1))
        } else {
            None
        }
    }
}

#[repr(C)]
pub struct ObjectSet {
    objects: *mut RawObject,
    length: u32,
}

impl ObjectSet {
    pub fn new(mut objects: Vec<RawObject>) -> Self {
        objects.shrink_to_fit();
        let objects_ptr = objects.as_mut_ptr();
        ObjectSet {
            objects: objects_ptr,
            length: objects.len() as u32,
        }
    }

    /*pub fn get_object(&self, index: u32) -> Option<(u64, &[u8])> {
        if self.length > index {
            let object = unsafe { &*self.objects.offset(index as isize) };
            let slice = object.object_as_slice();
            Some((object.oid, slice))
        } else {
            None
        }
    }

    pub fn set_oid(&self, index: u32, oid: u64) {
        if self.length > index {
            let object = unsafe { &mut *self.objects.offset(index as isize) };
            object.oid = oid;
        }
    }*/

    pub fn length(&self) -> u32 {
        self.length
    }
}
