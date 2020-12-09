use crate::collection::IsarCollection;
use crate::object::object_id::ObjectId;
use std::{ptr, slice};

#[repr(C)]
pub struct RawObjectId {
    time: u32,
    rand_counter: u64,
}

impl RawObjectId {
    pub fn from_object_id(oid: ObjectId) -> Self {
        RawObjectId {
            time: oid.get_time(),
            rand_counter: oid.get_rand_counter(),
        }
    }

    pub fn zero() -> Self {
        RawObjectId {
            time: 0,
            rand_counter: 0,
        }
    }

    pub fn get_object_id(&self, collection: &IsarCollection) -> ObjectId {
        collection.get_object_id(self.time, self.rand_counter)
    }
}

#[repr(C)]
pub struct RawObject {
    oid: RawObjectId,
    data: *const u8,
    data_length: u32,
}

impl RawObject {
    pub fn new(oid: ObjectId, object: &[u8]) -> Self {
        let mut obj = RawObject {
            oid: RawObjectId::from_object_id(oid),
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

    pub fn set_object_id(&mut self, oid: ObjectId) {
        self.oid = RawObjectId::from_object_id(oid);
    }

    pub fn set_empty(&mut self) {
        self.oid = RawObjectId::zero();
        self.data = ptr::null();
        self.data_length = 0;
    }

    pub fn object_as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data, self.data_length as usize) }
    }

    pub fn get_object_id(&self, col: &IsarCollection) -> Option<ObjectId> {
        if self.oid.time != 0 {
            Some(self.oid.get_object_id(col))
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
