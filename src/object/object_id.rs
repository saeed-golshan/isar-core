use std::mem;

#[derive(Copy, Clone, Eq, Debug)]
#[repr(packed)]
pub struct ObjectId {
    prefix: u16,
    time: u32, // big endian
    rand_counter: u64,
}

impl ObjectId {
    pub const fn get_size() -> usize {
        mem::size_of::<ObjectId>()
    }

    pub fn new(prefix: u16, time: u32, rand_counter: u64) -> Self {
        ObjectId {
            prefix,
            time: time.to_be(),
            rand_counter,
        }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> &Self {
        let (_, body, _) = unsafe { bytes.align_to::<Self>() };
        &body[0]
    }

    pub fn get_time(&self) -> u32 {
        self.time.to_be()
    }

    pub fn get_rand_counter(&self) -> u64 {
        self.rand_counter
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        let bytes = unsafe {
            ::std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                ::std::mem::size_of::<Self>(),
            )
        };
        &bytes
    }
}

impl PartialEq for ObjectId {
    fn eq(&self, other: &Self) -> bool {
        other.time == self.time && other.rand_counter == self.rand_counter
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_as_bytes() {
        /*let mut oid = ObjectId::new(123, 222);
        assert_eq!(
            oid.as_bytes(99),
            &[99, 0, 0, 0, 0, 123, 222, 0, 0, 0, 0, 0, 0, 0]
        )*/
    }
}
