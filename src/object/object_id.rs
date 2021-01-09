use std::mem;

#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash)]
#[repr(packed)]
pub struct ObjectId {
    prefix: u16,
    time: u32,    // big endian
    counter: u32, // big endian
    rand: u32,
}

impl ObjectId {
    pub const fn get_size() -> usize {
        mem::size_of::<ObjectId>()
    }

    pub fn new(prefix: u16, time: u32, counter: u32, rand: u32) -> Self {
        ObjectId {
            prefix,
            time: time.to_be(),
            counter: counter.to_be(),
            rand,
        }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> &Self {
        let (_, body, _) = unsafe { bytes.align_to::<Self>() };
        &body[0]
    }

    pub(crate) fn get_prefix(&self) -> u16 {
        self.prefix
    }

    pub fn get_time(&self) -> u32 {
        self.time.to_be()
    }

    pub fn get_counter(&self) -> u32 {
        self.counter.to_be()
    }

    pub fn get_rand(&self) -> u32 {
        self.rand
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

    #[inline]
    pub(crate) fn as_bytes_without_prefix(&self) -> &[u8] {
        &self.as_bytes()[2..]
    }
}

impl ToString for ObjectId {
    fn to_string(&self) -> String {
        hex::encode(self.as_bytes_without_prefix())
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
