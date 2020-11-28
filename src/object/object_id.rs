#[derive(Copy, Clone, Eq, Debug)]
#[repr(packed)]
pub struct ObjectId {
    prefix: u16,
    time: u32, // big endian
    rand_counter: u64,
    _padding: u16,
}

impl ObjectId {
    pub fn new(time: u32, rand_counter: u64) -> Self {
        ObjectId {
            prefix: 0,
            time: time.to_be(),
            rand_counter,
            _padding: 0,
        }
    }

    pub(crate) fn from_bytes_with_prefix_padding(bytes: &[u8]) -> &Self {
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
        &bytes[2..14]
    }

    #[inline]
    pub fn as_bytes_with_prefix(&mut self, prefix: u16) -> &[u8] {
        self.prefix = prefix;
        let bytes = unsafe {
            ::std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                ::std::mem::size_of::<Self>(),
            )
        };
        &bytes[..14]
    }

    #[inline]
    pub fn as_bytes_with_prefix_padding(&mut self, prefix: u16) -> &[u8] {
        self.prefix = prefix;
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
    use super::*;

    #[test]
    fn test_as_bytes() {
        let oid = ObjectId::new(123, 222);
        assert_eq!(oid.as_bytes(), &[0, 0, 0, 123, 222, 0, 0, 0, 0, 0, 0, 0])
    }

    #[test]
    fn test_as_bytes_prefix() {
        let mut oid = ObjectId::new(123, 222);
        assert_eq!(
            oid.as_bytes_with_prefix(99),
            &[99, 0, 0, 0, 0, 123, 222, 0, 0, 0, 0, 0, 0, 0]
        )
    }

    #[test]
    fn test_as_bytes_prefix_padding() {
        let mut oid = ObjectId::new(123, 222);
        assert_eq!(
            oid.as_bytes_with_prefix_padding(99),
            &[99, 0, 0, 0, 0, 123, 222, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        )
    }
}
