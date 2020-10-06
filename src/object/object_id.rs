use std::convert::TryInto;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct ObjectId {
    _padding: u32,
    time: u32,
    rand_counter: u64,
}

impl ObjectId {
    pub fn new(time: u32, rand_counter: u64) -> Self {
        ObjectId {
            time,
            rand_counter,
            _padding: 0,
        }
    }

    pub fn get_time(&self) -> u32 {
        self.time
    }

    pub fn get_rand_counter(&self) -> u64 {
        self.rand_counter
    }

    pub fn to_bytes(&self) -> &[u8] {
        let bytes = unsafe {
            ::std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                ::std::mem::size_of::<Self>(),
            )
        };
        &bytes[4..16]
    }

    pub fn to_bytes_with_prefix(&self, prefix: &[u8]) -> Vec<u8> {
        let mut bytes = prefix.to_vec();
        bytes.extend_from_slice(&self.to_bytes());
        bytes.extend_from_slice(&[0, 0]);
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let (_, body, _) = unsafe { bytes.align_to::<Self>() };
        body[0]
    }

    pub fn from_bytes_with_prefix(bytes: &[u8]) -> (u16, Self) {
        let prefix = u16::from_be_bytes(bytes.try_into().unwrap());
        let oid = Self::from_bytes(&bytes[2..]);
        (prefix, oid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_bytes() {
        let oid = ObjectId::new(123, 222);
        assert_eq!(oid.to_bytes(), &[123, 0, 0, 0, 222, 0, 0, 0, 0, 0, 0, 0])
    }
}
