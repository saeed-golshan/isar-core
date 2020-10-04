use crate::utils::seconds_since_epoch;
use std::convert::TryInto;

pub struct ObjectIdGenerator {
    counter: u16,
    time: fn() -> u64,
    random: fn() -> u64,
}

impl ObjectIdGenerator {
    pub fn new(counter: u16) -> Self {
        ObjectIdGenerator {
            counter,
            time: seconds_since_epoch,
            random: rand::random,
        }
    }

    #[cfg(test)]
    pub fn new_debug(counter: u16, time: fn() -> u64, random: fn() -> u64) -> Self {
        ObjectIdGenerator {
            counter,
            time,
            random,
        }
    }

    pub fn generate(&mut self) -> ObjectId {
        let time = (self.time)();
        let random_number: u64 = (self.random)();
        let rand_counter = random_number << 16 | self.counter as u64;
        self.counter = self.counter.wrapping_add(1);

        ObjectId::new((time & 0xFFFFFFFF) as u32, rand_counter)
    }
}

#[derive(Copy, Clone)]
pub struct ObjectId {
    time: u32,
    rand_counter: u64,
}

impl ObjectId {
    pub fn new(time: u32, rand_counter: u64) -> Self {
        ObjectId { time, rand_counter }
    }

    pub fn get_time(&self) -> u32 {
        self.time
    }

    pub fn get_rand_counter(&self) -> u64 {
        self.rand_counter
    }

    pub fn to_bytes(&self) -> &[u8] {
        unsafe {
            ::std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                ::std::mem::size_of::<Self>(),
            )
        }
    }

    pub fn to_bytes_with_prefix(&self, prefix: &[u8]) -> Vec<u8> {
        let mut bytes = prefix.to_vec();
        bytes.extend_from_slice(&self.to_bytes());
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
    fn test_object_id_generator_generate() {
        let mut oidg = ObjectIdGenerator::new_debug(555, || 1231231231, || 12345);

        let oid = oidg.generate();
        assert_eq!(oid.get_time(), 1231231231);
        assert_eq!(oid.get_rand_counter(), 809042475);

        let oid = oidg.generate();
        assert_eq!(oid.get_time(), 1231231231);
        assert_eq!(oid.get_rand_counter(), 809042476);

        let oid = oidg.generate();
        assert_eq!(oid.get_time(), 1231231231);
        assert_eq!(oid.get_rand_counter(), 809042477);
    }
}
