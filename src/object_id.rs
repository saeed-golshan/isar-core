use crate::error::IsarError;
use crate::error::Result;
use crate::utils::mockable_rand::random;
use crate::utils::mockable_time::time_now;
use std::convert::TryInto;
use std::time::UNIX_EPOCH;

pub struct ObjectIdGenerator {
    counter: u16,
}

impl ObjectIdGenerator {
    pub fn new(counter: u16) -> Self {
        ObjectIdGenerator { counter }
    }

    pub fn generate(&mut self) -> Result<ObjectId> {
        let time = time_now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| IsarError::Error {
                source: Some(Box::new(e)),
                message: "Could not acquire system time.".to_string(),
            })?
            .as_secs();
        let random_number: u64 = random();
        let rand_counter = random_number << 16 | self.counter as u64;
        self.counter = self.counter.wrapping_add(1);

        Ok(ObjectId::new((time & 0xFFFFFFFF) as u32, rand_counter))
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

    pub fn to_bytes_with_prefix(&self, prefix: u16) -> Vec<u8> {
        let mut bytes = Vec::from(u16::to_be_bytes(prefix));
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
    use crate::utils::mockable_time::mock_time::set_mock_time;
    use std::ops::Add;
    use std::time::Duration;

    #[test]
    fn test_object_id_generator_generate() {
        let fake_time = UNIX_EPOCH.add(Duration::from_secs(1231231231));
        set_mock_time(Some(fake_time));

        let mut oidg = ObjectIdGenerator::new(555);

        let oid = oidg.generate().unwrap();
        assert_eq!(oid.get_time(), 1231231231);
        assert_eq!(oid.get_rand_counter(), 65534);

        let oid = oidg.generate().unwrap();
        assert_eq!(oid.get_time(), 1231231231);
        assert_eq!(oid.get_rand_counter(), 65535);

        let oid = oidg.generate().unwrap();
        assert_eq!(oid.get_time(), 1231231231);
        assert_eq!(oid.get_rand_counter(), 65535);
    }
}
