use crate::error::IsarError;
use crate::error::Result;
use crate::utils::mockable_time::time_now;
use std::convert::TryInto;
use std::time::UNIX_EPOCH;

pub struct ObjectIdGenerator {
    counter: u16,
    bank_id: u16,
}

impl ObjectIdGenerator {
    pub fn new(counter: u16, bank_id: u16) -> Self {
        ObjectIdGenerator { counter, bank_id }
    }

    pub fn generate(&mut self) -> Result<ObjectId> {
        let time = time_now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| IsarError::Error {
                source: Some(Box::new(e)),
                message: "Could not acquire system time.".to_string(),
            })?
            .as_secs();
        let id = (self.bank_id as u64) << 48 | (time & 0xFFFFFFFF) << 16 | self.counter as u64;
        self.counter = self.counter.wrapping_add(1);

        Ok(ObjectId::new(id))
    }
}

pub struct ObjectId(pub u64);

impl ObjectId {
    pub fn new(id: u64) -> Self {
        ObjectId(id)
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        ObjectId(u64::from_be_bytes(bytes.try_into().unwrap()))
    }

    pub fn get_bank_id(&self) -> u16 {
        (self.0 >> 48) as u16
    }

    pub fn get_time(&self) -> u32 {
        (self.0 >> 16) as u32
    }

    pub fn get_counter(&self) -> u16 {
        self.0 as u16
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        u64::to_be_bytes(self.0)
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

        let mut oidg = ObjectIdGenerator::new(65534, 555);

        let oid = oidg.generate().unwrap();
        assert_eq!(oid.get_bank_id(), 555);
        assert_eq!(oid.get_time(), 1231231231);
        assert_eq!(oid.get_counter(), 65534);

        let oid = oidg.generate().unwrap();
        assert_eq!(oid.get_bank_id(), 555);
        assert_eq!(oid.get_time(), 1231231231);
        assert_eq!(oid.get_counter(), 65535);

        let oid = oidg.generate().unwrap();
        assert_eq!(oid.get_bank_id(), 555);
        assert_eq!(oid.get_time(), 1231231231);
        assert_eq!(oid.get_counter(), 65535);
    }
}
