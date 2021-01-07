use crate::object::object_id::ObjectId;
use crate::utils::seconds_since_epoch;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct ObjectIdGenerator {
    prefix: u16,
    counter_reset_time: AtomicU64,
    time: fn() -> u64,
    random: fn() -> u64,
}

impl ObjectIdGenerator {
    pub fn new(prefix: u16) -> Self {
        ObjectIdGenerator {
            prefix,
            counter_reset_time: AtomicU64::new(0),
            time: seconds_since_epoch,
            random: rand::random,
        }
    }

    #[cfg(test)]
    pub fn new_debug(prefix: u16, time: fn() -> u64, random: fn() -> u64) -> Self {
        ObjectIdGenerator {
            prefix,
            counter_reset_time: AtomicU64::new(0),
            time,
            random,
        }
    }

    pub fn generate(&self) -> ObjectId {
        let current_time = ((self.time)() & 0xFFFFFFFF) as u32;

        let counter_time = self.counter_reset_time.load(Ordering::Relaxed);
        let time = (counter_time >> 32) as u32;
        let (oid_time, oid_counter) = if time != current_time {
            let val = (current_time as u64) << 32;
            self.counter_reset_time.store(val, Ordering::Relaxed);
            (current_time, 0)
        } else {
            let counter_time = self.counter_reset_time.fetch_add(1, Ordering::Relaxed);
            ((counter_time >> 32) as u32, counter_time as u32 + 1)
        };

        let random_number: u64 = (self.random)();
        let rand_counter = (oid_counter as u16).to_be() as u64 | random_number << 16;

        ObjectId::new(self.prefix, oid_time, rand_counter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate() {
        let mut oidg = ObjectIdGenerator::new_debug(
            u16::from_be_bytes([1, 2]),
            || u32::from_be_bytes([3, 4, 5, 6]) as u64,
            || u64::from_be_bytes([7, 8, 9, 10, 11, 12, 13, 14]),
        );

        assert_eq!(
            oidg.generate().as_bytes(),
            [2, 1, 3, 4, 5, 6, 0, 0, 14, 13, 12, 11, 10, 9]
        );
        assert_eq!(
            oidg.generate().as_bytes(),
            [2, 1, 3, 4, 5, 6, 0, 1, 14, 13, 12, 11, 10, 9]
        );
        assert_eq!(
            oidg.generate().as_bytes(),
            [2, 1, 3, 4, 5, 6, 0, 2, 14, 13, 12, 11, 10, 9]
        );

        oidg.time = || u32::from_be_bytes([3, 4, 5, 7]) as u64;

        assert_eq!(
            oidg.generate().as_bytes(),
            [2, 1, 3, 4, 5, 7, 0, 0, 14, 13, 12, 11, 10, 9]
        );
        assert_eq!(
            oidg.generate().as_bytes(),
            [2, 1, 3, 4, 5, 7, 0, 1, 14, 13, 12, 11, 10, 9]
        );
        assert_eq!(
            oidg.generate().as_bytes(),
            [2, 1, 3, 4, 5, 7, 0, 2, 14, 13, 12, 11, 10, 9]
        );
    }
}
