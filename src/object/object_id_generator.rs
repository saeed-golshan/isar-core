use crate::object::object_id::ObjectId;
use crate::utils::seconds_since_epoch;
use std::cell::Cell;

pub struct ObjectIdGenerator {
    prefix: u16,
    counter: Cell<u16>,
    counter_reset_time: Cell<u32>,
    time: fn() -> u64,
    random: fn() -> u64,
}

impl ObjectIdGenerator {
    pub fn new(prefix: u16) -> Self {
        ObjectIdGenerator {
            prefix,
            counter: Cell::new(0),
            counter_reset_time: Cell::new(0),
            time: seconds_since_epoch,
            random: rand::random,
        }
    }

    #[cfg(test)]
    pub fn new_debug(prefix: u16, time: fn() -> u64, random: fn() -> u64) -> Self {
        ObjectIdGenerator {
            prefix,
            counter: Cell::new(0),
            counter_reset_time: Cell::new(0),
            time,
            random,
        }
    }

    pub fn generate(&self) -> ObjectId {
        let precise_time = (self.time)();
        let time = (precise_time & 0xFFFFFFFF) as u32;
        let counter = if self.counter_reset_time.get() != time {
            self.counter_reset_time.set(time);
            self.counter.set(0);
            0
        } else {
            self.counter.get()
        };
        let random_number: u64 = (self.random)();
        let rand_counter = counter.to_be() as u64 | random_number << 16;
        self.counter.set(self.counter.get().wrapping_add(1));

        ObjectId::new(self.prefix, time, rand_counter)
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
