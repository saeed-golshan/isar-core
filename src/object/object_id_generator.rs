use crate::object::object_id::ObjectId;
use crate::utils::seconds_since_epoch;
use std::cell::Cell;

pub struct ObjectIdGenerator {
    counter: Cell<u16>,
    time: fn() -> u64,
    random: fn() -> u64,
}

impl ObjectIdGenerator {
    pub fn new(counter: u16) -> Self {
        ObjectIdGenerator {
            counter: Cell::new(counter),
            time: seconds_since_epoch,
            random: rand::random,
        }
    }

    #[cfg(test)]
    pub fn new_debug(counter: u16, time: fn() -> u64, random: fn() -> u64) -> Self {
        ObjectIdGenerator {
            counter: Cell::new(counter),
            time,
            random,
        }
    }

    pub fn generate(&self) -> ObjectId {
        let time = (self.time)();
        let random_number: u64 = (self.random)();
        let rand_counter = random_number << 16 | self.counter.get() as u64;
        self.counter.set(self.counter.get().wrapping_add(1));

        ObjectId::new((time & 0xFFFFFFFF) as u32, rand_counter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate() {
        let oidg = ObjectIdGenerator::new_debug(555, || 1231231231, || 12345);

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
