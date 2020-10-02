use rand::distributions::{Distribution, Standard};

#[cfg(not(test))]
pub fn random<T>() -> T
where
    Standard: Distribution<T>,
{
    rand::random()
}

#[cfg(test)]
pub mod mock_rand {
    use super::*;
    use rand::{Error, Rng, RngCore};
    use std::cell::RefCell;

    thread_local! {
        static MOCK_RAND: RefCell<Option<StaticRng>> = RefCell::new(Some(StaticRng {num: 5}));
    }

    pub fn random<T>() -> T
    where
        Standard: Distribution<T>,
    {
        MOCK_RAND.with(|cell| cell.borrow_mut().as_mut().unwrap().gen())
    }

    struct StaticRng {
        pub num: u64,
    }

    impl RngCore for StaticRng {
        fn next_u32(&mut self) -> u32 {
            self.num as u32
        }

        fn next_u64(&mut self) -> u64 {
            self.num
        }

        fn fill_bytes(&mut self, dest: &mut [u8]) {
            unimplemented!()
        }

        fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), Error> {
            unimplemented!()
        }
    }

    pub fn set_static_rand(num: u64) {
        MOCK_RAND.with(|cell| *cell.borrow_mut() = Some(StaticRng { num }));
    }
}

#[cfg(test)]
pub use mock_rand::random;
#[cfg(test)]
pub use mock_rand::set_static_rand;
