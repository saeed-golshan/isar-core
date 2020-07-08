#![feature(type_alias_impl_trait)]

use crate::error::Result;
use crate::lmdb::cursor::{Cursor, CursorIterator};
use crate::lmdb::KeyVal;

#[derive(Clone)]
pub struct KeyRange {
    lower_key: Option<Vec<u8>>,
    upper_key: Option<Vec<u8>>,
}

impl KeyRange {
    pub fn new(lower_key: Option<Vec<u8>>, upper_key: Option<Vec<u8>>) -> Self {
        KeyRange {
            lower_key,
            upper_key,
        }
    }

    pub fn iter<'a, 'txn>(&'a self, cursor: Cursor<'txn>) -> Result<KeyRangeIterator<'a, 'txn>> {
        KeyRangeIterator::new(&self, cursor)
    }

    pub fn is_unbound(&self) -> bool {
        self.is_unbound_left() && self.is_unbound_right()
    }

    pub fn is_unbound_left(&self) -> bool {
        self.lower_key.is_none()
    }

    pub fn is_unbound_right(&self) -> bool {
        self.upper_key.is_none()
    }

    pub fn contains(&self, other: &KeyRange) -> bool {
        if let Some(lower_key) = self.lower_key.as_ref() {
            if let Some(other_lower_key) = other.lower_key.as_ref() {
                if lower_key > other_lower_key {
                    return false;
                }
            }
        } else if other.is_unbound_left() {
            return false;
        }

        if let Some(upper_key) = self.upper_key.as_ref() {
            if let Some(other_upper_key) = other.upper_key.as_ref() {
                if upper_key < other_upper_key {
                    return false;
                }
            }
        } else if other.is_unbound_right() {
            return false;
        }

        true
    }

    pub fn add_prefix(&mut self, prefix: &[u8]) {
        if let Some(lower_key) = &mut self.lower_key {
            lower_key.splice(0..0, prefix.iter().cloned());
        } else {
            self.lower_key = Some(prefix.to_vec());
        }
        if let Some(upper_key) = &mut self.upper_key {
            upper_key.splice(0..0, prefix.iter().cloned());
        } else {
            self.upper_key = Some(prefix.to_vec());
        }
    }
}

pub struct KeyRangeIterator<'a, 'txn> {
    range: &'a KeyRange,
    iter: CursorIterator<'txn>,
}

impl<'a, 'txn> KeyRangeIterator<'a, 'txn> {
    pub fn new(range: &'a KeyRange, cursor: Cursor<'txn>) -> Result<Self> {
        if let Some(lower_key) = &range.lower_key {
            cursor.move_to_key_greater_than_or_equal_to(lower_key)?;
        } else {
            cursor.move_to_first()?;
        }
        Ok(KeyRangeIterator {
            range,
            iter: cursor.iter(),
        })
    }
}

impl<'a, 'txn> Iterator for KeyRangeIterator<'a, 'txn> {
    type Item = Result<KeyVal<'txn>>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        if let Option::Some(upper_key) = &self.range.upper_key {
            match next? {
                Ok((key, val)) => {
                    if key <= upper_key {
                        Some(Ok((key, val)))
                    } else {
                        None
                    }
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            next
        }
    }
}
