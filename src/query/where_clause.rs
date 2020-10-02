use crate::data_dbs::IndexType;
use crate::error::Result;
use crate::index::{Index, MAX_STRING_INDEX_SIZE};
use crate::lmdb::cursor::{Cursor, CursorIterator};
use crate::lmdb::KeyVal;
use std::convert::TryInto;

#[derive(Clone)]
pub struct WhereClause {
    pub lower_key: Vec<u8>,
    pub upper_key: Vec<u8>,
    pub index_type: IndexType,
}

impl WhereClause {
    pub fn new(prefix: &[u8], lower_size: usize, upper_size: usize, index_type: IndexType) -> Self {
        let mut lower_key = Vec::with_capacity(lower_size + prefix.len());
        lower_key.extend_from_slice(prefix);
        let mut upper_key = Vec::with_capacity(upper_size + prefix.len());
        upper_key.extend_from_slice(prefix);
        WhereClause {
            lower_key,
            upper_key,
            index_type,
        }
    }

    pub fn iter<'a, 'txn>(
        &'a self,
        cursor: &'a mut Cursor<'txn>,
    ) -> Result<WhereClauseIterator<'a, 'txn>> {
        WhereClauseIterator::new(&self, cursor)
    }

    pub fn contains(&self, other: &WhereClause) -> bool {
        self.lower_key <= other.lower_key && self.upper_key >= other.upper_key
    }

    pub fn add_lower_int(&mut self, mut value: i64, include: bool) {
        if !include {
            value += 1;
        }
        self.lower_key.extend_from_slice(&Index::get_int_key(value));
    }

    pub fn add_upper_int(&mut self, mut value: i64, include: bool) {
        if !include {
            value -= 1;
        }
        self.upper_key.extend_from_slice(&Index::get_int_key(value));
    }

    pub fn add_lower_double(&mut self, value: f64, include: bool) {
        let mut key = Index::get_double_key(value);
        if !include {
            let u64_key = u64::from_be_bytes(key.as_slice().try_into().unwrap());
            key = u64::to_be_bytes(u64_key + 1).to_vec();
        }
        self.lower_key.extend_from_slice(&key);
    }

    pub fn add_upper_double(&mut self, value: f64, include: bool) {
        let mut key = Index::get_double_key(value);
        if !include {
            let u64_key = u64::from_be_bytes(key.as_slice().try_into().unwrap());
            key = u64::to_be_bytes(u64_key - 1).to_vec();
        }
        self.upper_key.extend_from_slice(&key);
    }

    pub fn add_lower_bool(&mut self, value: bool) {
        self.lower_key
            .extend_from_slice(&Index::get_bool_key(value));
    }

    pub fn add_upper_bool(&mut self, value: bool) {
        self.upper_key
            .extend_from_slice(&Index::get_bool_key(value));
    }

    pub fn add_string_hash(&mut self, value: &str) {
        let hash = Index::get_string_hash_key(value.as_bytes());
        self.lower_key.extend_from_slice(&hash);
        self.upper_key.extend_from_slice(&hash);
    }

    pub fn add_lower_string_value(&mut self, value: &str, include: bool) {
        let str_bytes = value.as_bytes();
        let mut bytes = Index::get_string_value_key(str_bytes);

        if !include {
            assert!(str_bytes.len() < MAX_STRING_INDEX_SIZE);
            let bytes_len = bytes.len();
            bytes[bytes_len - 1] += 1;
        }

        self.lower_key.extend_from_slice(&bytes);
    }

    pub fn add_upper_string_value(&mut self, value: &str, include: bool) {
        let str_bytes = value.as_bytes();
        let mut bytes = Index::get_string_value_key(str_bytes);

        if !include {
            assert!(str_bytes.len() < MAX_STRING_INDEX_SIZE);
            assert!(str_bytes.len() > 0);
            let bytes_len = bytes.len();
            bytes[bytes_len - 1] -= 1;
        }

        self.upper_key.extend_from_slice(&bytes);
    }
}

pub struct WhereClauseIterator<'a, 'txn> {
    where_clause: &'a WhereClause,
    iter: CursorIterator<'a, 'txn>,
}

impl<'a, 'txn> WhereClauseIterator<'a, 'txn> {
    pub fn new(where_clause: &'a WhereClause, cursor: &'a mut Cursor<'txn>) -> Result<Self> {
        cursor.move_to_key_greater_than_or_equal_to(&where_clause.lower_key)?;
        Ok(WhereClauseIterator {
            where_clause,
            iter: cursor.iter(),
        })
    }
}

impl<'a, 'txn> Iterator for WhereClauseIterator<'a, 'txn> {
    type Item = Result<KeyVal<'txn>>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        match next? {
            Ok((key, val)) => {
                if key <= &self.where_clause.upper_key {
                    Some(Ok((key, val)))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}
