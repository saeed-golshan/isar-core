#![feature(type_alias_impl_trait)]

use crate::error::Result;
use crate::index::Index;
use crate::lmdb::cursor::CursorIterator;
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::lmdb::KeyVal;

#[derive(Clone)]
pub struct WhereClause {
    lower_key: Vec<u8>,
    upper_key: Vec<u8>,
    db: Db,
    primary_index: bool,
    unique: bool,
}

impl WhereClause {
    pub fn new(
        prefix: &[u8],
        lower_size: usize,
        upper_size: usize,
        db: Db,
        primary_index: bool,
        unique: bool,
    ) -> Self {
        let mut lower_key = Vec::with_capacity(lower_size + prefix.len());
        lower_key.extend_from_slice(prefix);
        let mut upper_key = Vec::with_capacity(upper_size + prefix.len());
        upper_key.extend_from_slice(prefix);
        WhereClause {
            lower_key,
            upper_key,
            db,
            primary_index,
            unique,
        }
    }

    pub fn iter<'a, 'txn>(&'a self, txn: &'txn Txn) -> Result<WhereClauseIterator<'a, 'txn>> {
        WhereClauseIterator::new(&self, txn)
    }

    pub fn contains(&self, other: &WhereClause) -> bool {
        self.lower_key <= other.lower_key && self.upper_key >= other.upper_key
    }

    pub fn add_int(&mut self, lower: bool, value: i64) {
        let key = if lower {
            &mut self.lower_key
        } else {
            &mut self.upper_key
        };
        key.extend_from_slice(&Index::get_int_key(value));
    }

    pub fn add_double(&mut self, lower: bool, value: f64) {
        let key = if lower {
            &mut self.lower_key
        } else {
            &mut self.upper_key
        };
        key.extend_from_slice(&Index::get_double_key(value));
    }

    pub fn add_bool(&mut self, lower: bool, value: bool) {
        let key = if lower {
            &mut self.lower_key
        } else {
            &mut self.upper_key
        };
        key.extend_from_slice(&Index::get_bool_key(value));
    }

    pub fn add_string_hash(&mut self, lower: bool, value: &str) {
        let key = if lower {
            &mut self.lower_key
        } else {
            &mut self.upper_key
        };
        key.extend_from_slice(&Index::get_string_hash_key(value.as_bytes()));
    }

    pub fn add_string_value(&mut self, lower: bool, value: &str) {
        let key = if lower {
            &mut self.lower_key
        } else {
            &mut self.upper_key
        };
        key.extend_from_slice(&Index::get_string_value_key(value.as_bytes()));
    }
}

pub struct WhereClauseIterator<'a, 'txn> {
    where_clause: &'a WhereClause,
    iter: CursorIterator<'txn>,
}

impl<'a, 'txn> WhereClauseIterator<'a, 'txn> {
    pub fn new(where_clause: &'a WhereClause, txn: &'txn Txn) -> Result<Self> {
        let cursor = where_clause.db.cursor(txn)?;
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
