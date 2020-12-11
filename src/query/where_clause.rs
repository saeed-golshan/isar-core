use crate::error::Result;
use crate::index::{Index, IndexType};
use crate::lmdb::cursor::{Cursor, CursorIterator};
use crate::lmdb::KeyVal;
use crate::object::object_id::ObjectId;
use std::convert::TryInto;

#[derive(Clone)]
pub struct WhereClause {
    lower_key: Vec<u8>,
    upper_key: Vec<u8>,
    pub(super) index_type: IndexType,
}

impl WhereClause {
    pub fn new(prefix: &[u8], index_type: IndexType) -> Self {
        WhereClause {
            lower_key: prefix.to_vec(),
            upper_key: prefix.to_vec(),
            index_type,
        }
    }

    pub fn iter<'a, 'txn>(
        &'a self,
        cursor: &'a mut Cursor<'txn>,
    ) -> Result<Option<WhereClauseIterator<'a, 'txn>>> {
        WhereClauseIterator::new(&self, cursor)
    }

    pub fn is_empty(&self) -> bool {
        !self.check_below_upper_key(&self.lower_key)
    }

    #[inline]
    fn check_below_upper_key(&self, mut key: &[u8]) -> bool {
        let upper_key: &[u8] = &self.upper_key;
        if upper_key.len() < key.len() {
            key = &key[0..self.upper_key.len()]
        }
        upper_key >= key
    }

    /*pub(super) fn merge(&self, other: &WhereClause) -> Option<WhereClause> {
        unimplemented!()
    }*/

    pub fn add_oid(&mut self, oid: ObjectId) {
        let bytes = oid.as_bytes_without_prefix();
        self.lower_key.extend_from_slice(bytes);
        self.upper_key.extend_from_slice(bytes);
    }

    pub fn add_lower_oid_time(&mut self, mut time: u32, include: bool) {
        if !include {
            time += 1;
        }
        self.lower_key.extend_from_slice(&time.to_be_bytes());
    }

    pub fn add_upper_oid_time(&mut self, mut time: u32, include: bool) {
        if !include {
            time -= 1;
        }
        self.upper_key.extend_from_slice(&time.to_be_bytes());
    }

    pub fn add_lower_int(&mut self, mut value: i32, include: bool) {
        if !include {
            value += 1;
        }
        self.lower_key.extend_from_slice(&Index::get_int_key(value));
    }

    pub fn add_upper_int(&mut self, mut value: i32, include: bool) {
        if !include {
            value -= 1;
        }
        self.upper_key.extend_from_slice(&Index::get_int_key(value));
    }

    pub fn add_lower_long(&mut self, mut value: i64, include: bool) {
        if !include {
            value += 1;
        }
        self.lower_key
            .extend_from_slice(&Index::get_long_key(value));
    }

    pub fn add_upper_long(&mut self, mut value: i64, include: bool) {
        if !include {
            value -= 1;
        }
        self.upper_key
            .extend_from_slice(&Index::get_long_key(value));
    }

    pub fn add_lower_float(&mut self, value: f32, include: bool) {
        let mut key = Index::get_float_key(value);
        if !include {
            let u32_key = u32::from_be_bytes(key.as_slice().try_into().unwrap());
            key = u32::to_be_bytes(u32_key + 1).to_vec();
        }
        self.lower_key.extend_from_slice(&key);
    }

    pub fn add_upper_float(&mut self, value: f32, include: bool) {
        let mut key = Index::get_float_key(value);
        if !include {
            let u32_key = u32::from_be_bytes(key.as_slice().try_into().unwrap());
            key = u32::to_be_bytes(u32_key - 1).to_vec();
        }
        self.upper_key.extend_from_slice(&key);
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

    pub fn add_bool(&mut self, value: Option<bool>) {
        let bytes = &Index::get_bool_key(value);
        self.lower_key.extend_from_slice(bytes);
        self.upper_key.extend_from_slice(bytes);
    }

    pub fn add_string_hash(&mut self, value: Option<&str>) {
        let str_bytes = value.map(|s| s.as_bytes());
        let hash = Index::get_string_hash_key(str_bytes);
        self.lower_key.extend_from_slice(&hash);
        self.upper_key.extend_from_slice(&hash);
    }

    pub fn add_lower_string_value(&mut self, value: Option<&str>, include: bool) {
        let str_bytes = value.map(|s| s.as_bytes());
        let mut bytes = Index::get_string_value_key(str_bytes);

        if !include {
            let bytes_len = bytes.len();
            bytes[bytes_len - 1] += 1;
        }

        self.lower_key.extend_from_slice(&bytes);
    }

    pub fn add_upper_string_value(&mut self, value: Option<&str>, include: bool) {
        let str_bytes = value.map(|s| s.as_bytes());
        let mut bytes = Index::get_string_value_key(str_bytes);

        if !include {
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
    fn new(where_clause: &'a WhereClause, cursor: &'a mut Cursor<'txn>) -> Result<Option<Self>> {
        let result = cursor.move_to_gte(&where_clause.lower_key)?;
        if result.is_some() {
            Ok(Some(WhereClauseIterator {
                where_clause,
                iter: cursor.iter(),
            }))
        } else {
            Ok(None)
        }
    }
}

impl<'a, 'txn> Iterator for WhereClauseIterator<'a, 'txn> {
    type Item = Result<KeyVal<'txn>>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        match next? {
            Ok((key, val)) => {
                if self.where_clause.check_below_upper_key(&key) {
                    Some(Ok((key, val)))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    //use super::*;
    //use itertools::Itertools;

    #[macro_export]
    macro_rules! exec_wc (
        ($txn:ident, $col:ident, $wc:ident, $res:ident) => {
            let mut cursor = $col.debug_get_index(0).debug_get_db().cursor(&$txn).unwrap();
            let $res = $wc.iter(&mut cursor)
                .unwrap()
                .map(Result::unwrap)
                .map(|(_, v)| v)
                .collect_vec();
        };
    );

    /*fn get_str_obj(col: &IsarCollection, str: &str) -> Vec<u8> {
        let mut ob = col.get_object_builder();
        ob.write_string(Some(str));
        ob.finish()
    }*/

    #[test]
    fn test_iter() {
        /*isar!(isar, col => col!(field => String; ind!(field)));

        let txn = isar.begin_txn(true).unwrap();
        let oid1 = col.put(&txn, None, &get_str_obj(&col, "aaaa")).unwrap();
        let oid2 = col.put(&txn, None, &get_str_obj(&col, "aabb")).unwrap();
        let oid3 = col.put(&txn, None, &get_str_obj(&col, "bbaa")).unwrap();
        let oid4 = col.put(&txn, None, &get_str_obj(&col, "bbbb")).unwrap();

        let all_oids = &[
            oid1.as_bytes(),
            oid2.as_bytes(),
            oid3.as_bytes(),
            oid4.as_bytes(),
        ];

        let mut wc = col.create_where_clause(Some(0)).unwrap();
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, all_oids);

        wc.add_lower_string_value(Some("aa"), true);
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, all_oids);

        let mut wc = col.create_where_clause(Some(0)).unwrap();
        wc.add_lower_string_value(Some("aa"), false);
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, &[oid3.as_bytes(), oid4.as_bytes()]);

        wc.add_upper_string_value(Some("bba"), true);
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, &[oid3.as_bytes()]);

        let mut wc = col.create_where_clause(Some(0)).unwrap();
        wc.add_lower_string_value(Some("x"), false);
        exec_wc!(txn, col, wc, oids);
        assert_eq!(&oids, &[] as &[&[u8]]);*/
    }

    #[test]
    fn test_add_upper_oid() {}
}
