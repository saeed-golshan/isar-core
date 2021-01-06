use crate::error::Result;
use crate::index::{Index, IndexType};
use crate::lmdb::cursor::{Cursor, CursorIterator};
use crate::lmdb::KeyVal;
use crate::object::object_id::ObjectId;

#[derive(Clone)]
pub struct WhereClause {
    lower_key: Vec<u8>,
    upper_key: Vec<u8>,
    prefix_len: usize,
    pub(super) index_type: IndexType,
}

impl WhereClause {
    pub(crate) fn new(prefix: &[u8], index_type: IndexType) -> Self {
        WhereClause {
            lower_key: prefix.to_vec(),
            upper_key: prefix.to_vec(),
            prefix_len: prefix.len(),
            index_type,
        }
    }

    pub(crate) fn empty() -> Self {
        WhereClause {
            lower_key: vec![0],
            upper_key: vec![10],
            prefix_len: 0,
            index_type: IndexType::Primary,
        }
    }

    pub(crate) fn iter<'a, 'txn>(
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

    pub(crate) fn try_exclude(&mut self, include_lower: bool, include_upper: bool) -> bool {
        if !include_lower {
            let mut increased = false;
            for i in (self.prefix_len..self.lower_key.len()).rev() {
                if let Some(added) = self.lower_key[i].checked_add(1) {
                    self.lower_key[i] = added;
                    increased = true;
                    break;
                }
            }
            if !increased {
                return false;
            }
        }
        if !include_upper {
            let mut decreased = false;
            for i in (self.prefix_len..self.upper_key.len()).rev() {
                if let Some(subtracted) = self.upper_key[i].checked_sub(1) {
                    eprintln!("before {}, after {}", self.upper_key[i], subtracted);
                    self.upper_key[i] = subtracted;
                    decreased = true;
                    break;
                }
            }
            if !decreased {
                return false;
            }
        }
        true
    }

    /*pub(super) fn merge(&self, other: &WhereClause) -> Option<WhereClause> {
        unimplemented!()
    }*/

    pub fn add_oid(&mut self, oid: ObjectId) {
        let bytes = oid.as_bytes_without_prefix();
        self.lower_key.extend_from_slice(bytes);
        self.upper_key.extend_from_slice(bytes);
    }

    pub fn add_oid_time(&mut self, lower: u32, upper: u32) {
        self.lower_key.extend_from_slice(&lower.to_be_bytes());
        self.upper_key.extend_from_slice(&upper.to_be_bytes());
    }

    pub fn add_byte(&mut self, lower: u8, upper: u8) {
        self.lower_key
            .extend_from_slice(&Index::get_byte_key(lower));
        self.upper_key
            .extend_from_slice(&Index::get_byte_key(upper));
    }

    pub fn add_int(&mut self, lower: i32, upper: i32) {
        self.lower_key.extend_from_slice(&Index::get_int_key(lower));
        self.upper_key.extend_from_slice(&Index::get_int_key(upper));
    }

    pub fn add_float(&mut self, lower: f32, upper: f32) {
        self.lower_key
            .extend_from_slice(&Index::get_float_key(lower));
        self.upper_key
            .extend_from_slice(&Index::get_float_key(upper));
    }

    pub fn add_long(&mut self, lower: i64, upper: i64) {
        self.lower_key
            .extend_from_slice(&Index::get_long_key(lower));
        self.upper_key
            .extend_from_slice(&Index::get_long_key(upper));
    }

    pub fn add_double(&mut self, lower: f64, upper: f64) {
        self.lower_key
            .extend_from_slice(&Index::get_double_key(lower));
        self.upper_key
            .extend_from_slice(&Index::get_double_key(upper));
    }

    pub fn add_string_hash(&mut self, value: Option<&str>) {
        let hash = Index::get_string_hash_key(value);
        self.lower_key.extend_from_slice(&hash);
        self.upper_key.extend_from_slice(&hash);
    }

    pub fn add_string_value(&mut self, lower: Option<&str>, upper: Option<&str>) {
        self.lower_key
            .extend_from_slice(&Index::get_string_value_key(lower));
        self.upper_key
            .extend_from_slice(&Index::get_string_value_key(upper));
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
