use crate::error::Result;
use crate::lmdb::db::Db;
use crate::map_option;
use crate::object::object_id::ObjectId;
use crate::object::property::Property;
use crate::query::filter::*;
use crate::query::where_clause::WhereClause;
use crate::query::where_executor::WhereExecutor;
use crate::txn::IsarTxn;
use hashbrown::HashSet;
use std::hash::Hasher;
use wyhash::WyHash;

pub enum Sort {
    Ascending,
    Descending,
}

pub enum Case {
    Sensitive,
    Insensitive,
}

pub struct Query {
    where_clauses: Vec<WhereClause>,
    where_clauses_overlapping: bool,
    primary_db: Db,
    secondary_db: Option<Db>,
    secondary_dup_db: Option<Db>,
    filter: Option<Filter>,
    sort: Vec<(Property, Sort)>,
    distinct: Option<Vec<Property>>,
    offset_limit: Option<(usize, usize)>,
}

impl Query {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        where_clauses: Vec<WhereClause>,
        primary_db: Db,
        secondary_db: Option<Db>,
        secondary_dup_db: Option<Db>,
        filter: Option<Filter>,
        sort: Vec<(Property, Sort)>,
        distinct: Option<Vec<Property>>,
        offset_limit: Option<(usize, usize)>,
    ) -> Self {
        Query {
            where_clauses,
            where_clauses_overlapping: true,
            primary_db,
            secondary_db,
            secondary_dup_db,
            filter,
            sort,
            distinct,
            offset_limit,
        }
    }

    fn execute_raw<'txn, F>(&self, txn: &'txn IsarTxn, mut callback: F) -> Result<()>
    where
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    {
        let lmdb_txn = txn.get_txn()?;
        let primary_cursor = self.primary_db.cursor(lmdb_txn)?;
        let secondary_cursor = map_option!(self.secondary_db, db, db.cursor(lmdb_txn)?);
        let secondary_dup_cursor = map_option!(self.secondary_dup_db, db, db.cursor(lmdb_txn)?);
        let mut executor = WhereExecutor::new(
            primary_cursor,
            secondary_cursor,
            secondary_dup_cursor,
            &self.where_clauses,
            self.where_clauses_overlapping,
        );
        if let Some(filter) = &self.filter {
            executor.run(|oid, val| {
                if filter.evaluate(val) {
                    callback(oid, val)
                } else {
                    true
                }
            })
        } else {
            executor.run(callback)
        }
    }

    fn execute_unsorted<'txn, F>(&self, txn: &'txn IsarTxn, callback: F) -> Result<()>
    where
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    {
        if self.distinct.is_some() {
            let callback = self.add_distinct(callback);
            if self.offset_limit.is_some() {
                let callback = self.add_offset_limit(callback);
                self.execute_raw(txn, callback)
            } else {
                self.execute_raw(txn, callback)
            }
        } else if self.offset_limit.is_some() {
            let callback = self.add_offset_limit(callback);
            self.execute_raw(txn, callback)
        } else {
            self.execute_raw(txn, callback)
        }
    }

    fn execute_sorted<'txn, F>(&self, _txn: &'txn IsarTxn, _callback: F) -> Result<()>
    where
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    {
        /*let mut result = vec![];
        self.execute_raw(txn, |key,val| {
            result.push((key,val));
            true
        });
        result.sort_by()
        let callback = self.add_distinct(callback);
        let callback = self.add_offset_limit(callback);*/
        Ok(())
    }

    fn add_distinct<'txn, F>(
        &self,
        mut callback: F,
    ) -> impl FnMut(&'txn ObjectId, &'txn [u8]) -> bool
    where
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    {
        let properties = self.distinct.as_ref().unwrap().clone();
        let mut hashes = HashSet::new();
        move |key, val| {
            let mut hasher = WyHash::default();
            for property in &properties {
                property.hash_value(val, &mut hasher);
            }
            let hash = hasher.finish();
            if hashes.insert(hash) {
                callback(key, val)
            } else {
                true
            }
        }
    }

    fn add_offset_limit<'txn, F>(
        &self,
        mut callback: F,
    ) -> impl FnMut(&'txn ObjectId, &'txn [u8]) -> bool
    where
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    {
        let (offset, limit) = self.offset_limit.unwrap();
        let mut count = 0;
        move |key, value| {
            let result = if count >= offset {
                callback(key, value)
            } else {
                true
            };
            count += 1;
            result && limit.saturating_add(offset) > count
        }
    }

    pub fn find_all<'txn, F>(&self, txn: &'txn IsarTxn, callback: F) -> Result<()>
    where
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    {
        if self.sort.is_empty() {
            self.execute_unsorted(txn, callback)
        } else {
            self.execute_sorted(txn, callback)
        }
    }

    pub fn find_all_vec<'txn>(
        &self,
        txn: &'txn IsarTxn,
    ) -> Result<Vec<(&'txn ObjectId, &'txn [u8])>> {
        let mut results = vec![];
        self.find_all(txn, |key, value| {
            results.push((key, value));
            true
        })?;
        Ok(results)
    }

    pub fn count(&self, txn: &IsarTxn) -> Result<u32> {
        let mut counter = 0;
        self.find_all(txn, &mut |_, _| {
            counter += 1;
            true
        })?;
        Ok(counter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::IsarInstance;
    use crate::object::object_id::ObjectId;
    use crate::{col, ind, isar, set};

    fn get_col(data: Vec<(i32, String)>) -> (IsarInstance, Vec<ObjectId>) {
        isar!(isar, col => col!(field1 => Int, field2 => String; ind!(field1, field2; true), ind!(field2)));
        let mut txn = isar.begin_txn(true).unwrap();
        let mut ids = vec![];
        for (f1, f2) in data {
            let mut o = col.get_object_builder();
            o.write_int(f1);
            o.write_string(Some(&f2));
            let bytes = o.finish();
            ids.push(col.put(&mut txn, None, bytes.as_bytes()).unwrap());
        }
        txn.commit().unwrap();
        (isar, ids)
    }

    fn keys(result: Vec<(&ObjectId, &[u8])>) -> Vec<ObjectId> {
        result.iter().map(|(k, _)| **k).collect()
    }

    #[test]
    fn test_no_where_clauses() {
        let (isar, ids) = get_col(vec![(1, "a".to_string()), (2, "b".to_string())]);
        let col = isar.get_collection(0).unwrap();
        let txn = isar.begin_txn(false).unwrap();

        let q = isar.create_query_builder(col).build();
        let results = q.find_all_vec(&txn).unwrap();

        assert_eq!(keys(results), vec![ids[0], ids[1]]);
    }

    #[test]
    fn test_single_primary_where_clause() {}

    #[test]
    fn test_single_secondary_where_clause() {
        let (isar, ids) = get_col(vec![
            (1, "a".to_string()),
            (1, "b".to_string()),
            (1, "c".to_string()),
            (2, "d".to_string()),
            (2, "a".to_string()),
            (3, "b".to_string()),
        ]);
        let col = isar.get_collection(0).unwrap();
        let txn = isar.begin_txn(false).unwrap();

        let mut wc = col.create_secondary_where_clause(0).unwrap();
        wc.add_int(1, 1);

        let mut qb = isar.create_query_builder(col);
        qb.add_where_clause(wc.clone(), true, true);
        let q = qb.build();

        let results = q.find_all_vec(&txn).unwrap();
        assert_eq!(keys(results), vec![ids[0], ids[1], ids[2]]);

        wc.add_string_value(Some("b"), Some("x"));
        let mut qb = isar.create_query_builder(col);
        qb.add_where_clause(wc, true, true);
        let q = qb.build();

        let results = q.find_all_vec(&txn).unwrap();
        assert_eq!(keys(results), vec![ids[1], ids[2]]);
    }

    #[test]
    fn test_single_secondary_where_clause_dup() {
        let (isar, ids) = get_col(vec![
            (1, "aa".to_string()),
            (2, "ab".to_string()),
            (4, "bb".to_string()),
            (3, "ab".to_string()),
        ]);
        let col = isar.get_collection(0).unwrap();
        let txn = isar.begin_txn(false).unwrap();

        let mut wc = col.create_secondary_where_clause(1).unwrap();
        wc.add_string_value(Some("ab"), Some("xx"));

        let mut qb = isar.create_query_builder(col);
        qb.add_where_clause(wc, true, true);
        let q = qb.build();

        let results = q.find_all_vec(&txn).unwrap();
        assert_eq!(keys(results), vec![ids[1], ids[3], ids[2]]);

        let mut wc = col.create_secondary_where_clause(1).unwrap();
        wc.add_string_value(Some("ab"), Some("ab"));
        let mut qb = isar.create_query_builder(col);
        qb.add_where_clause(wc, true, true);
        let q = qb.build();

        let results = q.find_all_vec(&txn).unwrap();
        assert_eq!(keys(results), vec![ids[1], ids[3]]);
    }

    #[test]
    fn test_multiple_where_clauses() {
        let (isar, ids) = get_col(vec![
            (1, "aa".to_string()),
            (1, "ab".to_string()),
            (0, "ab".to_string()),
            (1, "bb".to_string()),
            (0, "bb".to_string()),
            (1, "bc".to_string()),
        ]);
        let col = isar.get_collection(0).unwrap();
        let txn = isar.begin_txn(false).unwrap();

        let mut primary_wc = col.create_primary_where_clause();
        primary_wc.add_oid(ids[5]);

        let mut secondary_wc = col.create_secondary_where_clause(0).unwrap();
        secondary_wc.add_int(0, 0);

        let mut secondary_dup_wc = col.create_secondary_where_clause(1).unwrap();
        secondary_dup_wc.add_string_value(None, Some("aa"));

        let mut qb = isar.create_query_builder(col);
        qb.add_where_clause(primary_wc, true, true);
        qb.add_where_clause(secondary_wc, true, true);
        qb.add_where_clause(secondary_dup_wc, true, true);
        let q = qb.build();

        let results = q.find_all_vec(&txn).unwrap();
        let set: HashSet<ObjectId> = keys(results).into_iter().collect();
        assert_eq!(set, set!(ids[0], ids[2], ids[4], ids[5]));
    }
}
