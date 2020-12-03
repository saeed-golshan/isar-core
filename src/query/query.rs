use crate::error::Result;
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::map_option;
use crate::object::object_id::ObjectId;
use crate::object::property::Property;
use crate::query::filter::*;
use crate::query::where_clause::WhereClause;
use crate::query::where_executor::WhereExecutor;
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
    distinct: Vec<Property>,
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
        distinct: Vec<Property>,
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

    fn execute_raw<'txn, F>(&self, txn: &'txn Txn, mut callback: F) -> Result<()>
    where
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    {
        let primary_cursor = self.primary_db.cursor(&txn)?;
        let secondary_cursor = map_option!(self.secondary_db, db, db.cursor(&txn)?);
        let secondary_dup_cursor = map_option!(self.secondary_dup_db, db, db.cursor(&txn)?);
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

    fn execute_unsorted<'txn, F>(&self, txn: &'txn Txn, callback: F) -> Result<()>
    where
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    {
        if !self.distinct.is_empty() {
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

    fn execute_sorted<'txn, F>(&self, _txn: &'txn Txn, _callback: F) -> Result<()>
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
        let properties = self.distinct.clone();
        let mut hashes = HashSet::new();
        move |key, val| {
            let mut hasher = WyHash::default();
            for property in &properties {
                let static_bytes = property.get_static_raw(val);
                hasher.write(static_bytes);
                if property.data_type.is_dynamic() {
                    if let Some(dynamic_bytes) = property.get_dynamic_raw(val) {
                        hasher.write(dynamic_bytes);
                    }
                }
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

    pub fn find_all<'txn, F>(&self, txn: &'txn Txn, callback: F) -> Result<()>
    where
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    {
        if self.sort.is_empty() {
            self.execute_unsorted(txn, callback)
        } else {
            self.execute_sorted(txn, callback)
        }
    }

    pub fn find_all_vec<'txn>(&self, txn: &'txn Txn) -> Result<Vec<(&'txn ObjectId, &'txn [u8])>> {
        let mut results = vec![];
        self.find_all(txn, |key, value| {
            results.push((key, value));
            true
        })?;
        Ok(results)
    }

    pub fn count(&self, txn: &Txn) -> Result<u32> {
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
    use crate::{col, ind, isar};

    fn get_col(data: Vec<(i32, bool, String)>) -> Result<(IsarInstance, Vec<ObjectId>)> {
        isar!(isar, col => col!(field1 => Bool,field2 => Int,field3=>String; ind!(field1, field2, field3), ind!(field2, field3), ind!(field3)));
        let txn = isar.begin_txn(true)?;
        let mut ids = vec![];
        for (f1, f2, f3) in data {
            let mut o = col.get_object_builder();
            o.write_bool(Some(f2));
            o.write_int(f1);
            o.write_string(Some(&f3));
            let bytes = o.to_bytes();
            println!("{:?}", bytes);
            ids.push(col.put(&txn, None, &bytes)?);
        }
        Ok((isar, ids))
    }

    #[test]
    fn test_primary_where_clause() -> Result<()> {
        /*let (isar, ids) = get_col(vec![(25, true, "ab".to_string())])?;
        let col = isar.get_collection(0).unwrap();

        let mut qb = isar.create_query_builder();
        let mut wc = col.create_where_clause(Some(0)).unwrap();
        wc.add_lower_int(2, true);
        wc.add_upper_int(4, false);
        qb.add_where_clause(wc);
        let q = qb.build();

        let txn = isar.begin_txn(false)?;
        let results = q.find_all_vec(&txn).unwrap();
        //assert_eq!(results[0].0, &ids[1]);
        //assert_eq!(results[1].0, &ids[2]);*/

        Ok(())
    }
}
