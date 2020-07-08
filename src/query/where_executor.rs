use crate::bank::IsarBank;
use crate::error::Result;
use crate::index::Index;
use crate::lmdb::txn::Txn;
use crate::query::key_range::{KeyRange, KeyRangeIterator};
use std::collections::HashSet;

struct WhereExecutor<'a> {
    where_clauses: Vec<KeyRange>,
    index: Option<&'a Index>,
    bank: &'a IsarBank,
}

impl<'a> WhereExecutor<'a> {
    pub fn new(bank: &'a IsarBank, index: Option<&'a Index>) -> Self {
        WhereExecutor {
            where_clauses: vec![],
            index,
            bank,
        }
    }



    fn run<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(&'trx [u8], &'trx [u8]) -> bool,
    {
        match self.where_clauses.len() {
            0 => {
                let index = self.hive_box.get_primary_index();
                let range = KeyRange::new(None, None);
                self.execute_where_clause(&range, &mut None, &mut callback)?;
            }
            1 => {
                let where_clause = self.where_clauses.first().unwrap();
                self.execute_where_clause(&where_clause, &mut None, &mut callback)?;
            }
            _ => {
                let mut hash_set = HashSet::new();
                let mut result_ids = if self.check_where_clauses_overlap() {
                    Some(&mut hash_set)
                } else {
                    None
                };
                for where_clause in self.where_clauses {
                    let result =
                        self.execute_where_clause(&where_clause, &mut result_ids, &mut callback)?;
                    if !result {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    fn execute_no_range<F>(&self, txn: &Txn, mut callback: F) -> Result<()>
    where
        F: FnMut(&'trx [u8], &'trx [u8]) -> bool,
    {
        let mut range = KeyRange::new(None, None);
        let iter = self.bank.iter(txn, &mut range)?;
        self.execute_primary_where_clause(iter, &mut None, &mut callback)?;
        Ok(())
    }

    fn execute_single_range<F>(&self, range: &KeyRange, mut callback: F) -> Result<()>
    where
        F: FnMut(&'trx [u8], &'trx [u8]) -> bool,
    {
        if let Some(index) = self.index
        let iter = self.bank.iter(txn, &mut range)?;
        self.execute_primary_where_clause(iter, &mut None, &mut callback)?;
        Ok(())
    }

    fn check_where_clauses_overlap(&self) -> bool {
        for (i1, where_clause1) in self.where_clauses.iter().enumerate() {
            for (i2, where_clause2) in self.where_clauses.iter().enumerate() {
                if i1 == i2 {
                    continue;
                }
                if where_clause1.index.id != where_clause2.index.id || where_clause1.is_unbound() {
                    return false;
                }
                if where_clause1.is_unbound_left() && where_clause2.is_unbound_left()
                    || where_clause1.is_unbound_right() && where_clause2.is_unbound_right()
                {
                    return false;
                }
                //if where_clause1.lower_key<= where_clause2.lower_key
            }
        }
        true
    }

    fn execute_where_clause(
        &self,
        range: &KeyRange,
        result_ids: &mut Option<&mut HashSet<&'trx [u8]>>,
        callback: &mut impl FnMut(&'trx [u8], &'trx [u8]) -> bool,
    ) -> Result<bool> {
        let mut cursor = self.trx.open_ro_cursor(where_clause.index.db)?;
        let iter = where_clause.iter(&mut cursor);
        if where_clause.index.primary {
            self.execute_primary_where_clause(iter, result_ids, callback)
        } else {
            self.execute_secondary_where_clause(iter, result_ids, callback)
        }
    }

    fn execute_primary_where_clause(
        &self,
        txn: &Txn,
        mut range: KeyRange,
        result_ids: &mut Option<&mut HashSet<&'trx [u8]>>,
        callback: &mut impl FnMut(&'trx [u8], &'trx [u8]) -> bool,
    ) -> Result<bool> {
        let iter = self.bank.iter(txn,&mut range);
        for entry in iter.for_each() {
            let (key, val) = entry?;
            if let Some(result_ids) = result_ids {
                if !result_ids.insert(key) {
                    continue;
                }
            }
            if !callback(key, val) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn execute_secondary_where_clause(
        &self,
        txn: &Txn,
        mut range: KeyRange,
        result_ids: &mut Option<&mut HashSet<&'trx [u8]>>,
        callback: &mut impl FnMut(&'trx [u8], &'trx [u8]) -> bool,
    ) -> Result<bool> {
        let iter = self.bank.iter(txn,&mut range);
        for index_entry in iter {
            let (_, entry_id) = index_entry?;
            if let Some(result_ids) = result_ids {
                if !result_ids.insert(entry_id) {
                    continue;
                }
            }

            let entry = primary_cursor.get(Some(entry_id), None, MDB_SET_KEY)?;
            if let (Some(key), val) = entry {
                if !callback(key, val) {
                    return Ok(false);
                }
            } else {
                return Err(Error::Other(111));
            }
        }
        Ok(true)
    }
}
