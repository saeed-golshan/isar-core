use crate::error::{illegal_state, Result};
use crate::index::IndexType;
use crate::lmdb::cursor::Cursor;
use crate::option;
use crate::query::where_clause::WhereClause;
use std::collections::HashSet;

pub(super) struct WhereExecutor<'a, 'txn> {
    where_clauses: &'a [WhereClause],
    where_clauses_overlapping: bool,
    primary_cursor: Cursor<'txn>,
    secondary_cursor: Option<Cursor<'txn>>,
    secondary_dup_cursor: Option<Cursor<'txn>>,
}

impl<'a, 'txn> WhereExecutor<'a, 'txn> {
    pub fn new(
        primary_cursor: Cursor<'txn>,
        secondary_cursor: Option<Cursor<'txn>>,
        secondary_dup_cursor: Option<Cursor<'txn>>,
        where_clauses: &'a [WhereClause],
        where_clauses_overlapping: bool,
    ) -> Self {
        WhereExecutor {
            where_clauses,
            where_clauses_overlapping,
            primary_cursor,
            secondary_cursor,
            secondary_dup_cursor,
        }
    }

    pub fn run<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(&'txn [u8], &'txn [u8]) -> bool,
    {
        match self.where_clauses.len() {
            1 => {
                let where_clause = self.where_clauses.first().unwrap();
                self.execute_where_clause(&where_clause, &mut None, &mut callback)?;
            }
            _ => {
                let mut hash_set = HashSet::new();
                let mut result_ids = option!(self.where_clauses_overlapping, &mut hash_set);
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

    fn execute_where_clause(
        &mut self,
        where_clause: &WhereClause,
        result_ids: &mut Option<&mut HashSet<&'txn [u8]>>,
        callback: &mut impl FnMut(&'txn [u8], &'txn [u8]) -> bool,
    ) -> Result<bool> {
        if where_clause.index_type == IndexType::Primary {
            self.execute_primary_where_clause(where_clause, result_ids, callback)
        } else {
            self.execute_secondary_where_clause(where_clause, result_ids, callback)
        }
    }

    fn execute_primary_where_clause(
        &mut self,
        where_clause: &WhereClause,
        result_ids: &mut Option<&mut HashSet<&'txn [u8]>>,
        callback: &mut impl FnMut(&'txn [u8], &'txn [u8]) -> bool,
    ) -> Result<bool> {
        let cursor = &mut self.primary_cursor;
        let iter = where_clause.iter(cursor)?;
        for entry in iter {
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
        &mut self,
        where_clause: &WhereClause,
        result_ids: &mut Option<&mut HashSet<&'txn [u8]>>,
        callback: &mut impl FnMut(&'txn [u8], &'txn [u8]) -> bool,
    ) -> Result<bool> {
        let cursor = if where_clause.index_type == IndexType::Secondary {
            self.secondary_cursor.as_mut().unwrap()
        } else {
            self.secondary_dup_cursor.as_mut().unwrap()
        };
        let iter = where_clause.iter(cursor)?;
        for index_entry in iter {
            let (_, entry_id) = index_entry?;
            if let Some(result_ids) = result_ids {
                if !result_ids.insert(entry_id) {
                    continue;
                }
            }

            let entry = self.primary_cursor.move_to(entry_id)?;
            if let Some((key, val)) = entry {
                if !callback(key, val) {
                    return Ok(false);
                }
            } else {
                illegal_state("UNKNOWN!")?;
            }
        }
        Ok(true)
    }
}
