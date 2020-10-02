use crate::data_dbs::IndexType;
use crate::error::{illegal_state, Result};
use crate::lmdb::cursor::Cursor;
use crate::query::where_clause::WhereClause;
use std::collections::HashSet;

struct WhereExecutor<'a, 'txn> {
    where_clauses: &'a [WhereClause],
    primary_cursor: Cursor<'txn>,
    secondary_cursor: Option<Cursor<'txn>>,
    secondary_cursor_dup: Option<Cursor<'txn>>,
}

impl<'a, 'txn> WhereExecutor<'a, 'txn> {
    pub fn new(
        primary_cursor: Cursor<'txn>,
        secondary_cursor: Option<Cursor<'txn>>,
        secondary_cursor_dup: Option<Cursor<'txn>>,
        where_clauses: &'a [WhereClause],
    ) -> Self {
        WhereExecutor {
            where_clauses,
            primary_cursor,
            secondary_cursor,
            secondary_cursor_dup,
        }
    }

    fn run<F>(&mut self, mut callback: F) -> Result<()>
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
            self.secondary_cursor_dup.as_mut().unwrap()
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
