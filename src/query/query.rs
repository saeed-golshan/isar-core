use crate::data_dbs::IndexType;
use crate::index::Index;
use crate::query::filter::Filter;
use crate::query::where_clause::WhereClause;
use itertools::Itertools;
use std::borrow::Borrow;
use std::cell::Cell;
use std::cmp::max;
use std::ops::Deref;

pub struct Query {
    where_clauses: Vec<WhereClause>,
    filter: Option<Filter>,
}

impl Query {
    pub(crate) fn new() -> Self {
        Query {
            where_clauses: Vec::new(),
            filter: None,
        }
    }

    pub(crate) fn add_where(&mut self, where_clause: WhereClause) {
        self.where_clauses.push(where_clause);
    }

    pub fn set_filter(&mut self, filter: Filter) {
        self.filter = Some(filter);
    }

    /*fn execute<F>(&self, trx: &'trx RoTransaction<'trx>, mut callback: F) -> Result<(), Error>
    where
        F: FnMut(&'trx [u8], &'trx [u8]) -> bool,
    {
        let executor = WhereExecutor {
            where_clauses: &self.where_clauses,
            hive_box: self.hive_box,
            trx,
        };
        if let Some(filter) = &self.filter {
            executor.run(|key, val| {
                if filter.evaluate(val) {
                    callback(key, val)
                } else {
                    true
                }
            })
        } else {
            executor.run(callback)
        }
    }

    pub fn count(&self, trx: &'trx RoTransaction<'trx>) -> Result<u32, Error> {
        let mut counter = 0;
        self.execute(trx, &mut |_, _| {
            counter += 1;
            true
        })?;
        Ok(counter)
    }

    pub fn get_all(&self, trx: &'trx RoTransaction<'trx>) -> Result<Vec<&[u8]>, Error> {
        let mut vec = Vec::new();
        self.execute(trx, |_, val: &'trx [u8]| {
            vec.push(val);
            true
        })?;

        Ok(vec)
    }*/
}
