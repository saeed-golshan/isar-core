use crate::error::Result;
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::map_option;
use crate::query::where_clause::WhereClause;
use crate::query::where_executor::WhereExecutor;

pub struct Query {
    where_clauses: Vec<WhereClause>,
    where_clauses_overlapping: bool,
    primary_db: Db,
    secondary_db: Option<Db>,
    secondary_dup_db: Option<Db>,
    //filter: Option<Filter>,
}

impl Query {
    pub(crate) fn new(
        where_clauses: Vec<WhereClause>,
        primary_db: Db,
        secondary_db: Option<Db>,
        secondary_dup_db: Option<Db>,
    ) -> Self {
        Query {
            where_clauses,
            where_clauses_overlapping: true,
            primary_db,
            secondary_db,
            secondary_dup_db,
            //filter: None,
        }
    }

    fn execute<'txn, F>(&self, txn: &'txn Txn, mut callback: F) -> Result<()>
    where
        F: FnMut(&'txn [u8], &'txn [u8]) -> bool,
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
        /*if let Some(filter) = &self.filter {
            executor.run(|key, val| {
                if filter.evaluate(val) {
                    callback(key, val)
                } else {
                    true
                }
            })
        } else {*/
        executor.run(callback)
        // }
    }

    pub fn count(&self, txn: &Txn) -> Result<u32> {
        let mut counter = 0;
        self.execute(txn, &mut |_, _| {
            counter += 1;
            true
        })?;
        Ok(counter)
    }

    pub fn get_all<'txn>(&self, txn: &'txn Txn) -> Result<Vec<&'txn [u8]>> {
        let mut vec = Vec::new();
        self.execute(txn, |key, val| {
            vec.push(val);
            true
        })?;

        Ok(vec)
    }
}
