use crate::error::{illegal_state, Result};
use crate::index::IndexType;
use crate::lmdb::cursor::Cursor;
use crate::option;
use crate::query::where_clause::WhereClause;
use hashbrown::HashSet;

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
            let oid = &key[2..14];
            if let Some(result_ids) = result_ids {
                if !result_ids.insert(oid) {
                    continue;
                }
            }
            if !callback(oid, val) {
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
            let (_, oid) = index_entry?;
            if let Some(result_ids) = result_ids {
                if !result_ids.insert(oid) {
                    continue;
                }
            }

            let entry = self.primary_cursor.move_to(oid)?;
            if let Some((_, val)) = entry {
                if !callback(oid, val) {
                    return Ok(false);
                }
            } else {
                illegal_state("UNKNOWN!")?;
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::object::data_type::DataType;
    use crate::object::object_builder::ObjectBuilder;
    use crate::object::object_id::ObjectId;
    use crate::object::object_info::ObjectInfo;
    use crate::object::property::Property;
    use crate::query::where_executor::WhereExecutor;
    use crate::utils::debug::fill_db;
    use crate::*;
    use hashbrown::HashMap;

    fn run_executer(executer: &mut WhereExecutor) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut entries = HashMap::new();
        executer
            .run(|key, val| {
                if entries.insert(key.to_vec(), val.to_vec()).is_some() {
                    panic!("Duplicate entry");
                }
                true
            })
            .unwrap();
        entries
    }

    fn build_value(field1: i32, field2: &str) -> Vec<u8> {
        let properties = vec![
            Property::new(DataType::Int, 0),
            Property::new(DataType::String, 4),
        ];
        let info = ObjectInfo::new(properties);
        let mut builder = ObjectBuilder::new(&info);
        builder.write_int(field1);
        builder.write_string(Some(field2));
        builder.to_bytes().to_vec()
    }

    #[test]
    fn test_run_single_where_clause() {
        isar!(isar, col => col!(f1 => Int, f2 => String));

        let txn = isar.begin_txn(true).unwrap();

        let data = vec![
            (Some(ObjectId::new(1, 5)), build_value(1, "aaa")),
            (Some(ObjectId::new(2, 5)), build_value(1, "aaa")),
            (Some(ObjectId::new(3, 5)), build_value(1, "abb")),
            (Some(ObjectId::new(4, 5)), build_value(1, "abb")),
        ];
        let data = fill_db(col, &txn, &data);

        let primary_wc = vec![col.create_where_clause(None).unwrap()];
        let primary_cursor = col.debug_get_db().cursor(&txn).unwrap();
        let mut executer = WhereExecutor::new(primary_cursor, None, None, &primary_wc, false);
        assert_eq!(run_executer(&mut executer), data);

        let mut primary_wc = vec![col.create_where_clause(None).unwrap()];
        primary_wc[0].add_lower_oid(Some(2), None, true);
        let primary_cursor = col.debug_get_db().cursor(&txn).unwrap();
        let mut executer = WhereExecutor::new(primary_cursor, None, None, &primary_wc, false);
        assert_eq!(run_executer(&mut executer), data);
    }
}
