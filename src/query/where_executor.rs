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
    use crate::object::object_builder::ObjectBuilder;
    use crate::object::object_info::ObjectInfo;
    use crate::object::property::{DataType, Property};
    use crate::query::where_executor::WhereExecutor;
    use crate::*;
    use hashbrown::HashMap;
    use crate::utils::debug::fill_db;

    fn run_executer<'a>(executer: &'a mut WhereExecutor) -> HashMap<&'a [u8], &'a [u8]> {
        let mut entries = HashMap::new();
        executer
            .run(|key, val| {
                if entries.insert(key, val).is_some() {
                    panic!("Duplicate entry");
                }
                true
            })
            .unwrap();
        entries
    }

    fn build_value(field1: i32, field2: &str) -> Vec<u8> {
        let properties = vec![
            Property::new("f1", DataType::Int, 0),
            Property::new("f2", DataType::String, 4),
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

        let data = vec![(None, build_value(1, "aaa")),(None, build_value(1, "aaa")),(None, build_value(1, "aaa")),(None, build_value(1, "aaa"))];
        fill_db(col,&txn,data)
        let oid1 = col.put(&txn, None, &object1).unwrap();

        let object2 = build_value(1, "aaa");
        let oid2 = col.put(&txn, None, &object2).unwrap();

        let object3 =
        let oid3 = col.put(&txn, None, &build_value(1, "abb")).unwrap();
        let oid4 = col.put(&txn, None, &build_value(1, "abb")).unwrap();

        let primary_wc = col.create_where_clause(None).unwrap();
        let primary_cursor = col.debug_get_db().cursor(&txn).unwrap();
        let mut executer = WhereExecutor::new(primary_cursor, None, None, &vec![primary_wc], false);
        assert_eq!(run_executer(&mut executer), map!())
    }
}
