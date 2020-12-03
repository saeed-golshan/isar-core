use crate::error::{illegal_state, Result};
use crate::index::IndexType;
use crate::lmdb::cursor::Cursor;
use crate::object::object_id::ObjectId;
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
        F: FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
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
        callback: &mut impl FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
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
        callback: &mut impl FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
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
            if !callback(ObjectId::from_bytes_with_prefix_padding(key), val) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn execute_secondary_where_clause(
        &mut self,
        where_clause: &WhereClause,
        result_ids: &mut Option<&mut HashSet<&'txn [u8]>>,
        callback: &mut impl FnMut(&'txn ObjectId, &'txn [u8]) -> bool,
    ) -> Result<bool> {
        let cursor = if where_clause.index_type == IndexType::Secondary {
            self.secondary_cursor.as_mut().unwrap()
        } else {
            self.secondary_dup_cursor.as_mut().unwrap()
        };
        let iter = where_clause.iter(cursor)?;
        for index_entry in iter {
            let (_, key) = index_entry?;
            if let Some(result_ids) = result_ids {
                if !result_ids.insert(key) {
                    continue;
                }
            }

            let entry = self.primary_cursor.move_to(key)?;
            if let Some((_, val)) = entry {
                if !callback(ObjectId::from_bytes_with_prefix_padding(key), val) {
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
    use super::*;
    use crate::instance::IsarInstance;
    use crate::object::data_type::DataType;
    use crate::object::object_builder::ObjectBuilder;
    use crate::object::object_id::ObjectId;
    use crate::object::object_info::ObjectInfo;
    use crate::object::property::Property;
    use crate::utils::debug::fill_db;
    use crate::*;

    fn execute_where_clauses(
        isar: &IsarInstance,
        wc: &[WhereClause],
        overlapping: bool,
    ) -> Vec<(u32, u64)> {
        let txn = isar.begin_txn(false).unwrap();
        let primary_cursor = isar.debug_get_primary_db().cursor(&txn).unwrap();
        let secondary_cursor = isar.debug_get_primary_db().cursor(&txn).unwrap();
        let secondary_dup_cursor = isar.debug_get_primary_db().cursor(&txn).unwrap();
        let mut executer = WhereExecutor::new(
            primary_cursor,
            Some(secondary_cursor),
            Some(secondary_dup_cursor),
            &wc,
            overlapping,
        );
        let mut entries = vec![];
        executer
            .run(|oid, _| {
                entries.push((oid.get_time(), oid.get_rand_counter()));
                true
            })
            .unwrap();
        entries
    }

    fn build_value(field1: i32, field2: &str) -> Vec<u8> {
        let properties = vec![
            Property::new(DataType::Int, 0),
            Property::new(DataType::String, 8),
        ];
        let info = ObjectInfo::new(properties);
        let mut builder = ObjectBuilder::new(&info);
        builder.write_int(field1);
        builder.write_string(Some(field2));
        builder.to_bytes().to_vec()
    }

    fn get_test_db() -> IsarInstance {
        isar!(isar, col => col!(f1 => Int, f2 => String; ind!(f1, f2), ind!(f2)));
        let txn = isar.begin_txn(true).unwrap();

        let data = vec![
            (Some(ObjectId::new(1, 5)), build_value(1, "aaa")),
            (Some(ObjectId::new(2, 5)), build_value(1, "aaa")),
            (Some(ObjectId::new(3, 5)), build_value(2, "abb")),
            (Some(ObjectId::new(4, 5)), build_value(2, "abb")),
            (Some(ObjectId::new(5, 5)), build_value(3, "bbb")),
            (Some(ObjectId::new(6, 5)), build_value(3, "bbb")),
        ];
        fill_db(col, &txn, &data);
        txn.commit().unwrap();

        isar
    }

    #[test]
    fn test_run_single_primary_where_clause() {
        let isar = get_test_db();
        let col = isar.get_collection(0).unwrap();

        let mut wc = col.create_where_clause(None).unwrap();
        wc.add_lower_oid(Some(4), None);
        assert_eq!(
            execute_where_clauses(&isar, &[wc.clone()], false),
            vec![(4, 5), (5, 5), (6, 5)]
        );

        wc.add_upper_oid(Some(5), None);
        assert_eq!(
            execute_where_clauses(&isar, &[wc], false),
            vec![(4, 5), (5, 5)]
        );
    }

    #[test]
    fn test_run_single_secondary_where_clause() {
        let isar = get_test_db();
        let col = isar.get_collection(0).unwrap();

        /*let txn = isar.begin_txn(false).unwrap();
        let data = col.debug_get_index(0).debug_dump(&txn);
        println!("{:?}", data);*/

        let mut wc = col.create_where_clause(Some(0)).unwrap();
        wc.add_lower_int(2, true);
        assert_eq!(
            execute_where_clauses(&isar, &[wc.clone()], false),
            vec![(3, 5), (4, 5), (5, 5), (6, 5)]
        );

        wc.add_upper_int(3, false);
        assert_eq!(
            execute_where_clauses(&isar, &[wc], false),
            vec![(3, 5), (4, 5)]
        );
    }

    #[test]
    fn test_run_single_secondary_compound_where_clause() {
        let isar = get_test_db();
        let col = isar.get_collection(0).unwrap();

        let mut wc = col.create_where_clause(Some(0)).unwrap();
        wc.add_lower_int(2, true);
        assert_eq!(
            execute_where_clauses(&isar, &[wc.clone()], false),
            vec![(3, 5), (4, 5), (5, 5), (6, 5)]
        );

        wc.add_upper_int(3, false);
        assert_eq!(
            execute_where_clauses(&isar, &[wc], false),
            vec![(3, 5), (4, 5)]
        );
    }
}
