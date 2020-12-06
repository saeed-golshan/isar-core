use crate::collection::IsarCollection;
use crate::error::{illegal_arg, Result};
use crate::index::IndexType;
use crate::lmdb::db::Db;
use crate::object::property::Property;
use crate::option;
use crate::query::filter::Filter;
use crate::query::query::{Query, Sort};
use crate::query::where_clause::WhereClause;
use itertools::Itertools;

pub struct QueryBuilder {
    primary_where_clause: WhereClause,
    where_clauses: Vec<WhereClause>,
    primary_db: Db,
    secondary_db: Db,
    secondary_dup_db: Db,
    has_secondary_where: bool,
    has_secondary_dup_where: bool,
    filter: Option<Filter>,
    sort: Vec<(Property, Sort)>,
    distinct: Option<Vec<Property>>,
    offset_limit: Option<(usize, usize)>,
}

impl QueryBuilder {
    pub(crate) fn new(
        collection: &IsarCollection,
        primary_db: Db,
        secondary_db: Db,
        secondary_dup_db: Db,
    ) -> QueryBuilder {
        QueryBuilder {
            primary_where_clause: collection.create_primary_where_clause(),
            where_clauses: vec![],
            primary_db,
            secondary_db,
            secondary_dup_db,
            has_secondary_where: false,
            has_secondary_dup_where: false,
            filter: None,
            sort: vec![],
            distinct: None,
            offset_limit: None,
        }
    }

    pub fn add_where_clause(&mut self, wc: WhereClause) -> Result<()> {
        if wc.index_type == IndexType::Secondary {
            self.has_secondary_where = true;
        } else if wc.index_type == IndexType::SecondaryDup {
            self.has_secondary_dup_where = true;
        }
        self.where_clauses.push(wc);
        Ok(())
    }

    pub fn set_filter(&mut self, filter: Filter) {
        self.filter = Some(filter);
    }

    pub fn add_sort(&mut self, property: Property, sort: Sort) {
        self.sort.push((property, sort))
    }

    pub fn add_offset_limit(&mut self, offset: Option<usize>, limit: Option<usize>) -> Result<()> {
        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(usize::MAX);

        if offset > limit {
            illegal_arg("Offset has to less or equal than limit.")
        } else {
            self.offset_limit = Some((offset, limit));
            Ok(())
        }
    }

    pub fn set_distinct(&mut self, properties: &[Property]) {
        self.distinct = Some(properties.iter().copied().collect_vec());
    }

    /*pub fn merge_where_clauses(mut where_clauses: Vec<WhereClause>) -> Vec<WhereClause> {
        where_clauses.sort_unstable_by(|a, b| a.lower_key.cmp(&b.lower_key));

        let mut merged = vec![];
        let mut i = 0;
        while i < where_clauses.len() {
            let a = where_clauses.get(i).unwrap();
            let mut new_upper_key = None;
            loop {
                if let Some(b) = where_clauses.get(i + 1) {
                    if b.lower_key <= a.upper_key {
                        new_upper_key = Some(max(&a.upper_key, &b.upper_key));
                        i += 1;
                        continue;
                    }
                }
                break;
            }
            if let Some(new_upper_key) = new_upper_key {
                merged.push(WhereClause {
                    lower_key: a.lower_key.clone(),
                    upper_key: new_upper_key.clone(),
                    index_type: a.index_type,
                });
                i += 2;
            } else {
                merged.push(a.deref().clone());
                i += 1;
            }
        }

        merged
    }*/

    pub fn build(self) -> Query {
        let secondary_db = option!(self.has_secondary_where, self.secondary_db);
        let secondary_dup_db = option!(self.has_secondary_dup_where, self.secondary_dup_db);
        let where_clauses = if self.where_clauses.is_empty() {
            vec![self.primary_where_clause]
        } else {
            self.where_clauses
        };
        Query::new(
            where_clauses,
            self.primary_db,
            secondary_db,
            secondary_dup_db,
            self.filter,
            self.sort,
            self.distinct,
            self.offset_limit,
        )
    }
}
