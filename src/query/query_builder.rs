//use crate::query::filter::Filter;
use crate::index::IndexType;
use crate::lmdb::db::Db;
use crate::object::property::Property;
use crate::option;
use crate::query::filter::Filter;
use crate::query::query::{Query, Sort};
use crate::query::where_clause::WhereClause;

pub struct QueryBuilder {
    where_clauses: Vec<WhereClause>,
    primary_db: Db,
    secondary_db: Db,
    secondary_dup_db: Db,
    has_secondary_where: bool,
    has_secondary_dup_where: bool,
    filter: Option<Filter>,
    sort: Vec<(Property, Sort)>,
    distinct: Vec<Property>,
    offset_limit: Option<(usize, usize)>,
}

impl QueryBuilder {
    pub(crate) fn new(primary_db: Db, secondary_db: Db, secondary_dup_db: Db) -> QueryBuilder {
        QueryBuilder {
            where_clauses: vec![],
            primary_db,
            secondary_db,
            secondary_dup_db,
            has_secondary_where: false,
            has_secondary_dup_where: false,
            filter: None,
            sort: vec![],
            distinct: vec![],
            offset_limit: None,
        }
    }

    pub fn add_where_clause(&mut self, wc: WhereClause) {
        if wc.index_type == IndexType::Secondary {
            self.has_secondary_where = true;
        } else if wc.index_type == IndexType::SecondaryDup {
            self.has_secondary_dup_where = true;
        }
        self.where_clauses.push(wc)
    }

    pub fn set_filter(&mut self, filter: Filter) {
        self.filter = Some(filter);
    }

    pub fn add_sort(&mut self, property: Property, sort: Sort) {
        self.sort.push((property, sort))
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
        Query::new(
            self.where_clauses,
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
