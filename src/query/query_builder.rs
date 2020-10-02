use crate::query::filter::Filter;
use crate::query::where_clause::WhereClause;
use std::cmp::max;
use std::ops::Deref;

pub struct QueryBuilder {
    where_clauses: Vec<WhereClause>,
    filter: Option<Filter>,
}

impl QueryBuilder {
    fn merge_where_clauses(mut where_clauses: Vec<WhereClause>) -> Vec<WhereClause> {
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
    }
}
