use super::raw_object_set::RawObjectSet;
use isar_core::collection::IsarCollection;
use isar_core::instance::IsarInstance;
use isar_core::query::query::Query;
use isar_core::query::query_builder::QueryBuilder;
use isar_core::query::where_clause::WhereClause;
use isar_core::txn::IsarTxn;

#[no_mangle]
pub extern "C" fn isar_qb_create(
    isar: Option<&IsarInstance>,
    collection: Option<&IsarCollection>,
) -> *mut QueryBuilder {
    let builder = isar.unwrap().create_query_builder(collection.unwrap());
    Box::into_raw(Box::new(builder))
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_add_where_clause(
    builder: Option<&mut QueryBuilder>,
    where_clause: *mut WhereClause,
) {
    let wc = *Box::from_raw(where_clause);
    builder.unwrap().add_where_clause(wc).unwrap();
}

#[no_mangle]
pub unsafe extern "C" fn isar_qb_build(builder: *mut QueryBuilder) -> *mut Query {
    let query = Box::from_raw(builder).build();
    Box::into_raw(Box::new(query))
}

#[no_mangle]
pub unsafe extern "C" fn isar_q_find_all(
    query: &Query,
    txn: &IsarTxn,
    result: &mut RawObjectSet,
) -> u8 {
    isar_try! {
        result.fill_from_query(query, txn)?;
    }
}
