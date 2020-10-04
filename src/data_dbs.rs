use crate::lmdb::db::Db;

#[derive(Clone, Copy)]
pub struct DataDbs {
    pub info: Db,
    pub primary: Db,
    pub secondary: Db,
    pub secondary_dup: Db,
    pub links: Db,
}

impl DataDbs {
    pub fn new(info: Db, primary: Db, secondary: Db, secondary_dup: Db, links: Db) -> Self {
        DataDbs {
            info,
            primary,
            secondary,
            secondary_dup,
            links,
        }
    }
}
