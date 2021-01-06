use crate::lmdb::db::Db;

#[derive(Clone, Copy)]
pub struct DataDbs {
    pub info: Db,
    pub primary: Db,
    pub secondary: Db,
    pub secondary_dup: Db,
}

impl DataDbs {
    #[cfg(test)]
    pub fn debug_new() -> Self {
        DataDbs {
            info: Db::debug_new(false),
            primary: Db::debug_new(false),
            secondary: Db::debug_new(false),
            secondary_dup: Db::debug_new(true),
        }
    }
}
