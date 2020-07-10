use crate::lmdb::db::Db;

#[derive(Clone, Copy)]
pub struct DataDbs {
    pub primary: Db,
    pub secondary: Db,
    pub secondary_dup: Db,
}

impl DataDbs {
    pub fn new(primary: Db, secondary: Db, secondary_dup: Db) -> Self {
        DataDbs {
            primary,
            secondary,
            secondary_dup,
        }
    }

    pub fn get(&self, index_type: IndexType) -> Db {
        match index_type {
            IndexType::Primary => self.primary,
            IndexType::Secondary => self.secondary,
            IndexType::SecondaryDup => self.secondary_dup,
        }
    }
}

#[derive(Copy, Clone, PartialEq)]
pub enum IndexType {
    Primary,
    Secondary,
    SecondaryDup,
}
