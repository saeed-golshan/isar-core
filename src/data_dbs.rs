use crate::lmdb::db::Db;

#[derive(Clone, Copy)]
pub struct DataDbs {
    pub primary: Db,
    pub secondary: Db,
    pub secondary_dup: Db,
    pub links: Db,
}

impl DataDbs {
    pub fn new(primary: Db, secondary: Db, secondary_dup: Db, links: Db) -> Self {
        DataDbs {
            primary,
            secondary,
            secondary_dup,
            links,
        }
    }

    pub fn get(&self, index_type: IndexType) -> Db {
        match index_type {
            IndexType::Primary => self.primary,
            IndexType::Secondary => self.secondary,
            IndexType::SecondaryDup => self.secondary_dup,
            IndexType::Links => self.links,
        }
    }
}

#[derive(Copy, Clone, PartialEq)]
pub enum IndexType {
    Primary,
    Secondary,
    SecondaryDup,
    Links,
}
