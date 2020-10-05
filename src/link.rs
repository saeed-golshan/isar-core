use crate::error::Result;
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::object::object_id::ObjectId;

pub struct Link {
    forward_id: u16,
    backward_id: u16,
    foreign_collection_id: u16,
    foreign_link: Option<Box<Link>>,
    db: Db,
}

impl Link {
    pub fn add(&self, txn: &Txn, from: ObjectId, to: ObjectId) -> Result<()> {
        /*let from_bytes = from.to_bytes_with_prefix(self.forward_id);
        let to_bytes = to.to_bytes();
        self.db.put_no_dup_data(txn, &from_bytes, &to_bytes)?;

        let from_bytes = from.to_bytes();
        let to_bytes = to.to_bytes_with_prefix(self.backward_id);
        self.db.put_no_dup_data(txn, &to_bytes, &from_bytes)?;*/

        Ok(())
    }

    pub fn remove(&self, txn: &Txn, from: ObjectId, to: ObjectId) -> Result<()> {
        /*let from_bytes = from.to_bytes_with_prefix(self.forward_id);
        let to_bytes = to.to_bytes();
        self.db.delete(txn, &from_bytes, Some(&to_bytes))*/
        Ok(())
    }
}
