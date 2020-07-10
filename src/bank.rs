use crate::data_dbs::{DataDbs, IndexType};
use crate::error::{illegal_arg, illegal_state, Result};
use crate::field::Field;
use crate::index::Index;
use crate::lmdb::txn::Txn;
use crate::object_id::{ObjectId, ObjectIdGenerator};
use crate::query::where_clause::WhereClause;
use rand::random;

pub struct IsarBank {
    pub name: String,
    pub id: u16,
    fields: Vec<Field>,
    indexes: Vec<Index>,
    static_size: usize,
    first_dynamic_field_index: Option<usize>,
    dbs: DataDbs,
    oidg: ObjectIdGenerator,
}

impl IsarBank {
    pub fn new(
        name: String,
        id: u16,
        fields: Vec<Field>,
        indexes: Vec<Index>,
        dbs: DataDbs,
    ) -> Self {
        let mut offset = 0;
        for field in &fields {
            assert_eq!(field.offset, offset);
            offset += field.data_type.get_static_size() as usize;
        }

        let static_size = fields
            .iter()
            .map(|f| f.data_type.get_static_size() as usize)
            .sum();
        let first_dynamic_field_index = fields
            .iter()
            .enumerate()
            .filter(|(_, field)| field.data_type.is_dynamic())
            .map(|(i, _)| i)
            .next();

        IsarBank {
            name,
            id,
            fields,
            indexes,
            static_size,
            first_dynamic_field_index,
            dbs,
            oidg: ObjectIdGenerator::new(random(), id),
        }
    }

    pub fn get<'txn>(&self, txn: &'txn Txn, oid: &ObjectId) -> Result<Option<&'txn [u8]>> {
        self.dbs.primary.get(txn, &oid.to_bytes())
    }

    pub fn put(&mut self, txn: &Txn, oid: Option<ObjectId>, object: &[u8]) -> Result<ObjectId> {
        let oid = if let Some(oid) = oid {
            self.verify_object_id(&oid)?;
            if !self.delete_from_indexes(txn, &oid)? {
                illegal_state("ObjectId provided but no entry found.")?;
            }
            oid
        } else {
            self.oidg.generate()?
        };

        if !self.verify_object(object) {
            illegal_arg("Provided object is invalid.")?;
        }

        let oid_bytes = oid.to_bytes();

        for index in &self.indexes {
            let index_db = self.dbs.get(index.get_type());
            let index_key = index.create_key(object);
            index_db.put(txn, &index_key, &oid_bytes)?;
        }

        self.dbs.primary.put(txn, &oid_bytes, object)?;
        Ok(oid)
    }

    pub fn delete(&self, txn: &Txn, oid: &ObjectId) -> Result<()> {
        if self.delete_from_indexes(txn, oid)? {
            self.dbs.primary.delete(txn, &oid.to_bytes(), None)?;
        }
        Ok(())
    }

    pub fn clear(&self, txn: &Txn) -> Result<()> {
        for index in &self.indexes {
            let index_db = self.dbs.get(index.get_type());
            let mut cursor = index_db.cursor(txn)?;
            cursor.delete_key_prefix(&index.get_prefix())?;
        }
        let mut cursor = self.dbs.primary.cursor(txn)?;
        cursor.delete_key_prefix(&self.get_prefix())
    }

    pub fn new_where_clause(
        &self,
        index: usize,
        lower_size: usize,
        upper_size: usize,
    ) -> WhereClause {
        let index = self.indexes.get(index);
        let (prefix, index_type) = if let Some(index) = index {
            (index.get_prefix().to_vec(), index.get_type())
        } else {
            (self.get_prefix().to_vec(), IndexType::Primary)
        };

        WhereClause::new(&prefix, lower_size, upper_size, index_type)
    }

    fn get_prefix(&self) -> [u8; 2] {
        u16::to_le_bytes(self.id)
    }

    fn verify_object_id(&self, object_id: &ObjectId) -> Result<()> {
        if object_id.get_bank_id() != self.id {
            illegal_arg("ObjectId does not match the bank.")?;
        }
        Ok(())
    }

    fn verify_object(&self, object: &[u8]) -> bool {
        if let Some(first_dynamic_index) = self.first_dynamic_field_index {
            if object.len() < self.static_size {
                return false;
            }

            let mut dynamic_offset = self.static_size;
            for field in self.fields.iter().skip(first_dynamic_index) {
                if !field.is_null(object) {
                    let offset = field.get_data_offset(object);
                    if offset != dynamic_offset {
                        return false;
                    }

                    let length = field.get_length(object);
                    dynamic_offset += length;
                }
            }

            object.len() == dynamic_offset
        } else {
            object.len() == self.static_size
        }
    }

    fn delete_from_indexes(&self, txn: &Txn, oid: &ObjectId) -> Result<bool> {
        let old_object = self.get(txn, &oid)?;
        if let Some(old_object) = old_object {
            let oid_bytes = oid.to_bytes();
            for index in &self.indexes {
                let index_db = self.dbs.get(index.get_type());
                let index_key = index.create_key(old_object);
                index_db.delete(txn, &index_key, Some(&oid_bytes))?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bank::IsarBank;
    use crate::data_dbs::DataDbs;
    use crate::field::{DataType, Field};
    use crate::lmdb::db::Db;

    #[test]
    fn test_verify_object() {
        let static_fields = vec![
            Field::new(String::from("f"), DataType::Bool, 0),
            Field::new(String::from("f"), DataType::Int, 1),
        ];
        let string_field = vec![Field::new(String::from("f"), DataType::String, 0)];

        let mixed_fields = vec![
            Field::new(String::from("f"), DataType::Bool, 0),
            Field::new(String::from("f"), DataType::String, 1),
            Field::new(String::from("f"), DataType::Bytes, 9),
        ];

        fn bank(fields: &[Field]) -> IsarBank {
            IsarBank::new(
                "".to_string(),
                0,
                fields.to_vec(),
                vec![],
                DataDbs {
                    primary: Db { dbi: 0 },
                    secondary: Db { dbi: 0 },
                    secondary_dup: Db { dbi: 0 },
                },
            )
        }

        assert_eq!(bank(&static_fields).verify_object(&[]), false);
        assert_eq!(bank(&static_fields).verify_object(&[1, 4]), false);
        assert_eq!(bank(&static_fields).verify_object(&[0; 9]), true);
        assert_eq!(bank(&static_fields).verify_object(&[0; 10]), false);

        assert_eq!(bank(&string_field).verify_object(&[]), false);
        assert_eq!(bank(&string_field).verify_object(&[0; 8]), true);
        assert_eq!(bank(&string_field).verify_object(&[0; 9]), false);
        assert_eq!(
            bank(&string_field).verify_object(&[8, 0, 0, 0, 3, 0, 0, 0, 60, 61, 62]),
            true
        );
        assert_eq!(
            bank(&string_field).verify_object(&[1, 0, 0, 0, 3, 0, 0, 0, 60, 61, 62]),
            false
        );
        assert_eq!(
            bank(&string_field).verify_object(&[9, 0, 0, 0, 1, 0, 0, 0, 60, 61]),
            false
        );

        assert_eq!(bank(&mixed_fields).verify_object(&[]), false);
        assert_eq!(bank(&mixed_fields).verify_object(&[0; 17]), true);
        assert_eq!(bank(&mixed_fields).verify_object(&[0; 18]), false);
        assert_eq!(
            bank(&mixed_fields).verify_object(&[
                2, 17, 0, 0, 0, 1, 0, 0, 0, 18, 0, 0, 0, 3, 0, 0, 0, 63, 60, 61, 62
            ]),
            true
        );
        assert_eq!(
            bank(&mixed_fields).verify_object(&[
                2, 17, 0, 0, 0, 1, 0, 0, 0, 18, 0, 0, 0, 3, 0, 0, 0, 63, 60, 61, 62, 63
            ]),
            false
        );
        assert_eq!(
            bank(&mixed_fields).verify_object(&[
                2, 17, 0, 0, 0, 1, 0, 0, 0, 17, 0, 0, 0, 3, 0, 0, 0, 63, 60, 61, 62
            ]),
            false
        );
    }
}
