use crate::data_dbs::IndexType;
use crate::error::{illegal_arg, illegal_state, Result};
use crate::field::Field;
use crate::index::Index;
use crate::link::Link;
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::object_id::{ObjectId, ObjectIdGenerator};
use crate::query::where_clause::WhereClause;
use rand::random;

pub struct IsarCollection {
    pub name: String,
    pub id: u16,
    fields: Vec<Field>,
    links: Vec<Link>,
    indexes: Vec<Index>,
    static_size: usize,
    first_dynamic_field_index: Option<usize>,
    primary_db: Db,
    oidg: ObjectIdGenerator,
}

impl IsarCollection {
    pub fn new(
        name: String,
        id: u16,
        fields: Vec<Field>,
        links: Vec<Link>,
        indexes: Vec<Index>,
        primary_db: Db,
    ) -> Self {
        let static_size = Self::calculate_static_size(&fields);
        let first_dynamic_field_index = Self::find_first_dynamic_field_index(&fields);

        IsarCollection {
            name,
            id,
            fields,
            links,
            indexes,
            static_size,
            first_dynamic_field_index,
            primary_db,
            oidg: ObjectIdGenerator::new(random()),
        }
    }

    fn calculate_static_size(fields: &[Field]) -> usize {
        fields
            .iter()
            .map(|f| f.data_type.get_static_size() as usize)
            .sum()
    }

    fn find_first_dynamic_field_index(fields: &[Field]) -> Option<usize> {
        fields
            .iter()
            .enumerate()
            .filter(|(_, field)| field.data_type.is_dynamic())
            .map(|(i, _)| i)
            .next()
    }

    pub fn get<'txn>(&self, txn: &'txn Txn, oid: ObjectId) -> Result<Option<&'txn [u8]>> {
        let oid_bytes = oid.to_bytes_with_prefix(self.id);
        self.primary_db.get(txn, &oid_bytes)
    }

    pub fn put(&mut self, txn: &Txn, oid: Option<ObjectId>, object: &[u8]) -> Result<ObjectId> {
        let oid = if let Some(oid) = oid {
            if !self.delete_from_indexes(txn, oid)? {
                illegal_state("ObjectId provided but no entry found.")?;
            }
            oid
        } else {
            self.oidg.generate()
        };

        if !self.verify_object(object) {
            illegal_arg("Provided object is invalid.")?;
        }

        let oid_bytes = oid.to_bytes_with_prefix(self.id);

        for index in &self.indexes {
            index.create_for_object(txn, oid, object)?;
        }

        self.primary_db.put(txn, &oid_bytes, object)?;
        Ok(oid)
    }

    pub fn delete(&self, txn: &Txn, oid: ObjectId) -> Result<()> {
        if self.delete_from_indexes(txn, oid)? {
            let oid_bytes = oid.to_bytes_with_prefix(self.id);
            self.primary_db.delete(txn, &oid_bytes, None)?;
        }
        Ok(())
    }

    pub fn clear(&self, txn: &Txn) -> Result<()> {
        for index in &self.indexes {
            index.clear(txn)?;
        }
        let cursor = self.primary_db.cursor(txn)?;
        cursor.delete_key_prefix(&self.get_prefix())?;

        Ok(())
    }

    pub fn create_where_clause(&self, index_index: usize) -> WhereClause {
        if let Some(index) = self.indexes.get(index_index) {
            index.create_where_clause()
        } else {
            WhereClause::new(&self.get_prefix(), IndexType::Primary)
        }
    }

    fn get_prefix(&self) -> [u8; 2] {
        u16::to_le_bytes(self.id)
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

    fn delete_from_indexes(&self, txn: &Txn, oid: ObjectId) -> Result<bool> {
        let existing_object = self.get(txn, oid)?;
        if let Some(existing_object) = existing_object {
            for index in &self.indexes {
                index.delete_for_object(txn, oid, existing_object)?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::collection::IsarCollection;
    use crate::field::{DataType, Field};
    use crate::lmdb::db::Db;

    #[test]
    fn test_calculate_static_size() {
        let fields1 = vec![Field::new(DataType::Bool, 0), Field::new(DataType::Int, 1)];
        let fields2 = vec![
            Field::new(DataType::Bool, 0),
            Field::new(DataType::String, 1),
            Field::new(DataType::Bytes, 9),
            Field::new(DataType::Double, 9),
        ];

        assert_eq!(IsarCollection::calculate_static_size(&fields1), 9);
        assert_eq!(IsarCollection::calculate_static_size(&fields2), 25);
    }

    #[test]
    fn test_find_first_dynamic_field_index() {
        let static_fields = vec![Field::new(DataType::Bool, 0), Field::new(DataType::Int, 1)];
        let mixed_fields = vec![
            Field::new(DataType::Bool, 0),
            Field::new(DataType::String, 1),
        ];
        let dynamic_fields = vec![Field::new(DataType::String, 0)];

        assert_eq!(
            IsarCollection::find_first_dynamic_field_index(&static_fields),
            None
        );
        assert_eq!(
            IsarCollection::find_first_dynamic_field_index(&mixed_fields),
            Some(1)
        );
        assert_eq!(
            IsarCollection::find_first_dynamic_field_index(&dynamic_fields),
            Some(0)
        );
    }

    #[test]
    fn test_verify_object() {
        let static_fields = vec![Field::new(DataType::Bool, 0), Field::new(DataType::Int, 1)];
        let string_field = vec![Field::new(DataType::String, 0)];

        let mixed_fields = vec![
            Field::new(DataType::Bool, 0),
            Field::new(DataType::String, 1),
            Field::new(DataType::Bytes, 9),
        ];

        fn col(fields: &[Field]) -> IsarCollection {
            IsarCollection::new(
                "".to_string(),
                0,
                fields.to_vec(),
                vec![],
                vec![],
                Db { dbi: 0 },
            )
        }

        assert_eq!(col(&static_fields).verify_object(&[]), false);
        assert_eq!(col(&static_fields).verify_object(&[1, 4]), false);
        assert_eq!(col(&static_fields).verify_object(&[0; 9]), true);
        assert_eq!(col(&static_fields).verify_object(&[0; 10]), false);

        assert_eq!(col(&string_field).verify_object(&[]), false);
        assert_eq!(col(&string_field).verify_object(&[0; 8]), true);
        assert_eq!(col(&string_field).verify_object(&[0; 9]), false);
        assert_eq!(
            col(&string_field).verify_object(&[8, 0, 0, 0, 3, 0, 0, 0, 60, 61, 62]),
            true
        );
        assert_eq!(
            col(&string_field).verify_object(&[1, 0, 0, 0, 3, 0, 0, 0, 60, 61, 62]),
            false
        );
        assert_eq!(
            col(&string_field).verify_object(&[9, 0, 0, 0, 1, 0, 0, 0, 60, 61]),
            false
        );

        assert_eq!(col(&mixed_fields).verify_object(&[]), false);
        assert_eq!(col(&mixed_fields).verify_object(&[0; 17]), true);
        assert_eq!(col(&mixed_fields).verify_object(&[0; 18]), false);
        assert_eq!(
            col(&mixed_fields).verify_object(&[
                2, 17, 0, 0, 0, 1, 0, 0, 0, 18, 0, 0, 0, 3, 0, 0, 0, 63, 60, 61, 62
            ]),
            true
        );
        assert_eq!(
            col(&mixed_fields).verify_object(&[
                2, 17, 0, 0, 0, 1, 0, 0, 0, 18, 0, 0, 0, 3, 0, 0, 0, 63, 60, 61, 62, 63
            ]),
            false
        );
        assert_eq!(
            col(&mixed_fields).verify_object(&[
                2, 17, 0, 0, 0, 1, 0, 0, 0, 17, 0, 0, 0, 3, 0, 0, 0, 63, 60, 61, 62
            ]),
            false
        );
    }
}
