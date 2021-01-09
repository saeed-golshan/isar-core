use crate::collection::IsarCollection;
use crate::error::Result;
use crate::index::Index;
use crate::lmdb::db::Db;
use crate::lmdb::txn::Txn;
use crate::object::data_type::DataType;
use crate::object::object_builder::ObjectBuilder;
use crate::object::object_info::ObjectInfo;
use crate::object::property::Property;

pub struct CollectionMigrator<'env> {
    retained_properties: Vec<Option<Property>>,
    object_info: &'env ObjectInfo,
    object_migration_required: bool,
    removed_indexes: Vec<&'env Index>,
    added_indexes: Vec<&'env Index>,
}

impl<'env> CollectionMigrator<'env> {
    pub fn create(
        collection: &'env IsarCollection,
        existing_collection: &'env IsarCollection,
    ) -> Self {
        let oi = collection.get_object_info();
        let existing_oi = existing_collection.get_object_info();

        let mut retained_properties = vec![];
        for property in oi.get_properties() {
            let existing_property = existing_oi.get_properties().iter().find(|p| *p == property);
            retained_properties.push(existing_property.copied());
        }
        let object_migration_required = oi.get_properties() != existing_oi.get_properties();

        let mut added_indexes = vec![];
        for index in collection.get_indexes() {
            let existed = existing_collection
                .get_indexes()
                .iter()
                .any(|i| i.get_id() == index.get_id());
            if !existed {
                added_indexes.push(index);
            }
        }

        let mut removed_indexes = vec![];
        for existing_index in existing_collection.get_indexes() {
            let still_exists = collection
                .get_indexes()
                .iter()
                .any(|i| i.get_id() == existing_index.get_id());
            if !still_exists {
                removed_indexes.push(existing_index);
            }
        }

        CollectionMigrator {
            retained_properties,
            object_info: collection.get_object_info(),
            object_migration_required,
            added_indexes,
            removed_indexes,
        }
    }

    pub fn migrate(self, txn: &Txn, primary_db: Db) -> Result<()> {
        for removed_index in self.removed_indexes {
            removed_index.clear(txn)?;
        }

        if !self.added_indexes.is_empty() || self.object_migration_required {
            let mut cursor = primary_db.cursor(txn)?;
            if cursor.move_to_first()?.is_none() {
                return Ok(());
            }

            if self.object_migration_required {
                for entry in cursor.iter() {
                    let (key, object) = entry?;
                    let mut ob = ObjectBuilder::new(self.object_info);
                    for property in &self.retained_properties {
                        Self::write_property_to_ob(&mut ob, property, object);
                    }
                    let ob_result = ob.finish();
                    let new_object = ob_result.as_bytes();
                    primary_db.put(txn, key, new_object)?;
                    for index in &self.added_indexes {
                        index.create_for_object(&txn, key, new_object)?;
                    }
                }
            } else {
                for entry in cursor.iter() {
                    let (key, object) = entry?;
                    for index in &self.added_indexes {
                        index.create_for_object(&txn, key, object)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn write_property_to_ob(ob: &mut ObjectBuilder, property: &Option<Property>, object: &[u8]) {
        if let Some(p) = property {
            match p.data_type {
                DataType::Byte => ob.write_byte(p.get_byte(object)),
                DataType::Int => ob.write_int(p.get_int(object)),
                DataType::Float => ob.write_float(p.get_float(object)),
                DataType::Long => ob.write_long(p.get_long(object)),
                DataType::Double => ob.write_double(p.get_double(object)),
                DataType::String => ob.write_string(p.get_string(object)),
                DataType::ByteList => ob.write_byte_list(p.get_byte_list(object)),
                DataType::IntList => ob.write_int_list(p.get_int_list(object)),
                DataType::FloatList => ob.write_float_list(p.get_float_list(object)),
                DataType::LongList => ob.write_long_list(p.get_long_list(object)),
                DataType::DoubleList => ob.write_double_list(p.get_double_list(object)),
                DataType::StringList => {
                    unimplemented!("String list migration not ready yet")
                }
            }
        } else {
            ob.write_null();
        }
    }
}
