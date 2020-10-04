use crate::schema::collection_schema::CollectionSchema;
use crate::schema::field_schema::FieldSchema;
use crate::schema::index_schema::IndexSchema;
use crate::schema::schema::{CollectionSchema, FieldSchema, IndexSchema, Schema};
use crate::schema::Schema;

struct SchemaDiff<'a> {
    added_fields: Vec<&'a FieldSchema>,
    fields_removed: bool,
    removed_indexes: Vec<&'a IndexSchema>,
    added_indexes: Vec<&'a IndexSchema>,
}

impl<'a> SchemaDiff<'a> {
    fn create(old_schema: &'a Schema, new_schema: &'a Schema) -> Vec<Self> {
        let fields_removed = old_schema
            .fields
            .iter()
            .any(|old_field| !new_schema.fields.contains(old_field));

        let added_fields = new_schema
            .fields
            .iter()
            .filter(|new_field| !old_schema.fields.contains(new_field))
            .collect();

        let removed_indexes = old_schema
            .indexes
            .iter()
            .filter(|old_index| !new_schema.indexes.contains(old_index))
            .collect();

        let added_indexes = new_schema
            .indexes
            .iter()
            .filter(|new_index| !old_schema.indexes.contains(new_index))
            .collect();
    }

    fn create_collection_diff(
        old_schema: &'a CollectionSchema,
        new_schema: &'a CollectionSchema,
    ) -> Self {
        let fields_removed = old_schema
            .fields
            .iter()
            .any(|old_field| !new_schema.fields.contains(old_field));

        let added_fields = new_schema
            .fields
            .iter()
            .filter(|new_field| !old_schema.fields.contains(new_field))
            .collect();

        let removed_indexes = old_schema
            .indexes
            .iter()
            .filter(|old_index| !new_schema.indexes.contains(old_index))
            .collect();

        let added_indexes = new_schema
            .indexes
            .iter()
            .filter(|new_index| !old_schema.indexes.contains(new_index))
            .collect();

        SchemaDiff {
            fields_removed,
            added_fields,
            removed_indexes,
            added_indexes,
        }
    }

    fn no_change(&self) -> bool {
        !self.fields_removed
            && self.added_fields.is_empty()
            && self.removed_indexes.is_empty()
            && self.added_indexes.is_empty()
    }
}
