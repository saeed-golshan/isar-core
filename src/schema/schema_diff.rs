use crate::schema::collection_schema::CollectionSchema;
use crate::schema::property_schema::PropertySchema;
use crate::schema::index_schema::IndexSchema;
use crate::schema::schema::{CollectionSchema, IndexSchema, PropertySchema, Schema};
use crate::schema::Schema;

struct SchemaDiff<'a> {
    added_properties: Vec<&'a PropertySchema>,
    properties_removed: bool,
    removed_indexes: Vec<&'a IndexSchema>,
    added_indexes: Vec<&'a IndexSchema>,
}

impl<'a> SchemaDiff<'a> {
    fn create(old_schema: &'a Schema, new_schema: &'a Schema) -> Vec<Self> {
        let properties_removed = old_schema
            .properties
            .iter()
            .any(|old_property| !new_schema.properties.contains(old_property));

        let added_properties = new_schema
            .properties
            .iter()
            .filter(|new_property| !old_schema.properties.contains(new_property))
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
        let properties_removed = old_schema
            .properties
            .iter()
            .any(|old_property| !new_schema.properties.contains(old_property));

        let added_properties = new_schema
            .properties
            .iter()
            .filter(|new_property| !old_schema.properties.contains(new_property))
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
            properties_removed,
            added_properties,
            removed_indexes,
            added_indexes,
        }
    }

    fn no_change(&self) -> bool {
        !self.properties_removed
            && self.added_properties.is_empty()
            && self.removed_indexes.is_empty()
            && self.added_indexes.is_empty()
    }
}
