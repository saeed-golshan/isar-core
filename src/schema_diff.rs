use crate::schema::{Schema, SchemaField, SchemaIndex};

struct SchemaDiff<'a> {
    added_fields: Vec<&'a SchemaField>,
    fields_removed: bool,
    removed_indices: Vec<&'a SchemaIndex>,
    added_indices: Vec<&'a SchemaIndex>,
}

impl<'a> SchemaDiff<'a> {
    fn create(old_schema: &'a Schema, new_schema: &'a Schema) -> Self {
        let fields_removed = old_schema
            .fields
            .iter()
            .any(|old_field| !new_schema.fields.contains(old_field));

        let added_fields = new_schema
            .fields
            .iter()
            .filter(|new_field| !old_schema.fields.contains(new_field))
            .collect();

        let removed_indices = old_schema
            .indices
            .iter()
            .filter(|old_index| !new_schema.indices.contains(old_index))
            .collect();

        let added_indices = new_schema
            .indices
            .iter()
            .filter(|new_index| !old_schema.indices.contains(new_index))
            .collect();

        SchemaDiff {
            fields_removed,
            added_fields,
            removed_indices,
            added_indices,
        }
    }

    fn no_change(&self) -> bool {
        !self.fields_removed
            && self.added_fields.is_empty()
            && self.removed_indices.is_empty()
            && self.added_indices.is_empty()
    }
}
