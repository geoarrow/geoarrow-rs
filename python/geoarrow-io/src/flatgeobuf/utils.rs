use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};

pub(super) fn apply_projection(schema: SchemaRef, columns: &Option<Vec<String>>) -> SchemaRef {
    if let Some(cols) = columns {
        let fields: Vec<_> = schema
            .fields()
            .iter()
            .filter(|f| cols.contains(f.name()))
            .cloned()
            .collect();
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
    } else {
        schema
    }
}
