use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaBuilder};
use geozero::{FeatureProcessor, GeomProcessor, PropertyProcessor};

use crate::error::Result;
use crate::io::geozero::table::builder::anyvalue::AnyBuilder;
use indexmap::IndexMap;

/// A builder for a single RecordBatch of properties
// TODO: store an Arc<Schema> on this struct? Especially when known or user-provided?
// TODO: switch to ordered Vec of builders instead of a hashmap for sources like postgis
pub struct PropertiesBatchBuilder {
    /// A mapping from column name to its builder.
    ///
    /// For now, we use an IndexMap in order to maintain
    ///
    /// This is enough for FlatGeobuf where we know the schema up front, but for other formats like GeoJSON
    ///
    /// Note: After you've built one batch, you should be able to use that batch's schema to provision the schema for the next batch.
    ///
    /// We want to track column ordering
    /// TODO: track column ordering?
    columns: IndexMap<String, AnyBuilder>,

    /// A counter for the number of rows that have been added, excluding the current row.
    ///
    /// This counter is necessary because many formats do not have a rigid schema up front. So we
    /// may add 1000 rows of data and on the 1001st row we see a new property name that we haven't
    /// seen before. In that case, we need to provision a new column and we need to know how many
    /// null values should be pre-filled before the current row.
    ///
    /// The counter does not include the current row. So a row counter of 0 is expected if
    /// ingesting the first row.
    row_counter: usize,
}

impl PropertiesBatchBuilder {
    pub fn new() -> Self {
        Self {
            columns: IndexMap::new(),
            row_counter: 0,
        }
    }

    /// Note: If this is called after `feature_end`, it will include the most recent feature.
    /// Otherwise, will be len - 1
    pub fn len(&self) -> usize {
        self.row_counter
    }

    pub fn add_single_property(
        &mut self,
        name: &str,
        value: &geozero::ColumnValue,
    ) -> geozero::error::Result<()> {
        if let Some(any_builder) = self.columns.get_mut(name) {
            any_builder.add_value(value);
        } else {
            // If this column name doesn't yet exist
            let builder = AnyBuilder::from_value_prefill(value, self.row_counter);
            self.columns.insert(name.to_string(), builder);
        };
        Ok(())
    }

    pub fn from_schema(schema: &Schema) -> Self {
        Self::from_schema_with_capacity(schema, 0)
    }

    pub fn from_schema_with_capacity(schema: &Schema, capacity: usize) -> Self {
        let mut columns = IndexMap::with_capacity(schema.fields().len());
        for field in schema.fields().iter() {
            columns.insert(
                field.name().clone(),
                AnyBuilder::from_data_type_with_capacity(field.data_type(), capacity),
            );
        }

        Self {
            columns,
            row_counter: 0,
        }
    }

    pub fn schema(&self) -> Schema {
        let mut schema_builder = SchemaBuilder::with_capacity(self.columns.len());
        for (name, builder) in self.columns.iter() {
            schema_builder.push(Field::new(name, builder.data_type(), true));
        }
        schema_builder.finish()
    }

    pub fn finish(self) -> Result<RecordBatch> {
        let mut schema_builder = SchemaBuilder::with_capacity(self.columns.len());
        let mut columns = Vec::with_capacity(self.columns.len());

        for (name, builder) in self.columns.into_iter() {
            let array = builder.finish()?;
            schema_builder.push(Field::new(name, array.data_type().clone(), true));
            columns.push(array);
        }

        Ok(RecordBatch::try_new(
            Arc::new(schema_builder.finish()),
            columns,
        )?)
    }
}

impl Default for PropertiesBatchBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PropertyProcessor for PropertiesBatchBuilder {
    fn property(
        &mut self,
        // TODO: is this the row? Is this the positional index within the column?
        _idx: usize,
        name: &str,
        value: &geozero::ColumnValue,
    ) -> geozero::error::Result<bool> {
        self.add_single_property(name, value)?;
        Ok(false)
    }
}

// Note: We only implement this GeomProcessor here so that we can override some methods on the
// FeatureProcessor impl, which requires GeomProcessor.
impl GeomProcessor for PropertiesBatchBuilder {}

// It's useful to impl FeatureProcessor for PropertiesBatchBuilder even though the latter doesn't
// handle geometries so that we can manage adding null values to columns that weren't touched in
// this row.
impl FeatureProcessor for PropertiesBatchBuilder {
    fn properties_end(&mut self) -> geozero::error::Result<()> {
        for (_name, col) in self.columns.iter_mut() {
            if col.len() == self.row_counter + 1 {
                // This is _expected_ when all columns were visited
                continue;
            }

            // This can happen if a column did not have a value in this row, such as if the
            // properties keys in GeoJSON change per row.
            if col.len() == self.row_counter {
                col.append_null();
            } else {
                panic!("unexpected length");
            }
        }

        Ok(())
    }

    fn feature_end(&mut self, _idx: u64) -> geozero::error::Result<()> {
        self.row_counter += 1;
        Ok(())
    }
}
