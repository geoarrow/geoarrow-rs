//! Abstractions for Arrow tables. Useful for dataset IO where data will have geometries and
//! attributes.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::error::Result;

#[derive(Debug)]
pub struct GeoTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    geometry_column_index: usize,
}

impl GeoTable {
    pub fn try_new(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        geometry_column_index: usize,
    ) -> Result<Self> {
        // TODO: validate
        Ok(Self {
            schema,
            batches,
            geometry_column_index,
        })
    }

    pub fn len(&self) -> usize {
        self.batches.iter().fold(0, |sum, val| sum + val.num_rows())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_inner(self) -> (SchemaRef, Vec<RecordBatch>, usize) {
        (self.schema, self.batches, self.geometry_column_index)
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn batches(&self) -> &Vec<RecordBatch> {
        &self.batches
    }

    pub fn geometry_column_index(&self) -> usize {
        self.geometry_column_index
    }
}
