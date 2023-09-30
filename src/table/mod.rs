//! Abstractions for Arrow tables. Useful for dataset IO where data will have geometries and
//! attributes.

use crate::error::Result;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;

#[derive(Debug)]
pub struct GeoTable {
    schema: Schema,
    batches: Vec<Chunk<Box<dyn Array>>>,
    geometry_column_index: usize,
}

impl GeoTable {
    pub fn try_new(
        schema: Schema,
        batches: Vec<Chunk<Box<dyn Array>>>,
        geometry_column_index: usize,
    ) -> Result<Self> {
        // TODO: validate
        Ok(Self {
            schema,
            batches,
            geometry_column_index,
        })
    }

    pub fn into_inner(self) -> (Schema, Vec<Chunk<Box<dyn Array>>>, usize) {
        (self.schema, self.batches, self.geometry_column_index)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn batches(&self) -> &Vec<Chunk<Box<dyn Array>>> {
        &self.batches
    }

    pub fn geometry_column_index(&self) -> usize {
        self.geometry_column_index
    }
}
