//! Abstractions for Arrow tables. Useful for dataset IO where data will have geometries and
//! attributes.

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::types::Offset;

use crate::array::GeometryArray;
use crate::error::Result;

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
}
