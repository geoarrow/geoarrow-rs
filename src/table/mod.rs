//! Abstractions for Arrow tables. Useful for dataset IO where data will have geometries and
//! attributes.

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use crate::error::Result;

pub struct GeoTable {
    _schema: Schema,
    _batches: Vec<Chunk<Box<dyn Array>>>,
    _geometry_column_index: usize,
}

impl GeoTable {
    pub fn try_new(
        schema: Schema,
        batches: Vec<Chunk<Box<dyn Array>>>,
        geometry_column_index: usize,
    ) -> Result<Self> {
        // TODO: validate
        Ok(Self {
            _schema: schema,
            _batches: batches,
            _geometry_column_index: geometry_column_index,
        })
    }
}
