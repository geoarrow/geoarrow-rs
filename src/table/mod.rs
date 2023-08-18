//! Abstractions for Arrow tables. Useful for dataset IO where data will have geometries and
//! attributes.

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::types::Offset;

use crate::array::GeometryArray;

pub struct Table<O: Offset> {
    schema: Schema,
    batches: Vec<Chunk<Box<dyn Array>>>,
    geometry_column: GeometryArray<O>
}

impl<O: Offset> Table<O> {

}
