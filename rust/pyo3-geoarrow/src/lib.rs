#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod array;
mod array_reader;
mod chunked_array;
mod coord_buffer;
mod coord_type;
mod crs;
pub mod data_type;
mod dimension;
mod edges;
mod error;
mod ffi;
pub mod input;
mod offset_buffer;
mod scalar;
mod utils;

pub use array::PyGeoArray;
pub use array_reader::PyGeoArrayReader;
pub use chunked_array::PyGeoChunkedArray;
pub use coord_buffer::PyCoordBuffer;
pub use coord_type::PyCoordType;
pub use crs::{PyCrs, PyprojCRSTransform};
pub use dimension::PyDimension;
pub use edges::PyEdges;
pub use error::{PyGeoArrowError, PyGeoArrowResult};
pub use offset_buffer::PyOffsetBuffer;
pub use scalar::PyGeoScalar;
