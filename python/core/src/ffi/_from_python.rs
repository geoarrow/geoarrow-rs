use std::sync::Arc;

use crate::array::*;
use crate::chunked_array::*;
use crate::ffi::stream_chunked::ArrowArrayStreamReader;
use crate::table::GeoTable;
use arrow::datatypes::Field;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::{
    ArrowArrayStreamReader as ArrowRecordBatchStreamReader, FFI_ArrowArrayStream,
};
use arrow_array::Array;
use arrow_array::{make_array, ArrayRef, RecordBatchReader};
use geoarrow::array::from_arrow_array;
use geoarrow::chunked_array::{from_arrow_chunks, ChunkedGeometryArrayTrait};
use geoarrow::datatypes::GeoDataType;
use geoarrow::io::geozero::ToGeometry;
use geoarrow::GeometryArrayTrait;
use geozero::geojson::GeoJsonString;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict, PyTuple, PyType};
use pyo3::{intern, PyAny, PyResult};
