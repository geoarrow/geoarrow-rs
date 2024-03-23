pub mod chunks;
pub mod getitem;
pub mod primitive;
pub mod repr;

pub use primitive::{
    ChunkedBooleanArray, ChunkedFloat16Array, ChunkedFloat32Array, ChunkedFloat64Array,
    ChunkedInt16Array, ChunkedInt32Array, ChunkedInt64Array, ChunkedInt8Array,
    ChunkedLargeStringArray, ChunkedStringArray, ChunkedUInt16Array, ChunkedUInt32Array,
    ChunkedUInt64Array, ChunkedUInt8Array,
};

use pyo3::prelude::*;
use pyo3::types::PyIterator;

use crate::array::*;
use crate::error::PyGeoArrowResult;

macro_rules! impl_chunked_array {
    (
        $(#[$($attrss:meta)*])*
        pub struct $struct_name:ident(pub(crate) $geoarrow_arr:ty);
    ) => {
        $(#[$($attrss)*])*
        #[pyclass(module = "geoarrow.rust.core._rust")]
        pub struct $struct_name(pub(crate) $geoarrow_arr);

        impl From<$geoarrow_arr> for $struct_name {
            fn from(value: $geoarrow_arr) -> Self {
                Self(value)
            }
        }

        impl From<$struct_name> for $geoarrow_arr {
            fn from(value: $struct_name) -> Self {
                value.0
            }
        }
    };
}

impl_chunked_array! {
    /// An immutable chunked array of Point geometries using GeoArrow's in-memory representation.
    pub struct ChunkedPointArray(pub(crate) geoarrow::chunked_array::ChunkedPointArray);
}
impl_chunked_array! {
    /// An immutable chunked array of LineString geometries using GeoArrow's in-memory
    /// representation.
    pub struct ChunkedLineStringArray(pub(crate) geoarrow::chunked_array::ChunkedLineStringArray<i32>);
}
impl_chunked_array! {
    /// An immutable chunked array of Polygon geometries using GeoArrow's in-memory representation.
    pub struct ChunkedPolygonArray(pub(crate) geoarrow::chunked_array::ChunkedPolygonArray<i32>);
}
impl_chunked_array! {
    /// An immutable chunked array of MultiPoint geometries using GeoArrow's in-memory
    /// representation.
    pub struct ChunkedMultiPointArray(pub(crate) geoarrow::chunked_array::ChunkedMultiPointArray<i32>);
}
impl_chunked_array! {
    /// An immutable chunked array of MultiLineString geometries using GeoArrow's in-memory
    /// representation.
    pub struct ChunkedMultiLineStringArray(pub(crate) geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>);
}
impl_chunked_array! {
    /// An immutable chunked array of MultiPolygon geometries using GeoArrow's in-memory
    /// representation.
    pub struct ChunkedMultiPolygonArray(pub(crate) geoarrow::chunked_array::ChunkedMultiPolygonArray<i32>);
}
impl_chunked_array! {
    /// An immutable chunked array of Geometry geometries using GeoArrow's in-memory
    /// representation.
    pub struct ChunkedMixedGeometryArray(pub(crate) geoarrow::chunked_array::ChunkedMixedGeometryArray<i32>);
}
impl_chunked_array! {
    /// An immutable chunked array of GeometryCollection geometries using GeoArrow's in-memory
    /// representation.
    pub struct ChunkedGeometryCollectionArray(pub(crate) geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>);
}
impl_chunked_array! {
    /// An immutable chunked array of WKB-encoded geometries using GeoArrow's in-memory
    /// representation.
    pub struct ChunkedWKBArray(pub(crate) geoarrow::chunked_array::ChunkedWKBArray<i32>);
}
impl_chunked_array! {
    /// An immutable chunked array of Rect geometries using GeoArrow's in-memory representation.
    pub struct ChunkedRectArray(pub(crate) geoarrow::chunked_array::ChunkedRectArray);
}

// Constructors

macro_rules! impl_chunks {
    ($chunked_py_array:ty, $py_array:ty) => {
        #[pymethods]
        impl $chunked_py_array {
            /// Constructor
            ///
            /// Args:
            ///     chunks: an iterable of chunks to construct a chunked array
            ///
            /// Returns:
            ///     a new chunked array.
            #[new]
            pub fn py_new(chunks: &PyAny) -> PyGeoArrowResult<Self> {
                let rust_chunks = PyIterator::from_object(chunks)?
                    .into_iter()
                    .map(|object| {
                        let python_arr = object?.extract::<$py_array>()?;
                        Ok(python_arr.0)
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                let chunked_array = geoarrow::chunked_array::ChunkedGeometryArray::new(rust_chunks);
                Ok(Self(chunked_array))
            }
        }
    };
}

impl_chunks!(ChunkedPointArray, PointArray);
impl_chunks!(ChunkedLineStringArray, LineStringArray);
impl_chunks!(ChunkedPolygonArray, PolygonArray);
impl_chunks!(ChunkedMultiPointArray, MultiPointArray);
impl_chunks!(ChunkedMultiLineStringArray, MultiLineStringArray);
impl_chunks!(ChunkedMultiPolygonArray, MultiPolygonArray);
impl_chunks!(ChunkedMixedGeometryArray, MixedGeometryArray);
// impl_chunks!(ChunkedRectArray, RectArray);
impl_chunks!(ChunkedGeometryCollectionArray, GeometryCollectionArray);
impl_chunks!(ChunkedWKBArray, WKBArray);
