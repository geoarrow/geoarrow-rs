use crate::array::*;
use crate::chunked_array::*;
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3::{PyAny, PyResult};

macro_rules! impl_from_arrow_chunks {
    ($py_chunked_array:ty, $py_array:ty, $rs_chunked_array:ty) => {
        #[pymethods]
        impl $py_chunked_array {
            /// Construct this chunked array from existing Arrow data
            ///
            /// This is a temporary workaround for [this pyarrow
            /// issue](https://github.com/apache/arrow/issues/38717), where it's currently impossible to
            /// read a pyarrow [`ChunkedArray`][pyarrow.ChunkedArray] directly without adding a direct
            /// dependency on pyarrow.
            ///
            /// Args:
            ///     input: Arrow arrays to use for constructing this object
            ///
            /// Returns:
            ///     Self
            #[classmethod]
            fn from_arrow_arrays(_cls: &PyType, input: Vec<&PyAny>) -> PyResult<Self> {
                let py_arrays = input
                    .into_iter()
                    .map(|x| x.extract())
                    .collect::<PyResult<Vec<$py_array>>>()?;
                Ok(<$rs_chunked_array>::new(
                    py_arrays.into_iter().map(|py_array| py_array.0).collect(),
                )
                .into())
            }
        }
    };
}

impl_from_arrow_chunks!(
    ChunkedPointArray,
    PointArray,
    geoarrow::chunked_array::ChunkedPointArray
);
impl_from_arrow_chunks!(
    ChunkedLineStringArray,
    LineStringArray,
    geoarrow::chunked_array::ChunkedLineStringArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedPolygonArray,
    PolygonArray,
    geoarrow::chunked_array::ChunkedPolygonArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedMultiPointArray,
    MultiPointArray,
    geoarrow::chunked_array::ChunkedMultiPointArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedMultiLineStringArray,
    MultiLineStringArray,
    geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedMultiPolygonArray,
    MultiPolygonArray,
    geoarrow::chunked_array::ChunkedMultiPolygonArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedMixedGeometryArray,
    MixedGeometryArray,
    geoarrow::chunked_array::ChunkedMixedGeometryArray<i32>
);
// impl_from_arrow_chunks!(
//     ChunkedRectArray,
//     RectArray,
//     geoarrow::chunked_array::ChunkedRectArray
// );
impl_from_arrow_chunks!(
    ChunkedGeometryCollectionArray,
    GeometryCollectionArray,
    geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>
);
impl_from_arrow_chunks!(
    ChunkedWKBArray,
    WKBArray,
    geoarrow::chunked_array::ChunkedWKBArray<i32>
);
