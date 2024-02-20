use crate::array::*;
use crate::chunked_array::*;
use crate::ffi::from_python::utils::import_arrow_c_stream;
use crate::ffi::stream_chunked::ArrowArrayStreamReader;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3::{PyAny, PyResult};

macro_rules! impl_extract {
    ($py_chunked_array:ty, $rs_array:ty, $rs_chunked_array:ty) => {
        impl<'a> FromPyObject<'a> for $py_chunked_array {
            fn extract(ob: &'a PyAny) -> PyResult<Self> {
                let stream = import_arrow_c_stream(ob)?;
                let stream_reader = ArrowArrayStreamReader::try_new(stream)
                    .map_err(|err| PyValueError::new_err(err.to_string()))?;

                let mut geo_chunks = vec![];
                for array in stream_reader {
                    let array = array.map_err(|err| PyTypeError::new_err(err.to_string()))?;
                    let geo_array = <$rs_array>::try_from(array.as_ref())
                        .map_err(|err| PyValueError::new_err(err.to_string()))?;
                    geo_chunks.push(geo_array);
                }

                Ok(Self(<$rs_chunked_array>::new(geo_chunks)))
            }
        }
    };
}

impl_extract!(
    ChunkedPointArray,
    geoarrow::array::PointArray,
    geoarrow::chunked_array::ChunkedPointArray
);
impl_extract!(
    ChunkedLineStringArray,
    geoarrow::array::LineStringArray<i32>,
    geoarrow::chunked_array::ChunkedLineStringArray<i32>
);
impl_extract!(
    ChunkedPolygonArray,
    geoarrow::array::PolygonArray<i32>,
    geoarrow::chunked_array::ChunkedPolygonArray<i32>
);
impl_extract!(
    ChunkedMultiPointArray,
    geoarrow::array::MultiPointArray<i32>,
    geoarrow::chunked_array::ChunkedMultiPointArray<i32>
);
impl_extract!(
    ChunkedMultiLineStringArray,
    geoarrow::array::MultiLineStringArray<i32>,
    geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>
);
impl_extract!(
    ChunkedMultiPolygonArray,
    geoarrow::array::MultiPolygonArray<i32>,
    geoarrow::chunked_array::ChunkedMultiPolygonArray<i32>
);
impl_extract!(
    ChunkedMixedGeometryArray,
    geoarrow::array::MixedGeometryArray<i32>,
    geoarrow::chunked_array::ChunkedMixedGeometryArray<i32>
);
// impl_extract!(
//     ChunkedRectArray,
//     geoarrow::array::RectArray,
//     geoarrow::chunked_array::ChunkedRectArray
// );
impl_extract!(
    ChunkedGeometryCollectionArray,
    geoarrow::array::GeometryCollectionArray<i32>,
    geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>
);
impl_extract!(
    ChunkedWKBArray,
    geoarrow::array::WKBArray<i32>,
    geoarrow::chunked_array::ChunkedWKBArray<i32>
);

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
