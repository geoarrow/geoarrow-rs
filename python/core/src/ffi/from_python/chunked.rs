use crate::chunked_array::*;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};
use pyo3_arrow::input::AnyArray;

macro_rules! impl_extract {
    ($py_chunked_array:ty, $rs_array:ty, $rs_chunked_array:ty) => {
        impl<'a> FromPyObject<'a> for $py_chunked_array {
            fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
                let mut geo_chunks = vec![];
                for array in ob.extract::<AnyArray>()?.into_reader()? {
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
    geoarrow::array::PointArray<2>,
    geoarrow::chunked_array::ChunkedPointArray<2>
);
impl_extract!(
    ChunkedLineStringArray,
    geoarrow::array::LineStringArray<i32, 2>,
    geoarrow::chunked_array::ChunkedLineStringArray<i32, 2>
);
impl_extract!(
    ChunkedPolygonArray,
    geoarrow::array::PolygonArray<i32, 2>,
    geoarrow::chunked_array::ChunkedPolygonArray<i32, 2>
);
impl_extract!(
    ChunkedMultiPointArray,
    geoarrow::array::MultiPointArray<i32, 2>,
    geoarrow::chunked_array::ChunkedMultiPointArray<i32, 2>
);
impl_extract!(
    ChunkedMultiLineStringArray,
    geoarrow::array::MultiLineStringArray<i32, 2>,
    geoarrow::chunked_array::ChunkedMultiLineStringArray<i32, 2>
);
impl_extract!(
    ChunkedMultiPolygonArray,
    geoarrow::array::MultiPolygonArray<i32, 2>,
    geoarrow::chunked_array::ChunkedMultiPolygonArray<i32, 2>
);
impl_extract!(
    ChunkedMixedGeometryArray,
    geoarrow::array::MixedGeometryArray<i32, 2>,
    geoarrow::chunked_array::ChunkedMixedGeometryArray<i32, 2>
);
impl_extract!(
    ChunkedRectArray,
    geoarrow::array::RectArray<2>,
    geoarrow::chunked_array::ChunkedRectArray<2>
);
impl_extract!(
    ChunkedGeometryCollectionArray,
    geoarrow::array::GeometryCollectionArray<i32, 2>,
    geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32, 2>
);
impl_extract!(
    ChunkedWKBArray,
    geoarrow::array::WKBArray<i32>,
    geoarrow::chunked_array::ChunkedWKBArray<i32>
);
