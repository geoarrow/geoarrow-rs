use crate::array::*;
use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::ffi::from_python::import_arrow_c_array;
use geoarrow::algorithm::geo::Area;
use geoarrow::array::from_arrow_array;
use pyo3::prelude::*;

#[pyfunction]
pub fn area(ob: &PyAny) -> PyGeoArrowResult<Float64Array> {
    let (array, field) = import_arrow_c_array(ob)?;
    let array = from_arrow_array(&array, &field)?;
    Ok(array.as_ref().unsigned_area()?.into())
}

#[pyfunction]
pub fn signed_area(ob: &PyAny) -> PyGeoArrowResult<Float64Array> {
    let (array, field) = import_arrow_c_array(ob)?;
    let array = from_arrow_array(&array, &field)?;
    Ok(array.as_ref().signed_area()?.into())
}

macro_rules! impl_area {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Unsigned planar area of a geometry.
            pub fn area(&self) -> Float64Array {
                use geoarrow::algorithm::geo::Area;
                Area::unsigned_area(&self.0).into()
            }

            /// Signed planar area of a geometry.
            pub fn signed_area(&self) -> Float64Array {
                use geoarrow::algorithm::geo::Area;
                Area::signed_area(&self.0).into()
            }
        }
    };
}

impl_area!(PointArray);
impl_area!(LineStringArray);
impl_area!(PolygonArray);
impl_area!(MultiPointArray);
impl_area!(MultiLineStringArray);
impl_area!(MultiPolygonArray);
impl_area!(MixedGeometryArray);
impl_area!(GeometryCollectionArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Unsigned planar area of a geometry.
            pub fn area(&self) -> PyGeoArrowResult<ChunkedFloat64Array> {
                use geoarrow::algorithm::geo::Area;
                Ok(Area::unsigned_area(&self.0)?.into())
            }

            /// Signed planar area of a geometry.
            pub fn signed_area(&self) -> PyGeoArrowResult<ChunkedFloat64Array> {
                use geoarrow::algorithm::geo::Area;
                Ok(Area::signed_area(&self.0)?.into())
            }
        }
    };
}

impl_chunked!(ChunkedPointArray);
impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);
impl_chunked!(ChunkedMixedGeometryArray);
impl_chunked!(ChunkedGeometryCollectionArray);
