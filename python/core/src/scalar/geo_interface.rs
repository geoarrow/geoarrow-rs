use crate::error::PyGeoArrowResult;
use crate::scalar::*;
use geoarrow::error::GeoArrowError;
use geozero::ToJson;
use pyo3::intern;

macro_rules! impl_geo_interface {
    ($struct_name:ident, $geoarrow_scalar:ty) => {
        #[pymethods]
        impl $struct_name {
            /// Implements the "geo interface protocol".
            ///
            /// See <https://gist.github.com/sgillies/2217756>
            #[getter]
            pub fn __geo_interface__<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<&'a PyAny> {
                let scalar = <$geoarrow_scalar>::from(&self.0);
                let json_string = scalar.to_json().map_err(GeoArrowError::GeozeroError)?;
                let json_mod = py.import(intern!(py, "json"))?;
                let args = (json_string.into_py(py),);

                Ok(json_mod.call_method1(intern!(py, "loads"), args)?)
            }
        }
    };
}

impl_geo_interface!(Point, geoarrow::scalar::Point);
impl_geo_interface!(LineString, geoarrow::scalar::LineString<i32>);
impl_geo_interface!(Polygon, geoarrow::scalar::Polygon<i32>);
impl_geo_interface!(MultiPoint, geoarrow::scalar::MultiPoint<i32>);
impl_geo_interface!(MultiLineString, geoarrow::scalar::MultiLineString<i32>);
impl_geo_interface!(MultiPolygon, geoarrow::scalar::MultiPolygon<i32>);
impl_geo_interface!(Geometry, geoarrow::scalar::Geometry<i32>);
impl_geo_interface!(
    GeometryCollection,
    geoarrow::scalar::GeometryCollection<i32>
);
