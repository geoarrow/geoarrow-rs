use crate::array::*;
use crate::error::PyGeoArrowResult;
use geoarrow::error::GeoArrowError;
use geoarrow::trait_::GeometryArrayAccessor;
use geozero::geojson::GeoJsonWriter;
use geozero::{FeatureProcessor, GeozeroGeometry};
use pyo3::exceptions::PyIOError;
use pyo3::intern;

macro_rules! impl_geo_interface {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Implements the "geo interface protocol".
            ///
            /// See <https://gist.github.com/sgillies/2217756>
            #[getter]
            pub fn __geo_interface__<'a>(&'a self, py: Python<'a>) -> PyGeoArrowResult<&'a PyAny> {
                // Note: We use the lower-level GeoJsonWriter API directly so that we can force
                // each geometry to be its own Feature. This is the format that GeoPandas expects,
                // e.g. in GeoDataFrame.from_features(our_array)
                let mut json_data = Vec::new();
                let mut geojson_writer = GeoJsonWriter::new(&mut json_data);

                geojson_writer
                    .dataset_begin(None)
                    .map_err(GeoArrowError::GeozeroError)?;

                // TODO: what to do with missing values?
                for (idx, geom) in self.0.iter().flatten().enumerate() {
                    geojson_writer
                        .feature_begin(idx as u64)
                        .map_err(GeoArrowError::GeozeroError)?;
                    geojson_writer
                        .properties_begin()
                        .map_err(GeoArrowError::GeozeroError)?;
                    geojson_writer
                        .properties_end()
                        .map_err(GeoArrowError::GeozeroError)?;
                    geojson_writer
                        .geometry_begin()
                        .map_err(GeoArrowError::GeozeroError)?;
                    geom.process_geom(&mut geojson_writer)
                        .map_err(GeoArrowError::GeozeroError)?;
                    geojson_writer
                        .geometry_end()
                        .map_err(GeoArrowError::GeozeroError)?;
                    geojson_writer
                        .feature_end(idx as u64)
                        .map_err(GeoArrowError::GeozeroError)?;
                }

                geojson_writer
                    .dataset_end()
                    .map_err(GeoArrowError::GeozeroError)?;

                let json_string = String::from_utf8(json_data)
                    .map_err(|err| PyIOError::new_err(err.to_string()))?;
                let json_mod = py.import(intern!(py, "json"))?;
                let args = (json_string.into_py(py),);
                Ok(json_mod.call_method1(intern!(py, "loads"), args)?)
            }
        }
    };
}

impl_geo_interface!(PointArray);
impl_geo_interface!(LineStringArray);
impl_geo_interface!(PolygonArray);
impl_geo_interface!(MultiPointArray);
impl_geo_interface!(MultiLineStringArray);
impl_geo_interface!(MultiPolygonArray);
impl_geo_interface!(MixedGeometryArray);
impl_geo_interface!(GeometryCollectionArray);
