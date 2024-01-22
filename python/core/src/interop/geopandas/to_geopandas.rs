use crate::chunked_array::*;
use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use geoarrow::array::AsChunkedGeometryArray;
use geoarrow::datatypes::GeoDataType;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyDict;

pub(crate) fn import_pyarrow(py: Python) -> PyGeoArrowResult<&PyModule> {
    let pyarrow_mod = py.import(intern!(py, "pyarrow"))?;
    let pyarrow_version_string = pyarrow_mod
        .getattr(intern!(py, "__version__"))?
        .extract::<String>()?;
    let pyarrow_major_version = pyarrow_version_string
        .split('.')
        .next()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    if pyarrow_major_version < 14 {
        Err(PyValueError::new_err("pyarrow version 14.0 or higher required").into())
    } else {
        Ok(pyarrow_mod)
    }
}

#[pymethods]
impl GeoTable {
    /// Convert this GeoArrow Table to a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].
    ///
    /// ### Notes:
    ///
    /// - This requires [`pyarrow`][pyarrow] version 14 or later.
    ///
    /// Args:
    ///
    ///   input: A [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].
    ///
    /// Returns:
    ///
    ///     the converted GeoDataFrame
    fn to_geopandas(&self, py: Python) -> PyGeoArrowResult<PyObject> {
        // Imports and validation
        let pyarrow_mod = import_pyarrow(py)?;
        let geopandas_mod = py.import(intern!(py, "geopandas"))?;
        let pandas_mod = py.import(intern!(py, "pandas"))?;
        let geodataframe_class = geopandas_mod.getattr(intern!(py, "GeoDataFrame"))?;

        // Hack: create a new table because I can't figure out how to pass `self`
        let cloned_table = GeoTable(self.0.clone());
        let pyarrow_table = pyarrow_mod.call_method1(intern!(py, "table"), (cloned_table,))?;

        let geometry_column_index = self.0.geometry_column_index();
        let pyarrow_table =
            pyarrow_table.call_method1(intern!(py, "remove_column"), (geometry_column_index,))?;

        let kwargs = PyDict::new(py);
        kwargs.set_item(
            "types_mapper",
            pandas_mod.getattr(intern!(py, "ArrowDtype"))?,
        )?;
        let pandas_df = pyarrow_table.call_method(intern!(py, "to_pandas"), (), Some(kwargs))?;

        let geometry = self.0.geometry()?;
        let shapely_geometry = match geometry.data_type() {
            GeoDataType::Point(_) => ChunkedPointArray(geometry.as_ref().as_point().clone())
                .to_shapely(py)?
                .to_object(py),
            GeoDataType::LineString(_) => {
                ChunkedLineStringArray(geometry.as_ref().as_line_string().clone())
                    .to_shapely(py)?
                    .to_object(py)
            }
            GeoDataType::Polygon(_) => ChunkedPolygonArray(geometry.as_ref().as_polygon().clone())
                .to_shapely(py)?
                .to_object(py),
            GeoDataType::MultiPoint(_) => {
                ChunkedMultiPointArray(geometry.as_ref().as_multi_point().clone())
                    .to_shapely(py)?
                    .to_object(py)
            }
            GeoDataType::MultiLineString(_) => {
                ChunkedMultiLineStringArray(geometry.as_ref().as_multi_line_string().clone())
                    .to_shapely(py)?
                    .to_object(py)
            }
            GeoDataType::MultiPolygon(_) => {
                ChunkedMultiPolygonArray(geometry.as_ref().as_multi_polygon().clone())
                    .to_shapely(py)?
                    .to_object(py)
            }
            GeoDataType::Mixed(_) => {
                ChunkedMixedGeometryArray(geometry.as_ref().as_mixed().clone())
                    .to_shapely(py)?
                    .to_object(py)
            }
            GeoDataType::GeometryCollection(_) => {
                ChunkedGeometryCollectionArray(geometry.as_ref().as_geometry_collection().clone())
                    .to_shapely(py)?
                    .to_object(py)
            }
            GeoDataType::WKB => ChunkedWKBArray(geometry.as_ref().as_wkb().clone())
                .to_shapely(py)?
                .to_object(py),
            t => {
                return Err(PyValueError::new_err(format!("unexpected type {:?}", t)).into());
            }
        };

        let args = (pandas_df,);
        let kwargs = PyDict::new(py);
        kwargs.set_item("geometry", shapely_geometry)?;
        Ok(geodataframe_class.call(args, Some(kwargs))?.to_object(py))
    }
}
