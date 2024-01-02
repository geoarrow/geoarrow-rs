use crate::error::PyGeoArrowResult;
use crate::ffi::to_python::chunked_geometry_array_to_pyobject;
use pyo3::prelude::*;

/// A spatially-enabled table.
///
/// This is a table, or `DataFrame`, consisting of named columns with the same length. One of these columns contains a chunked geometry array.
///
/// This is similar to a GeoPandas [`GeoDataFrame`][geopandas.GeoDataFrame].
#[pyclass(module = "geoarrow.rust.core.rust")]
pub struct GeoTable(pub(crate) geoarrow::table::GeoTable);

#[pymethods]
impl GeoTable {
    /// Access the geometry column of this table
    ///
    /// Returns:
    ///     A chunked geometry array
    pub fn geometry(&self) -> PyGeoArrowResult<PyObject> {
        let chunked_geom_arr = self.0.geometry()?;
        Python::with_gil(|py| chunked_geometry_array_to_pyobject(py, chunked_geom_arr))
    }
}
