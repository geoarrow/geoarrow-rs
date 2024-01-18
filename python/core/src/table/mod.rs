mod geo_interface;

use crate::error::PyGeoArrowResult;
use crate::ffi::to_python::chunked_geometry_array_to_pyobject;
use pyo3::prelude::*;

/// A spatially-enabled table.
///
/// This is a table, or `DataFrame`, consisting of named columns with the same length. One of these columns contains a chunked geometry array.
///
/// This is similar to a GeoPandas [`GeoDataFrame`][geopandas.GeoDataFrame].
#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct GeoTable(pub(crate) geoarrow::table::GeoTable);

#[pymethods]
impl GeoTable {
    /// Access the geometry column of this table
    ///
    /// Returns:
    ///     A chunked geometry array
    #[getter]
    pub fn geometry(&self) -> PyGeoArrowResult<PyObject> {
        let chunked_geom_arr = self.0.geometry()?;
        Python::with_gil(|py| chunked_geometry_array_to_pyobject(py, chunked_geom_arr))
    }

    /// Number of columns in this table.
    #[getter]
    fn num_columns(&self) -> usize {
        self.0.num_columns()
    }
}

impl From<geoarrow::table::GeoTable> for GeoTable {
    fn from(value: geoarrow::table::GeoTable) -> Self {
        Self(value)
    }
}

impl From<GeoTable> for geoarrow::table::GeoTable {
    fn from(value: GeoTable) -> Self {
        value.0
    }
}
