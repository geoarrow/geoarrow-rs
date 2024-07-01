mod geo_interface;

use crate::error::PyGeoArrowResult;
use crate::ffi::to_python::chunked_geometry_array_to_pyobject;
use crate::interop::util::pytable_to_table;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;

/// A spatially-enabled table.
///
/// This is a table, or `DataFrame`, consisting of named columns with the same length. One of these columns contains a chunked geometry array.
///
/// This is similar to a GeoPandas [`GeoDataFrame`][geopandas.GeoDataFrame].
#[pyclass(module = "geoarrow.rust.core._rust")]
pub struct GeoTable(pub(crate) geoarrow::table::Table);

#[pymethods]
impl GeoTable {
    fn __repr__(&self) -> String {
        self.0.to_string()
    }
}

/// Access the geometry column of this table
///
/// Returns:
///     A chunked geometry array
#[pyfunction]
pub fn geometry_col(py: Python, table: PyTable) -> PyGeoArrowResult<PyObject> {
    let table = pytable_to_table(table)?;
    let chunked_geom_arr = table.geometry_column(None)?;
    chunked_geometry_array_to_pyobject(py, chunked_geom_arr)
}

impl From<geoarrow::table::Table> for GeoTable {
    fn from(value: geoarrow::table::Table) -> Self {
        Self(value)
    }
}

impl From<GeoTable> for geoarrow::table::Table {
    fn from(value: GeoTable) -> Self {
        value.0
    }
}
