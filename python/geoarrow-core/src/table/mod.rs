mod geo_interface;

use pyo3_geoarrow::PyGeoArrowResult;
use crate::ffi::to_python::chunked_geometry_array_to_pyobject;
use crate::interop::util::pytable_to_table;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;

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
