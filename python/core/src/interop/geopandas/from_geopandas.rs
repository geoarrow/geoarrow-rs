use crate::error::PyGeoArrowResult;
use crate::interop::util::{import_geopandas, pytable_to_table, table_to_pytable};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::PyAny;
use pyo3_arrow::PyTable;

/// Create a GeoArrow Table from a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].
///
/// ### Notes:
///
/// - Currently this will always generate a non-chunked GeoArrow array. This is partly because
///   [pyarrow.Table.from_pandas][pyarrow.Table.from_pandas] always creates a single batch.
///
/// Args:
///     input: A [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].
///
/// Returns:
///     A GeoArrow Table
#[pyfunction]
pub fn from_geopandas(py: Python, input: &Bound<PyAny>) -> PyGeoArrowResult<PyObject> {
    let geopandas_mod = import_geopandas(py)?;
    let geodataframe_class = geopandas_mod.getattr(intern!(py, "GeoDataFrame"))?;
    if !input.is_instance(&geodataframe_class)? {
        return Err(PyValueError::new_err("Expected GeoDataFrame input.").into());
    }

    // TODO: use arrow-native encoding for export?
    let table = input
        .call_method0(intern!(py, "to_arrow"))?
        .extract::<PyTable>()?;
    let mut table = pytable_to_table(table)?;
    table.parse_geometry_to_native(table.default_geometry_column_idx()?, None)?;
    Ok(table_to_pytable(table).to_arro3(py)?)
}
