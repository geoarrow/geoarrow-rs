use crate::error::PyGeoArrowResult;
use crate::interop::util::{pytable_to_table, table_to_pytable};
use geoarrow::algorithm::native::ExplodeTable;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;

/// Explode a table.
///
/// This is intended to be equivalent to the [`explode`][geopandas.GeoDataFrame.explode] function
/// in GeoPandas.
///
/// Args:
///     input: input table
///
/// Returns:
///     A new table with multi-part geometries exploded to separate rows.
#[pyfunction]
pub fn explode(py: Python, input: PyTable) -> PyGeoArrowResult<PyObject> {
    let table = pytable_to_table(input)?;
    let exploded_table = py.allow_threads(|| table.explode(None))?;
    Ok(table_to_pytable(exploded_table).to_arro3(py)?)
}
