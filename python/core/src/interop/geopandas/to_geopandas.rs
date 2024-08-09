use crate::error::PyGeoArrowResult;
use crate::interop::util::import_geopandas;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

/// Convert a GeoArrow Table to a [GeoPandas GeoDataFrame][geopandas.GeoDataFrame].
///
/// ### Notes:
///
/// - This is an alias to [GeoDataFrame.from_arrow][geopandas.GeoDataFrame.from_arrow].
///
/// Args:
///   input: A GeoArrow Table.
///
/// Returns:
///     the converted GeoDataFrame
#[pyfunction]
pub fn to_geopandas(py: Python, input: PyObject) -> PyGeoArrowResult<PyObject> {
    let geopandas_mod = import_geopandas(py)?;
    let geodataframe_class = geopandas_mod.getattr(intern!(py, "GeoDataFrame"))?;
    let gdf = geodataframe_class.call_method1(
        intern!(py, "from_arrow"),
        PyTuple::new_bound(py, vec![input]),
    )?;
    Ok(gdf.into())
}
