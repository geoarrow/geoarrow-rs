use crate::error::PyGeoArrowResult;
use crate::table::GeoTable;
use geoarrow::algorithm::geo::spatial_join;
use pyo3::prelude::*;

#[pyfunction]
pub fn sjoin(left: GeoTable, right: GeoTable) -> PyGeoArrowResult<GeoTable> {
    Ok(GeoTable(spatial_join(&left.0, &right.0)?))
}
