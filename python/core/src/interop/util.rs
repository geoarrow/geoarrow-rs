use crate::error::PyGeoArrowResult;
use geoarrow::error::GeoArrowError;
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3_arrow::PyTable;

/// Import geopandas and assert version 1.0.0 or higher
pub(crate) fn import_geopandas(py: Python) -> PyGeoArrowResult<Bound<PyModule>> {
    let geopandas_mod = py.import_bound(intern!(py, "geopandas"))?;
    let geopandas_version_string = geopandas_mod
        .getattr(intern!(py, "__version__"))?
        .extract::<String>()?;
    let geopandas_major_version = geopandas_version_string
        .split('.')
        .next()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    if geopandas_major_version < 1 {
        Err(PyValueError::new_err("geopandas version 1.0 or higher required").into())
    } else {
        Ok(geopandas_mod)
    }
}

pub(crate) fn table_to_pytable(table: geoarrow::table::Table) -> PyTable {
    let (schema, batches) = table.into_inner();
    PyTable::new(batches, schema)
}

pub(crate) fn pytable_to_table(table: PyTable) -> Result<geoarrow::table::Table, GeoArrowError> {
    let (batches, schema) = table.into_inner();
    geoarrow::table::Table::try_new(schema, batches)
}
