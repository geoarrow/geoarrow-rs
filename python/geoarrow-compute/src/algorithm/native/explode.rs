use geoarrow::algorithm::native::ExplodeTable;
use pyo3::prelude::*;
use pyo3_arrow::export::Arro3Table;
use pyo3_arrow::PyTable;
use pyo3_geoarrow::PyGeoArrowResult;

use crate::util::{pytable_to_table, table_to_pytable};

#[pyfunction]
pub fn explode(py: Python, input: PyTable) -> PyGeoArrowResult<Arro3Table> {
    let table = pytable_to_table(input)?;
    let exploded_table = py.allow_threads(|| table.explode(None))?;
    Ok(table_to_pytable(exploded_table).into())
}
