use geoarrow::error::GeoArrowError;
use pyo3_arrow::PyTable;

pub(crate) fn table_to_pytable(table: geoarrow::table::Table) -> PyTable {
    let (batches, schema) = table.into_inner();
    PyTable::try_new(batches, schema).unwrap()
}

pub(crate) fn pytable_to_table(table: PyTable) -> Result<geoarrow::table::Table, GeoArrowError> {
    let (batches, schema) = table.into_inner();
    geoarrow::table::Table::try_new(batches, schema)
}
