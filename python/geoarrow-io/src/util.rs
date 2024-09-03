use pyo3_arrow::PyTable;

pub(crate) fn table_to_pytable(table: geoarrow::table::Table) -> PyTable {
    let (batches, schema) = table.into_inner();
    PyTable::try_new(batches, schema).unwrap()
}
