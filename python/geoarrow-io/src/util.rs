use pyo3_arrow::PyTable;
use pyo3_arrow::export::Arro3Table;

pub(crate) fn to_arro3_table(table: geoarrow::table::Table) -> Arro3Table {
    let (batches, schema) = table.into_inner();
    PyTable::try_new(batches, schema).unwrap().into()
}
