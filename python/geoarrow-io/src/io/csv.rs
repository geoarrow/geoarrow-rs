use std::io::{Seek, SeekFrom};

use crate::error::PyGeoArrowResult;
use crate::io::input::sync::{FileReader, FileWriter};
use arrow::array::RecordBatchReader;
use geoarrow::io::csv;
use geoarrow::io::csv::CSVReaderOptions;
use pyo3::prelude::*;
use pyo3_arrow::export::Arro3Table;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::PyTable;
use pyo3_geoarrow::PyCoordType;

#[pyfunction]
#[pyo3(
    signature = (
        file,
        *,
        geometry_name=None,
        batch_size=65536,
        coord_type = PyCoordType::Interleaved,
        has_header=true,
        max_records=None,
        delimiter=None,
        escape=None,
        quote=None,
        terminator=None,
        comment=None,
    ),
    text_signature = "(file, *, geometry_name=None, batch_size=65536, coord_type='interleaved', has_header=True,max_records=None, delimiter=None, escape=None, quote=None, terminator=None, comment=None)"
)]
#[allow(clippy::too_many_arguments)]
pub fn read_csv(
    mut file: FileReader,
    geometry_name: Option<String>,
    batch_size: usize,
    coord_type: PyCoordType,
    has_header: bool,
    max_records: Option<usize>,
    delimiter: Option<char>,
    escape: Option<char>,
    quote: Option<char>,
    terminator: Option<char>,
    comment: Option<char>,
) -> PyGeoArrowResult<Arro3Table> {
    let mut options = CSVReaderOptions {
        coord_type: coord_type.into(),
        batch_size,
        geometry_column_name: geometry_name,
        has_header: Some(has_header),
        max_records,
        delimiter,
        escape,
        quote,
        terminator,
        comment,
    };

    let pos = file.stream_position()?;
    let (schema, _rows_read, geometry_col_name) = csv::infer_csv_schema(&mut file, &options)?;

    // So we don't have to search for the geometry column a second time if not provided
    options.geometry_column_name = Some(geometry_col_name);

    file.seek(SeekFrom::Start(pos))?;

    let record_batch_reader = csv::read_csv(file, schema.into(), options)?;
    let schema = record_batch_reader.schema();
    let batches = record_batch_reader.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(PyTable::try_new(batches, schema)?.into())
}

#[pyfunction]
#[pyo3(signature = (table, file))]
pub fn write_csv(table: AnyRecordBatch, file: FileWriter) -> PyGeoArrowResult<()> {
    csv::write_csv(table.into_reader()?, file)?;
    Ok(())
}
