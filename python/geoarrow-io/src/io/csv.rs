use crate::error::PyGeoArrowResult;
use crate::io::input::sync::{FileReader, FileWriter};
use geoarrow::algorithm::native::DowncastTable;
use geoarrow::io::csv;
use geoarrow::io::csv::{CSVReader, CSVReaderOptions};
use geoarrow::table::Table;
use pyo3::prelude::*;
use pyo3_arrow::export::{Arro3RecordBatchReader, Arro3Table};
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::{PyRecordBatchReader, PyTable};
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
        downcast_geometry=true,
    ),
    text_signature = "(file, *, geometry_name=None, batch_size=65536, coord_type='interleaved', has_header=True,max_records=None, delimiter=None, escape=None, quote=None, terminator=None, comment=None, downcast_geometry=True)"
)]
#[allow(clippy::too_many_arguments)]
pub fn read_csv(
    py: Python,
    file: FileReader,
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
    downcast_geometry: bool,
) -> PyGeoArrowResult<PyObject> {
    let options = CSVReaderOptions {
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
    let reader = CSVReader::try_new(file, options)?;

    if downcast_geometry {
        // Load the file into a table and then downcast
        let batch_reader = geoarrow::io::RecordBatchReader::new(Box::new(reader));
        let table = Table::try_from(batch_reader)?;
        let table = table.downcast()?;
        let (batches, schema) = table.into_inner();
        Ok(Arro3Table::from(PyTable::try_new(batches, schema)?)
            .into_pyobject(py)?
            .unbind())
    } else {
        let batch_reader = PyRecordBatchReader::new(Box::new(reader));
        let batch_reader = Arro3RecordBatchReader::from(batch_reader);
        Ok(batch_reader.into_pyobject(py)?.unbind())
    }
}

#[pyfunction]
#[pyo3(signature = (table, file))]
pub fn write_csv(table: AnyRecordBatch, file: FileWriter) -> PyGeoArrowResult<()> {
    csv::write_csv(table.into_reader()?, file)?;
    Ok(())
}
