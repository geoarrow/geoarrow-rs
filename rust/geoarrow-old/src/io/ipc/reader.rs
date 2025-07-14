use std::io::{Read, Seek};

use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_schema::ArrowError;

use crate::error::Result;
use crate::table::Table;

/// Read into a Table from Arrow IPC (Feather v2) file.
pub fn read_ipc<R: Read + Seek>(reader: R) -> Result<Table> {
    let reader = FileReader::try_new(reader, None)?;
    let schema = reader.schema();
    let batches = reader.collect::<std::result::Result<Vec<_>, ArrowError>>()?;
    Table::try_new(batches, schema)
}

/// Read into a Table from Arrow IPC record batch stream.
pub fn read_ipc_stream<R: Read>(reader: R) -> Result<Table> {
    let reader = StreamReader::try_new(reader, None)?;
    let schema = reader.schema();
    let batches = reader.collect::<std::result::Result<Vec<_>, ArrowError>>()?;
    Table::try_new(batches, schema)
}
