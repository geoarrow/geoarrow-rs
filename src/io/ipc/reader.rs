use std::io::{Read, Seek};

use arrow_array::RecordBatchReader;
use arrow_ipc::reader::{FileReader, StreamReader};

use crate::error::Result;

/// Create a [RecordBatchReader] from an Arrow IPC (Feather v2) file.
pub fn read_ipc<R: Read + Seek>(reader: R) -> Result<impl RecordBatchReader> {
    Ok(FileReader::try_new(reader, None)?)
}

/// Create a [RecordBatchReader] from an Arrow IPC record batch stream.
pub fn read_ipc_stream<R: Read>(reader: R) -> Result<impl RecordBatchReader> {
    Ok(StreamReader::try_new(reader, None)?)
}
