use std::io::Write;

use arrow_ipc::writer::{FileWriter, StreamWriter};

use crate::error::{GeoArrowError, Result};
use crate::io::geozero::RecordBatchReader;

/// Write a Table to an Arrow IPC (Feather v2) file
pub fn write_ipc<W: Write>(table: &mut RecordBatchReader, writer: W) -> Result<()> {
    let inner = table
        .take()
        .ok_or(GeoArrowError::General("Closed stream".to_string()))?;

    let schema = inner.schema();
    let mut writer = FileWriter::try_new(writer, &schema)?;
    for batch in inner {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(())
}

/// Write a Table to an Arrow IPC stream
pub fn write_ipc_stream<W: Write>(table: &mut RecordBatchReader, writer: W) -> Result<()> {
    let inner = table
        .take()
        .ok_or(GeoArrowError::General("Closed stream".to_string()))?;

    let schema = inner.schema();
    let mut writer = StreamWriter::try_new(writer, &schema)?;
    for batch in inner {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(())
}
