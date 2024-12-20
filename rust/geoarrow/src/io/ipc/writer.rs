use std::io::Write;

use arrow_ipc::writer::{FileWriter, StreamWriter};

use crate::error::Result;
use crate::io::stream::RecordBatchReader;

/// Write a Table to an Arrow IPC (Feather v2) file
pub fn write_ipc<W: Write, S: Into<RecordBatchReader>>(stream: S, writer: W) -> Result<()> {
    let inner: RecordBatchReader = stream.into();
    let inner = inner.into_inner();

    let schema = inner.schema();
    let mut writer = FileWriter::try_new(writer, &schema)?;
    for batch in inner {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(())
}

/// Write a Table to an Arrow IPC stream
pub fn write_ipc_stream<W: Write, S: Into<RecordBatchReader>>(stream: S, writer: W) -> Result<()> {
    let inner: RecordBatchReader = stream.into();
    let inner = inner.into_inner();

    let schema = inner.schema();
    let mut writer = StreamWriter::try_new(writer, &schema)?;
    for batch in inner {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(())
}
