use std::io::Write;

use arrow_ipc::writer::{FileWriter, StreamWriter};

use crate::error::Result;
use crate::table::GeoTable;

/// Write a GeoTable to an Arrow IPC (Feather v2) file
pub fn write_ipc<W: Write>(table: &mut GeoTable, writer: W) -> Result<()> {
    let mut writer = FileWriter::try_new(writer, table.schema())?;
    table
        .batches()
        .iter()
        .try_for_each(|batch| writer.write(batch))?;
    writer.finish()?;
    Ok(())
}

/// Write a GeoTable to an Arrow IPC stream
pub fn write_ipc_stream<W: Write>(table: &mut GeoTable, writer: W) -> Result<()> {
    let mut writer = StreamWriter::try_new(writer, table.schema())?;
    table
        .batches()
        .iter()
        .try_for_each(|batch| writer.write(batch))?;
    writer.finish()?;
    Ok(())
}
