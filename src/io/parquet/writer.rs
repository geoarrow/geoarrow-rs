use std::io::Write;

use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::error::Result;
use crate::table::GeoTable;

pub fn write_geoparquet<W: Write + Send>(
    table: &mut GeoTable,
    writer: W,
    writer_properties: Option<WriterProperties>,
) -> Result<()> {
    let schema = table.schema();
    let mut writer = ArrowWriter::try_new(writer, schema.clone(), writer_properties)?;

    for batch in table.batches() {
        writer.write(batch)?;
    }

    writer.close()?;

    Ok(())
}
