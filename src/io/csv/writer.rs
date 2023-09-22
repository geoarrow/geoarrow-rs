use crate::table::GeoTable;
use geozero::csv::CsvWriter;
use geozero::error::GeozeroError;
use geozero::GeozeroDatasource;
use std::io::Write;

/// Write a GeoTable to CSV
pub fn write_csv<W: Write>(table: &mut GeoTable, writer: W) -> Result<(), GeozeroError> {
    let mut csv_writer = CsvWriter::new(writer);
    table.process(&mut csv_writer)?;
    Ok(())
}
