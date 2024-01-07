use crate::error::Result;
use crate::table::GeoTable;
use geozero::csv::CsvWriter;
use geozero::GeozeroDatasource;
use std::io::Write;

/// Write a GeoTable to CSV
pub fn write_csv<W: Write>(table: &mut GeoTable, writer: W) -> Result<()> {
    let mut csv_writer = CsvWriter::new(writer);
    table.process(&mut csv_writer)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point;
    use std::io::BufWriter;

    #[test]
    fn test_write() {
        let mut table = point::table();

        let mut output_buffer = Vec::new();
        let writer = BufWriter::new(&mut output_buffer);
        write_csv(&mut table, writer).unwrap();
        let output_string = String::from_utf8(output_buffer).unwrap();
        println!("{}", output_string);
    }
}
