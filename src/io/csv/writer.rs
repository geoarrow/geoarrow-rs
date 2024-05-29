use crate::error::Result;
use crate::io::stream::RecordBatchReader;
use std::io::Write;

/// Writes a [Table](crate::table::Table) in Comma-Separated Value (CSV) format.
pub fn write_csv<W: Write, S: Into<RecordBatchReader>>(_: S, _: W) -> Result<()> {
    todo!()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point;
    use std::io::BufWriter;

    #[test]
    fn test_write() {
        let table = point::table();

        let mut output_buffer = Vec::new();
        let writer = BufWriter::new(&mut output_buffer);
        write_csv(&table, writer).unwrap();
        String::from_utf8(output_buffer).unwrap();
    }
}
