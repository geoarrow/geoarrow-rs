use crate::array::NativeArrayDyn;
use crate::error::Result;
use crate::io::stream::RecordBatchReader;
use crate::io::wkt::ToWKT;
use crate::{ArrayBase, NativeArray};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use std::io::Write;
use std::sync::Arc;

// TODO: add CSV writer options

/// Write a Table to CSV
pub fn write_csv<W: Write, S: Into<RecordBatchReader>>(stream: S, writer: W) -> Result<()> {
    let mut stream: RecordBatchReader = stream.into();
    let reader = stream.take().unwrap();

    let mut csv_writer = arrow_csv::Writer::new(writer);
    for batch in reader {
        csv_writer.write(&encode_batch(batch?)?)?;
    }

    Ok(())
}

fn encode_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let fields = schema.fields();

    let mut new_fields = Vec::with_capacity(fields.len());
    let mut new_columns = Vec::with_capacity(fields.len());

    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if let Ok(arr) = NativeArrayDyn::from_arrow_array(&column, field) {
            let wkt_arr = arr.as_ref().to_wkt::<i32>()?;
            new_fields.push(wkt_arr.extension_field());
            new_columns.push(wkt_arr.into_array_ref());
        } else {
            new_fields.push(field.clone());
            new_columns.push(column.clone());
        }
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(new_fields).with_metadata(schema.metadata().clone())),
        new_columns,
    )?)
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
        let output_string = String::from_utf8(output_buffer).unwrap();
        println!("{}", output_string);
    }
}
