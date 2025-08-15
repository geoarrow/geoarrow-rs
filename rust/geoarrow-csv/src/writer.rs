//! Write GeoArrow data to CSV

use std::io::Write;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::to_wkt_view;
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::GeoArrowResult;

/// A CSV writer that encodes geometries as WKT strings
pub struct CsvWriter<W: Write> {
    inner: arrow_csv::Writer<W>,
}

impl<W: Write> CsvWriter<W> {
    /// Create a new CSV writer
    pub fn new(writer: arrow_csv::Writer<W>) -> Self {
        Self { inner: writer }
    }

    /// Write a record batch to the CSV
    pub fn write(&mut self, batch: &RecordBatch) -> GeoArrowResult<()> {
        let batch = encode_batch(batch)?;
        self.inner.write(&batch)?;
        Ok(())
    }

    /// Return the underlying writer
    pub fn into_inner(self) -> W {
        self.inner.into_inner()
    }
}

/// Write a Table to CSV
pub fn write_csv<W: Write, S: RecordBatchReader>(
    stream: S,
    writer: &mut arrow_csv::Writer<W>,
) -> GeoArrowResult<()> {
    for batch in stream {
        writer.write(&encode_batch(&batch?)?)?;
    }

    Ok(())
}

fn encode_batch(batch: &RecordBatch) -> GeoArrowResult<RecordBatch> {
    let schema = batch.schema();
    let fields = schema.fields();

    let mut new_fields = Vec::with_capacity(fields.len());
    let mut new_columns = Vec::with_capacity(fields.len());

    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if let Ok(_typ) = GeoArrowType::from_extension_field(field) {
            let geo_arr = from_arrow_array(&column, field)?;
            let wkt_view_arr = to_wkt_view(&geo_arr)?;

            new_fields.push(
                wkt_view_arr
                    .data_type()
                    .to_field(field.name(), field.is_nullable())
                    .into(),
            );
            new_columns.push(wkt_view_arr.into_array_ref());
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
