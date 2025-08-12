use std::io::Write;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_json::{ArrayWriter, LineDelimitedWriter, WriterBuilder};
use arrow_schema::ArrowError;

use crate::encoder::GeoArrowEncoderFactory;

pub struct GeoJsonWriter<W: Write> {
    /// Underlying writer to use to write bytes
    writer: ArrayWriter<W>,
}

impl<W: Write> GeoJsonWriter<W> {
    /// Construct a new writer
    pub fn new(mut writer: W) -> std::io::Result<Self> {
        Self::write_header(&mut writer).unwrap();

        let array_writer = WriterBuilder::new()
            .with_encoder_factory(Arc::new(GeoArrowEncoderFactory))
            .build(writer);
        Ok(Self {
            writer: array_writer,
        })
    }

    fn write_header(w: &mut W) -> std::io::Result<()> {
        // Don't include the initial `[` because the ArrayWriter will write the open brace
        let s = br#"{"type":"FeatureCollection","features":"#;
        w.write_all(s)?;
        Ok(())
    }

    /// Serialize batch to GeoJSON output
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        let batch = transform_batch(batch)?;
        self.writer.write(&batch)
    }

    /// Serialize batches to GeoJSON output
    pub fn write_batches(&mut self, batches: &[&RecordBatch]) -> Result<(), ArrowError> {
        for batch in batches {
            self.write(&transform_batch(batch)?)?;
        }
        Ok(())
    }

    /// Finishes the output stream. This function must be called after
    /// all record batches have been produced. (e.g. producing the final `']'` if writing
    /// arrays.
    ///
    /// Consumes self and returns the underlying writer.
    pub fn finish(mut self) -> Result<W, ArrowError> {
        self.writer.finish()?;
        let mut w = self.writer.into_inner();
        // Write the closing brace
        w.write_all(b"}")?;
        Ok(w)
    }
}

pub struct GeoJsonLinesWriter<W: Write> {
    /// Underlying writer to use to write bytes
    writer: LineDelimitedWriter<W>,
}

impl<W: Write> GeoJsonLinesWriter<W> {
    /// Construct a new writer
    pub fn new(writer: W) -> Self {
        let line_writer = WriterBuilder::new()
            .with_encoder_factory(Arc::new(GeoArrowEncoderFactory))
            .build(writer);
        Self {
            writer: line_writer,
        }
    }

    /// Serialize batch to GeoJSON output
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        let batch = transform_batch(batch)?;
        self.writer.write(&batch)
    }

    /// Serialize batches to GeoJSON output
    pub fn write_batches(&mut self, batches: &[&RecordBatch]) -> Result<(), ArrowError> {
        for batch in batches {
            self.write(&transform_batch(batch)?)?;
        }
        Ok(())
    }

    /// Finishes the output stream. This function must be called after
    /// all record batches have been produced.
    ///
    /// Consumes self and returns the underlying writer.
    pub fn finish(mut self) -> Result<W, ArrowError> {
        self.writer.finish()?;
        Ok(self.writer.into_inner())
    }
}

/// Transform the batch to a format that can be written as GeoJSON
///
/// Steps:
/// - Find geometry column(s); error if there's more than one geometry column. Make sure the
///   geometry column is called `"geometry"`.
/// - For all non-geometry columns, wrap into a struct called "properties"
/// - Keep `id` column separate, if designated, as it's at the top-level in GeoJSON
/// - The custom encoders handle geometry types.
fn transform_batch(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    Ok(batch.clone())
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::vec;

    use arrow_schema::Schema;
    use geoarrow_array::test::point;
    use geoarrow_array::{GeoArrowArray, IntoArrow};
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;

    #[test]
    fn test_geometry_encoder_factory() {
        let point_arr = test::point::array(CoordType::Interleaved, Dimension::XY);

        // Slice to avoid empty points
        let point_arr = point_arr.slice(0, 2);

        let field = point_arr.extension_type().to_field("geometry", true);
        let array = point_arr.to_array_ref();

        let schema = Schema::new(vec![Arc::new(field)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap();

        let mut buffer = Vec::new();
        let mut geo_writer = GeoJsonWriter::new(&mut buffer).unwrap();
        geo_writer.write(&batch).unwrap();
        geo_writer.finish().unwrap();

        let s = String::from_utf8(buffer).unwrap();
        println!("{}", s);

        let _expected = r#"
        {
            "type":"FeatureCollection",
            "features": [
                {"geometry":{"type":"Point","coordinates":[30,10]}},
                {"geometry":{"type":"Point","coordinates":[40,20]}}
            ]
        }
        "#;

        // {
        //   "type": "FeatureCollection",
        //   "features": [
        //     { "geometry": { "type": "Point", "coordinates": [0, 1] } },
        //     { "geometry": { "type": "Point", "coordinates": [1, 2] } },
        //     {},
        //     { "geometry": { "type": "Point", "coordinates": [2, 3] } }
        //   ]
        // }
    }
}
