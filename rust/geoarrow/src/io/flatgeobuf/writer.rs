use std::io::Write;

use flatgeobuf::{FgbCrs, FgbWriter, FgbWriterOptions};
use geozero::GeozeroDatasource;

use crate::array::metadata::ArrayMetadata;
use crate::datatypes::{Dimension, NativeType};
use crate::error::Result;
use crate::io::crs::CRSTransform;
use crate::io::stream::RecordBatchReader;
use crate::schema::GeoSchemaExt;

/// Options for the FlatGeobuf writer
#[derive(Debug)]
pub struct FlatGeobufWriterOptions {
    /// Write index and sort features accordingly.
    pub write_index: bool,
    /// Detect geometry type when `geometry_type` is Unknown.
    pub detect_type: bool,
    /// Convert single to multi geometries, if `geometry_type` is multi type or Unknown
    pub promote_to_multi: bool,
    // Dataset title
    pub title: Option<String>,
    // Dataset description (intended for free form long text)
    pub description: Option<String>,
    // Dataset metadata (intended to be application specific and
    pub metadata: Option<String>,

    /// A method for transforming CRS to WKT
    pub crs_transform: Option<Box<dyn CRSTransform>>,
}

impl FlatGeobufWriterOptions {
    fn create_fgb_options(&self, geo_data_type: N array_meta: &ArrayMetadata) -> FgbWriterOptions {
        FgbCrs {

        }

        FgbWriterOptions {
            write_index: value.write_index,
            detect_type: (),
            promote_to_multi: (),
            crs: (),
            has_z: (),
            has_m: (),
            has_t: (),
            has_tm: (),
            title: (),
            description: (),
            metadata: (),
        }
    }
}


// TODO: always write CRS saved in Table metadata (you can do this by adding an option)
/// Write a Table to a FlatGeobuf file.
pub fn write_flatgeobuf<W: Write, S: Into<RecordBatchReader>>(
    stream: S,
    writer: W,
    name: &str,
) -> Result<()> {
    write_flatgeobuf_with_options(stream, writer, name, Default::default())
}

/// Write a Table to a FlatGeobuf file with specific writer options.
///
/// Note: this `name` argument is what OGR observes as the layer name of the file.
pub fn write_flatgeobuf_with_options<W: Write, S: Into<RecordBatchReader>>(
    stream: S,
    writer: W,
    name: &str,
    mut options: FgbWriterOptions,
) -> Result<()> {
    let mut stream = stream.into();

    let (geometry_type, has_z) = infer_flatgeobuf_geometry_type(&stream)?;
    options.has_z = has_z;

    let mut fgb = FgbWriter::create_with_options(name, geometry_type, options)?;
    stream.process(&mut fgb)?;
    fgb.write(writer)?;
    Ok(())
}

fn infer_flatgeobuf_geometry_type(
    stream: &RecordBatchReader,
) -> Result<(flatgeobuf::GeometryType, bool)> {
    let schema = stream.schema()?;
    let fields = &schema.fields;
    let geom_col_idxs = schema.as_ref().geometry_columns();
    if geom_col_idxs.len() != 1 {
        panic!("Only one geometry column currently supported in FlatGeobuf writer");
    }

    let geometry_field = &fields[geom_col_idxs[0]];
    let geo_data_type = NativeType::try_from(geometry_field.as_ref())?;

    use NativeType::*;
    let (geometry_type, has_z) = match geo_data_type {
        Point(_, dim) => (
            flatgeobuf::GeometryType::Point,
            matches!(dim, Dimension::XYZ),
        ),
        LineString(_, dim) => (
            flatgeobuf::GeometryType::LineString,
            matches!(dim, Dimension::XYZ),
        ),
        Polygon(_, dim) => (
            flatgeobuf::GeometryType::Polygon,
            matches!(dim, Dimension::XYZ),
        ),
        MultiPoint(_, dim) => (
            flatgeobuf::GeometryType::MultiPoint,
            matches!(dim, Dimension::XYZ),
        ),
        MultiLineString(_, dim) => (
            flatgeobuf::GeometryType::MultiLineString,
            matches!(dim, Dimension::XYZ),
        ),
        MultiPolygon(_, dim) => (
            flatgeobuf::GeometryType::MultiPolygon,
            matches!(dim, Dimension::XYZ),
        ),
        Mixed(_, dim) | Rect(dim) => (
            flatgeobuf::GeometryType::Unknown,
            matches!(dim, Dimension::XYZ),
        ),
        GeometryCollection(_, dim) => (
            flatgeobuf::GeometryType::GeometryCollection,
            matches!(dim, Dimension::XYZ),
        ),
        // TODO: how to know when WKB has 3d geometries?
        // WKB | LargeWKB => (flatgeobuf::GeometryType::Unknown, false),
    };
    Ok((geometry_type, has_z))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::io::flatgeobuf::read_flatgeobuf;
    use crate::test::point;
    use std::io::{BufWriter, Cursor};

    #[test]
    fn test_write() {
        let table = point::table();

        let mut output_buffer = Vec::new();
        let writer = BufWriter::new(&mut output_buffer);
        write_flatgeobuf(&table, writer, "name").unwrap();

        let mut reader = Cursor::new(output_buffer);
        let new_table = read_flatgeobuf(&mut reader, Default::default()).unwrap();

        // TODO: it looks like it's getting read back in backwards row order!
        let batch = &new_table.batches()[0];
        let arr = batch.column(0);
        dbg!(arr);
        dbg!(new_table);
        // dbg!(output_buffer);
    }

    #[test]
    fn test_write_z() {
        let table = point::table_z();

        let mut output_buffer = Vec::new();
        let writer = BufWriter::new(&mut output_buffer);
        write_flatgeobuf(&table, writer, "name").unwrap();

        let mut reader = Cursor::new(output_buffer);
        let new_table = read_flatgeobuf(&mut reader, Default::default()).unwrap();

        // TODO: it looks like it's getting read back in backwards row order!
        let batch = &new_table.batches()[0];
        let arr = batch.column(0);
        dbg!(arr);
        dbg!(new_table);
        // dbg!(output_buffer);
    }
}
