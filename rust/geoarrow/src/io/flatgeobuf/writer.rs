use core::panic;
use std::io::Write;

use arrow_schema::Schema;
use flatgeobuf::{FgbCrs, FgbWriter, FgbWriterOptions};
use geoarrow_schema::Dimension;
use geoarrow_schema::Metadata;
use geozero::GeozeroDatasource;

use crate::datatypes::NativeType;
use crate::error::Result;
use crate::io::crs::{CRSTransform, DefaultCRSTransform};
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
    /// Dataset title
    pub title: Option<String>,
    /// Dataset description (intended for free form long text)
    pub description: Option<String>,
    /// Dataset metadata (intended to be application specific and
    pub metadata: Option<String>,
    /// A method for transforming CRS to WKT
    ///
    /// This is implemented as an external trait so that external libraries can inject the method
    /// for CRS conversions. For example, the Python API uses the `pyproj` Python library to
    /// perform the conversion rather than linking into PROJ from Rust.
    pub crs_transform: Option<Box<dyn CRSTransform>>,
}

impl Default for FlatGeobufWriterOptions {
    fn default() -> Self {
        Self {
            write_index: true,
            detect_type: true,
            promote_to_multi: true,
            crs_transform: Some(Box::new(DefaultCRSTransform::default())),
            title: None,
            description: None,
            metadata: None,
        }
    }
}

impl FlatGeobufWriterOptions {
    /// Create a WKT CRS from whatever CRS exists in the [Metadata].
    ///
    /// This uses the [CRSTransform] supplied in the [FlatGeobufWriterOptions].
    ///
    /// If no CRS exists in the Metadata, None will be returned here.
    fn create_wkt_crs(&self, array_meta: &Metadata) -> Result<Option<String>> {
        if let Some(crs_transform) = &self.crs_transform {
            crs_transform.extract_wkt(array_meta.crs())
        } else {
            DefaultCRSTransform::default().extract_wkt(array_meta.crs())
        }
    }

    /// Create [FgbWriterOptions]
    fn create_fgb_options<'a>(
        &'a self,
        geo_data_type: NativeType,
        wkt_crs: Option<&'a str>,
    ) -> FgbWriterOptions<'a> {
        let (has_z, has_m) = match geo_data_type.dimension() {
            Some(Dimension::XY) => (false, false),
            Some(Dimension::XYZ) => (true, false),
            // TODO: not sure how to handle geometry arrays
            None => (false, false),
            _ => panic!("XYM and XYZM not supported"),
        };
        let crs = FgbCrs {
            wkt: wkt_crs,
            ..Default::default()
        };

        FgbWriterOptions {
            write_index: self.write_index,
            detect_type: self.detect_type,
            promote_to_multi: self.promote_to_multi,
            crs,
            has_z,
            has_m,
            has_t: false,
            has_tm: false,
            title: self.title.as_deref(),
            description: self.description.as_deref(),
            metadata: self.metadata.as_deref(),
        }
    }
}

/// Write an iterator of GeoArrow RecordBatches to a FlatGeobuf file.
///
/// `name` is the string passed to [`FgbWriter::create`] and is what OGR observes as the layer name
/// of the file.
pub fn write_flatgeobuf<W: Write, S: Into<RecordBatchReader>>(
    stream: S,
    writer: W,
    name: &str,
) -> Result<()> {
    write_flatgeobuf_with_options(stream, writer, name, Default::default())
}

/// Write a Table to a FlatGeobuf file with specific writer options.
///
/// `name` is the string passed to [`FgbWriter::create`] and is what OGR observes as the layer name
/// of the file.
pub fn write_flatgeobuf_with_options<W: Write, S: Into<RecordBatchReader>>(
    stream: S,
    writer: W,
    name: &str,
    options: FlatGeobufWriterOptions,
) -> Result<()> {
    let mut stream: RecordBatchReader = stream.into();

    let schema = stream.schema();
    let fields = &schema.fields;
    let geom_col_idxs = schema.as_ref().geometry_columns();
    if geom_col_idxs.len() != 1 {
        panic!("Only one geometry column currently supported in FlatGeobuf writer");
    }

    let geometry_field = &fields[geom_col_idxs[0]];
    let geo_data_type = NativeType::try_from(geometry_field.as_ref())?;
    let array_meta = Metadata::try_from(geometry_field.as_ref())?;

    let wkt_crs_str = options.create_wkt_crs(&array_meta)?;
    let fgb_options = options.create_fgb_options(geo_data_type, wkt_crs_str.as_deref());

    let geometry_type = infer_flatgeobuf_geometry_type(stream.schema().as_ref())?;

    let mut fgb = FgbWriter::create_with_options(name, geometry_type, fgb_options)?;
    stream.process(&mut fgb)?;
    fgb.write(writer)?;
    Ok(())
}

fn infer_flatgeobuf_geometry_type(schema: &Schema) -> Result<flatgeobuf::GeometryType> {
    let fields = &schema.fields;
    let geom_col_idxs = schema.geometry_columns();
    if geom_col_idxs.len() != 1 {
        panic!("Only one geometry column currently supported in FlatGeobuf writer");
    }

    let geometry_field = &fields[geom_col_idxs[0]];
    let geo_data_type = NativeType::try_from(geometry_field.as_ref())?;

    use NativeType::*;
    let geometry_type = match geo_data_type {
        Point(_) => flatgeobuf::GeometryType::Point,
        LineString(_) => flatgeobuf::GeometryType::LineString,
        Polygon(_) => flatgeobuf::GeometryType::Polygon,
        MultiPoint(_) => flatgeobuf::GeometryType::MultiPoint,
        MultiLineString(_) => flatgeobuf::GeometryType::MultiLineString,
        MultiPolygon(_) => flatgeobuf::GeometryType::MultiPolygon,
        Rect(_) | Geometry(_) => flatgeobuf::GeometryType::Unknown,
        GeometryCollection(_) => flatgeobuf::GeometryType::GeometryCollection,
    };
    Ok(geometry_type)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::io::flatgeobuf::FlatGeobufReaderBuilder;
    use crate::table::Table;
    use crate::test::point;
    use std::io::{BufWriter, Cursor};

    #[test]
    fn test_write() {
        let table = point::table();

        let mut output_buffer = Vec::new();
        let writer = BufWriter::new(&mut output_buffer);
        write_flatgeobuf(&table, writer, "name").unwrap();

        let reader = Cursor::new(output_buffer);
        let reader_builder = FlatGeobufReaderBuilder::open(reader).unwrap();
        let record_batch_reader = reader_builder.read(Default::default()).unwrap();
        let new_table = Table::try_from(
            Box::new(record_batch_reader) as Box<dyn arrow_array::RecordBatchReader>
        )
        .unwrap();

        // Note: backwards row order is due to the reordering during the spatial index
        let batch = &new_table.batches()[0];
        let arr = batch.column(0);
        dbg!(arr);
        dbg!(new_table);
        // dbg!(output_buffer);
    }

    #[test]
    fn test_write_no_index() {
        let table = point::table();

        let mut output_buffer = Vec::new();
        let writer = BufWriter::new(&mut output_buffer);
        let options = FlatGeobufWriterOptions {
            write_index: false,
            ..Default::default()
        };
        write_flatgeobuf_with_options(&table, writer, "name", options).unwrap();

        let reader = Cursor::new(output_buffer);
        let reader_builder = FlatGeobufReaderBuilder::open(reader).unwrap();
        let record_batch_reader = reader_builder.read(Default::default()).unwrap();
        let new_table = Table::try_from(
            Box::new(record_batch_reader) as Box<dyn arrow_array::RecordBatchReader>
        )
        .unwrap();
        assert_eq!(table, new_table);
    }

    #[test]
    fn test_write_z() {
        let table = point::table_z();

        let mut output_buffer = Vec::new();
        let writer = BufWriter::new(&mut output_buffer);
        write_flatgeobuf(&table, writer, "name").unwrap();

        let reader = Cursor::new(output_buffer);
        let reader_builder = FlatGeobufReaderBuilder::open(reader).unwrap();
        let record_batch_reader = reader_builder.read(Default::default()).unwrap();
        let new_table = Table::try_from(
            Box::new(record_batch_reader) as Box<dyn arrow_array::RecordBatchReader>
        )
        .unwrap();

        // Note: backwards row order is due to the reordering during the spatial index
        let batch = &new_table.batches()[0];
        let _arr = batch.column(0);
    }
}
