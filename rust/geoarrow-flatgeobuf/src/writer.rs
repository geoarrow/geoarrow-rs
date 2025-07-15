use std::io::Write;

use arrow_schema::Schema;
use flatgeobuf::{FgbCrs, FgbWriter, FgbWriterOptions};
use geoarrow_array::GeoArrowType;
use geoarrow_array::crs::{CRSTransform, DefaultCRSTransform};
use geoarrow_array::error::{GeoArrowError, Result};
use geoarrow_array::geozero::export::GeozeroRecordBatchReader;
use geoarrow_array::geozero::export::GeozeroRecordBatchReader;
use geoarrow_schema::crs::{CrsTransform, DefaultCrsTransform};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{Dimension, GeoArrowType, Metadata};
use geoarrow_schema::{Dimension, Metadata};
use geozero::GeozeroDatasource;

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
    pub crs_transform: Option<Box<dyn CrsTransform>>,
}

impl Default for FlatGeobufWriterOptions {
    fn default() -> Self {
        Self {
            write_index: true,
            detect_type: true,
            promote_to_multi: true,
            crs_transform: Some(Box::new(DefaultCrsTransform::default())),
            title: None,
            description: None,
            metadata: None,
        }
    }
}

impl FlatGeobufWriterOptions {
    /// Create a WKT CRS from whatever CRS exists in the [Metadata].
    ///
    /// This uses the [CrsTransform] supplied in the [FlatGeobufWriterOptions].
    ///
    /// If no CRS exists in the Metadata, None will be returned here.
    fn create_wkt_crs(&self, array_meta: &Metadata) -> GeoArrowResult<Option<String>> {
        if let Some(crs_transform) = &self.crs_transform {
            crs_transform.extract_wkt(array_meta.crs())
        } else {
            DefaultCrsTransform::default().extract_wkt(array_meta.crs())
        }
    }

    /// Create [FgbWriterOptions]
    fn create_fgb_options<'a>(
        &'a self,
        geo_data_type: GeoArrowType,
        wkt_crs: Option<&'a str>,
    ) -> FgbWriterOptions<'a> {
        let (has_z, has_m) = match geo_data_type.dimension() {
            Some(Dimension::XY) => (false, false),
            Some(Dimension::XYZ) => (true, false),
            Some(Dimension::XYM) => (false, true),
            Some(Dimension::XYZM) => (true, true),
            // TODO: not sure how to handle geometry arrays
            // Here, we declare them as not having z or m dimensions.
            None => (false, false),
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
pub fn write_flatgeobuf<W: Write, S: Into<GeozeroRecordBatchReader>>(
    stream: S,
    writer: W,
    name: &str,
) -> GeoArrowResult<()> {
    write_flatgeobuf_with_options(stream, writer, name, Default::default())
}

/// Write a Table to a FlatGeobuf file with specific writer options.
///
/// `name` is the string passed to [`FgbWriter::create`] and is what OGR observes as the layer name
/// of the file.
pub fn write_flatgeobuf_with_options<W: Write, S: Into<GeozeroRecordBatchReader>>(
    stream: S,
    writer: W,
    name: &str,
    options: FlatGeobufWriterOptions,
) -> GeoArrowResult<()> {
    let mut stream: GeozeroRecordBatchReader = stream.into();

    let schema = stream.as_ref().schema();
    let fields = &schema.fields;
    let geom_col_idxs = geometry_columns(schema.as_ref());
    if geom_col_idxs.len() != 1 {
        return Err(GeoArrowError::FlatGeobuf(
            "Only one geometry column currently supported in FlatGeobuf writer".to_string(),
        ));
    }

    let geometry_field = &fields[geom_col_idxs[0]];
    let geo_data_type = GeoArrowType::try_from(geometry_field.as_ref())?;

    let wkt_crs_str = options.create_wkt_crs(geo_data_type.metadata())?;
    let fgb_options = options.create_fgb_options(geo_data_type, wkt_crs_str.as_deref());

    let geometry_type = infer_flatgeobuf_geometry_type(stream.as_ref().schema().as_ref())?;

    let mut fgb = FgbWriter::create_with_options(name, geometry_type, fgb_options)
        .map_err(|err| GeoArrowError::External(Box::new(err)))?;
    stream
        .process(&mut fgb)
        .map_err(|err| GeoArrowError::External(Box::new(err)))?;
    fgb.write(writer)
        .map_err(|err| GeoArrowError::External(Box::new(err)))?;
    Ok(())
}

fn infer_flatgeobuf_geometry_type(schema: &Schema) -> GeoArrowResult<flatgeobuf::GeometryType> {
    let fields = &schema.fields;
    let geom_col_idxs = geometry_columns(schema);
    if geom_col_idxs.len() != 1 {
        return Err(GeoArrowError::FlatGeobuf(
            "Only one geometry column currently supported in FlatGeobuf writer".to_string(),
        ));
    }

    let geometry_field = &fields[geom_col_idxs[0]];
    let geo_data_type = GeoArrowType::try_from(geometry_field.as_ref())?;

    use GeoArrowType::*;
    let geometry_type = match geo_data_type {
        Point(_) => flatgeobuf::GeometryType::Point,
        LineString(_) => flatgeobuf::GeometryType::LineString,
        Polygon(_) => flatgeobuf::GeometryType::Polygon,
        MultiPoint(_) => flatgeobuf::GeometryType::MultiPoint,
        MultiLineString(_) => flatgeobuf::GeometryType::MultiLineString,
        MultiPolygon(_) => flatgeobuf::GeometryType::MultiPolygon,
        Rect(_) | Geometry(_) | Wkb(_) | LargeWkb(_) | WkbView(_) | Wkt(_) | LargeWkt(_)
        | WktView(_) => flatgeobuf::GeometryType::Unknown,
        GeometryCollection(_) => flatgeobuf::GeometryType::GeometryCollection,
    };
    Ok(geometry_type)
}

// Note: this is duplicated from the `geoarrow-array` crate.
fn geometry_columns(schema: &Schema) -> Vec<usize> {
    let mut geom_indices = vec![];
    for (field_idx, field) in schema.fields().iter().enumerate() {
        // We first check that an extension type name is set and then check that we can coerce to a
        // GeoArrowType so that we don't accept columns that are _compatible_ with geoarrow storage
        // but aren't set as geoarrow extension types.
        if let Some(_ext_name) = field.extension_type_name() {
            if let Ok(_geoarrow_type) = GeoArrowType::try_from(field.as_ref()) {
                geom_indices.push(field_idx);
            }
        }
    }
    geom_indices
}

#[cfg(test)]
mod test {
    use std::io::BufWriter;
    use std::sync::Arc;

    use arrow_array::{RecordBatch, RecordBatchIterator, create_array};
    use arrow_schema::{DataType, Field};
    use geoarrow_array::GeoArrowArray;
    use geoarrow_array::array::PointArray;
    use geoarrow_array::builder::PointBuilder;
    use geoarrow_schema::PointType;
    use wkt::wkt;

    use super::*;

    // FlatGeobuf, or at least the FlatGeobuf rust library, doesn't support writing null or empty
    // points.
    fn non_empty_point_array() -> PointArray {
        let geoms = vec![
            Some(wkt! { POINT (30. 10.) }),
            Some(wkt! { POINT (40. 20.) }),
            Some(wkt! { POINT (1. 2.) }),
            Some(wkt! { POINT (1. 2.) }),
        ];
        let typ = PointType::new(Dimension::XY, Default::default());
        PointBuilder::from_nullable_points(geoms.iter().map(|x| x.as_ref()), typ).finish()
    }

    fn table() -> (Vec<RecordBatch>, Arc<Schema>) {
        let point_array = non_empty_point_array();
        let u8_array = create_array!(UInt8, [1, 2, 3, 4]);
        let string_array = create_array!(Utf8, ["1", "2", "3", "4"]);

        let fields = vec![
            Arc::new(Field::new("u8", DataType::UInt8, true)),
            Arc::new(Field::new("string", DataType::Utf8, true)),
            Arc::new(point_array.data_type().to_field("geometry", true)),
        ];
        let schema = Arc::new(Schema::new(fields));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![u8_array, string_array, point_array.into_array_ref()],
        )
        .unwrap();

        (vec![batch], schema)
    }

    #[test]
    fn test_write() {
        let (batches, schema) = table();
        let reader = Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        ));
        let geozero_reader = GeozeroRecordBatchReader::new(Box::new(reader));

        let mut output_buffer = Vec::new();
        let writer = BufWriter::new(&mut output_buffer);
        write_flatgeobuf(geozero_reader, writer, "name").unwrap();

        // let reader = Cursor::new(output_buffer);
        // let reader_builder = FlatGeobufReaderBuilder::open(reader).unwrap();
        // let record_batch_reader = reader_builder.read(Default::default()).unwrap();
        // let new_table = Table::try_from(
        //     Box::new(record_batch_reader) as Box<dyn arrow_array::RecordBatchReader>
        // )
        // .unwrap();

        // // Note: backwards row order is due to the reordering during the spatial index
        // let batch = &new_table.batches()[0];
        // let arr = batch.column(0);
        // dbg!(arr);
        // dbg!(new_table);
        // dbg!(output_buffer);
    }

    #[test]
    fn test_write_no_index() {
        let (batches, schema) = table();
        let reader = Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        ));
        let geozero_reader = GeozeroRecordBatchReader::new(Box::new(reader));

        let mut output_buffer = Vec::new();
        let writer = BufWriter::new(&mut output_buffer);
        let options = FlatGeobufWriterOptions {
            write_index: false,
            ..Default::default()
        };
        write_flatgeobuf_with_options(geozero_reader, writer, "name", options).unwrap();

        // let reader = Cursor::new(output_buffer);
        // let reader_builder = FlatGeobufReaderBuilder::open(reader).unwrap();
        // let record_batch_reader = reader_builder.read(Default::default()).unwrap();
        // let new_table = Table::try_from(
        //     Box::new(record_batch_reader) as Box<dyn arrow_array::RecordBatchReader>
        // )
        // .unwrap();
        // assert_eq!(table, new_table);
    }

    // #[test]
    // fn test_write_z() {
    //     let table = point::table_z();

    //     let mut output_buffer = Vec::new();
    //     let writer = BufWriter::new(&mut output_buffer);
    //     write_flatgeobuf(&table, writer, "name").unwrap();

    //     let reader = Cursor::new(output_buffer);
    //     let reader_builder = FlatGeobufReaderBuilder::open(reader).unwrap();
    //     let record_batch_reader = reader_builder.read(Default::default()).unwrap();
    //     let new_table = Table::try_from(
    //         Box::new(record_batch_reader) as Box<dyn arrow_array::RecordBatchReader>
    //     )
    //     .unwrap();

    //     // Note: backwards row order is due to the reordering during the spatial index
    //     let batch = &new_table.batches()[0];
    //     let _arr = batch.column(0);
    // }
}
