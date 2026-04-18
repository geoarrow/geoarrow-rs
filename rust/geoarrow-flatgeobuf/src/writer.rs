//! Write to [FlatGeobuf](https://flatgeobuf.org/) files.

use std::io::Write;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, SchemaRef};
use flatgeobuf::{ColumnType, FgbCrs, FgbWriter, FgbWriterOptions};
use geoarrow_array::geozero::export::{GeozeroRecordBatchReader, GeozeroRecordBatchWriter};
use geoarrow_schema::crs::{CrsTransform, DefaultCrsTransform};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{Dimension, GeoArrowType, Metadata};
use geozero::GeozeroDatasource;

/// Options for the FlatGeobuf writer
#[derive(Debug)]
pub struct FlatGeobufWriterOptions {
    name: String,
    write_index: bool,
    detect_type: bool,
    promote_to_multi: bool,
    title: Option<String>,
    description: Option<String>,
    metadata: Option<String>,
    crs_transform: Option<Box<dyn CrsTransform>>,
}

impl FlatGeobufWriterOptions {
    /// Create a new FlatGeobufWriterOptions with the given name and default options.
    pub fn new(name: String) -> Self {
        Self {
            name,
            write_index: true,
            detect_type: true,
            promote_to_multi: true,
            crs_transform: Some(Box::new(DefaultCrsTransform::default())),
            title: None,
            description: None,
            metadata: None,
        }
    }

    /// Set whether to write an index to the file.
    pub fn with_write_index(self, write_index: bool) -> Self {
        Self {
            write_index,
            ..self
        }
    }

    /// Set whether to detect geometry type when `geometry_type` is Unknown.
    pub fn with_detect_type(self, detect_type: bool) -> Self {
        Self {
            detect_type,
            ..self
        }
    }

    /// Set whether to convert single to multi geometries, if `geometry_type` is multi type or
    /// Unknown
    pub fn with_promote_to_multi(self, promote_to_multi: bool) -> Self {
        Self {
            promote_to_multi,
            ..self
        }
    }

    /// Set the dataset title
    pub fn with_title(self, title: String) -> Self {
        Self {
            title: Some(title),
            ..self
        }
    }

    /// Set the dataset description (intended for free form long text)
    pub fn with_description(self, description: String) -> Self {
        Self {
            description: Some(description),
            ..self
        }
    }

    /// Set the dataset metadata (intended to be application specific)
    pub fn with_metadata(self, metadata: String) -> Self {
        Self {
            metadata: Some(metadata),
            ..self
        }
    }

    /// Set the method for transforming CRS to WKT
    ///
    /// This is implemented as an external trait so that external libraries can inject the method
    /// for CRS conversions. For example, the Python API uses the `pyproj` Python library to
    /// perform the conversion rather than linking into PROJ from Rust.
    pub fn with_crs_transform(self, crs_transform: Box<dyn CrsTransform>) -> Self {
        Self {
            crs_transform: Some(crs_transform),
            ..self
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

/// A FlatGeobuf writer.
///
/// This differs from `write_flatgeobuf` in that it allows for **push-based** instead of
/// **pull-based** iteration. The `stream` parameter of `write_flatgeobuf` only allows for
/// `write_flatgeobuf` to pull data; whereas some environments may find it easier to use a
/// push-based writer.
pub struct FlatGeobufWriter<'a, W: Write> {
    file: W,
    geozero_writer: GeozeroRecordBatchWriter<FgbWriter<'a>>,
}

impl<W: Write> FlatGeobufWriter<'_, W> {
    /// Create a new FlatGeobufWriter with the given options.
    pub fn try_new(
        file: W,
        schema: SchemaRef,
        options: FlatGeobufWriterOptions,
    ) -> GeoArrowResult<Self> {
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

        let geometry_type = infer_flatgeobuf_geometry_type(schema.as_ref())?;
        let mut fgb_writer =
            FgbWriter::create_with_options(&options.name, geometry_type, fgb_options)
                .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        register_columns(&mut fgb_writer, schema.as_ref(), geom_col_idxs[0]);
        let geozero_writer = GeozeroRecordBatchWriter::try_new(schema, fgb_writer, None).unwrap();

        Ok(Self {
            file,
            geozero_writer,
        })
    }

    /// Write a [`RecordBatch`] to the FlatGeobuf file.
    ///
    /// This will error if the schema of the `RecordBatch` does not match the schema originally
    /// passed to [`FlatGeobufWriter::try_new`].
    pub fn write(&mut self, batch: &RecordBatch) -> GeoArrowResult<()> {
        self.geozero_writer
            .write(batch)
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;

        Ok(())
    }

    /// Finish writing the FlatGeobuf file and return the underlying writer.
    pub fn finish(mut self) -> GeoArrowResult<W> {
        let fgb_writer = self
            .geozero_writer
            .finish()
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        fgb_writer
            .write(&mut self.file)
            .map_err(|err| GeoArrowError::External(Box::new(err)))?;
        Ok(self.file)
    }
}

/// Write an iterator of GeoArrow RecordBatches to a FlatGeobuf file.
///
/// `name` is the string passed to [`FgbWriter::create`] and is what OGR observes as the layer name
/// of the file.
pub fn write_flatgeobuf<W: Write, S: Into<GeozeroRecordBatchReader>>(
    stream: S,
    writer: W,
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

    let mut fgb = FgbWriter::create_with_options(&options.name, geometry_type, fgb_options)
        .map_err(|err| GeoArrowError::External(Box::new(err)))?;
    register_columns(&mut fgb, schema.as_ref(), geom_col_idxs[0]);
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
        Rect(_) | Polygon(_) => flatgeobuf::GeometryType::Polygon,
        MultiPoint(_) => flatgeobuf::GeometryType::MultiPoint,
        MultiLineString(_) => flatgeobuf::GeometryType::MultiLineString,
        MultiPolygon(_) => flatgeobuf::GeometryType::MultiPolygon,
        Geometry(_) | Wkb(_) | LargeWkb(_) | WkbView(_) | Wkt(_) | LargeWkt(_) | WktView(_) => {
            flatgeobuf::GeometryType::Unknown
        }
        GeometryCollection(_) => flatgeobuf::GeometryType::GeometryCollection,
    };
    Ok(geometry_type)
}

/// Pre-register all non-geometry columns on the FgbWriter so that column indices in the
/// FlatGeobuf header always match the Arrow schema, even when a column has 100% null values
/// (which would never be passed to `property()` and thus never auto-registered).
///
/// Without this, FgbWriter silently drops any property whose index is more than one past the
/// last registered column, corrupting all columns that follow a fully-null column.
fn register_columns(fgb: &mut FgbWriter, schema: &Schema, geom_col_idx: usize) {
    for (col_idx, field) in schema.fields().iter().enumerate() {
        if col_idx == geom_col_idx {
            continue;
        }
        let col_type = arrow_type_to_fgb_column_type(field.data_type());
        let nullable = field.is_nullable();
        fgb.add_column(field.name(), col_type, move |_fbb, col| {
            col.nullable = nullable;
        });
    }
}

fn arrow_type_to_fgb_column_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Boolean => ColumnType::Bool,
        DataType::UInt8 => ColumnType::UByte,
        DataType::Int8 => ColumnType::Byte,
        DataType::UInt16 => ColumnType::UShort,
        DataType::Int16 => ColumnType::Short,
        DataType::UInt32 => ColumnType::UInt,
        DataType::Int32 => ColumnType::Int,
        DataType::UInt64 => ColumnType::ULong,
        DataType::Int64 => ColumnType::Long,
        DataType::Float16 | DataType::Float32 => ColumnType::Float,
        DataType::Float64 => ColumnType::Double,
        DataType::Utf8 | DataType::LargeUtf8 => ColumnType::String,
        DataType::Binary | DataType::LargeBinary => ColumnType::Binary,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => ColumnType::DateTime,
        // Struct, List, LargeList, Map are serialised as JSON
        _ => ColumnType::Json,
    }
}

// Note: this is duplicated from the `geoarrow-array` crate.
fn geometry_columns(schema: &Schema) -> Vec<usize> {
    let mut geom_indices = vec![];
    for (field_idx, field) in schema.fields().iter().enumerate() {
        if let Ok(Some(_)) = GeoArrowType::from_extension_field(field.as_ref()) {
            geom_indices.push(field_idx);
        }
    }
    geom_indices
}

#[cfg(test)]
mod test {
    use std::io::{BufWriter, Cursor};
    use std::sync::Arc;

    use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, create_array};
    use arrow_schema::{DataType, Field, Schema};
    use flatgeobuf::{FallibleStreamingIterator, FgbReader};
    use geoarrow_array::GeoArrowArray;
    use geoarrow_array::array::PointArray;
    use geoarrow_array::builder::PointBuilder;
    use geoarrow_schema::PointType;
    use geozero::FeatureProperties;
    use wkt::wkt;

    use super::*;
    use crate::reader::{
        FlatGeobufHeaderExt, FlatGeobufReaderOptions, FlatGeobufRecordBatchIterator,
    };

    // FlatGeobuf, or at least the FlatGeobuf rust library, doesn't support writing null or empty
    // points.
    fn non_empty_point_array(dim: Dimension) -> PointArray {
        let geoms = match dim {
            Dimension::XY => vec![
                Some(wkt! { POINT (30. 10.) }),
                Some(wkt! { POINT (40. 20.) }),
                Some(wkt! { POINT (1. 2.) }),
                Some(wkt! { POINT (1. 2.) }),
            ],
            Dimension::XYZ => vec![
                Some(wkt! { POINT Z (30. 10. 5.) }),
                Some(wkt! { POINT Z (40. 20. 2.) }),
                Some(wkt! { POINT Z (1. 2. 1.) }),
                Some(wkt! { POINT Z (1. 2. 1.) }),
            ],
            Dimension::XYM => vec![
                Some(wkt! { POINT M (30. 10. 5.) }),
                Some(wkt! { POINT M (40. 20. 2.) }),
                Some(wkt! { POINT M (1. 2. 1.) }),
                Some(wkt! { POINT M (1. 2. 1.) }),
            ],
            Dimension::XYZM => vec![
                Some(wkt! { POINT ZM (30. 10. 5. 1.) }),
                Some(wkt! { POINT ZM (40. 20. 2. 2.) }),
                Some(wkt! { POINT ZM (1. 2. 1. 3.) }),
                Some(wkt! { POINT ZM (1. 2. 1. 4.) }),
            ],
        };
        let typ = PointType::new(dim, Default::default());
        PointBuilder::from_nullable_points(geoms.iter().map(|x| x.as_ref()), typ).finish()
    }

    fn table(geometry: Arc<dyn GeoArrowArray>) -> (Vec<RecordBatch>, Arc<Schema>) {
        let u8_array = create_array!(UInt8, [1, 2, 3, 4]);
        let string_array = create_array!(Utf8, ["1", "2", "3", "4"]);

        let fields = vec![
            Arc::new(Field::new("u8", DataType::UInt8, true)),
            Arc::new(Field::new("string", DataType::Utf8, true)),
            Arc::new(geometry.data_type().to_field("geometry", true)),
        ];
        let schema = Arc::new(Schema::new(fields));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![u8_array, string_array, geometry.into_array_ref()],
        )
        .unwrap();

        (vec![batch], schema)
    }

    #[test]
    fn test_write() {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let (orig_batches, orig_schema) = table(Arc::new(non_empty_point_array(dim)));
            let source_iterator = Box::new(RecordBatchIterator::new(
                orig_batches.clone().into_iter().map(Ok),
                orig_schema.clone(),
            ));
            let geozero_reader = GeozeroRecordBatchReader::new(Box::new(source_iterator));

            // Write to buffer
            let mut output_buffer = Vec::new();
            let writer = BufWriter::new(&mut output_buffer);
            write_flatgeobuf(
                geozero_reader,
                writer,
                FlatGeobufWriterOptions::new("name".to_string()),
            )
            .unwrap();

            // Read back from buffer
            let reader = Cursor::new(output_buffer);
            let fgb_reader = FgbReader::open(reader).unwrap();
            let fgb_header = fgb_reader.header();

            let properties_schema = fgb_header
                .properties_schema(false)
                .expect("file contains column information in metadata.");
            let geometry_type = fgb_header.geoarrow_type(Default::default()).unwrap();

            let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type);
            let selection = fgb_reader.select_all_seq().unwrap();
            let record_batch_reader =
                FlatGeobufRecordBatchIterator::try_new(selection, options).unwrap();

            let schema = record_batch_reader.schema();
            let field = schema.field_with_name("geometry").unwrap();
            assert!(matches!(
                GeoArrowType::try_from(field).unwrap(),
                GeoArrowType::Point(_)
            ));
            assert_eq!(schema, orig_schema);

            let batches = record_batch_reader.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(batches, orig_batches);
        }
    }

    #[test]
    fn test_write_no_index() {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let (orig_batches, orig_schema) = table(Arc::new(non_empty_point_array(dim)));
            let reader = Box::new(RecordBatchIterator::new(
                orig_batches.clone().into_iter().map(Ok),
                orig_schema.clone(),
            ));
            let geozero_reader = GeozeroRecordBatchReader::new(Box::new(reader));

            // Write to buffer
            let mut output_buffer = Vec::new();
            let writer = BufWriter::new(&mut output_buffer);
            let options = FlatGeobufWriterOptions::new("name".to_string()).with_write_index(false);
            write_flatgeobuf(geozero_reader, writer, options).unwrap();

            // Read back from buffer
            let reader = Cursor::new(output_buffer);
            let fgb_reader = FgbReader::open(reader).unwrap();
            let fgb_header = fgb_reader.header();

            let properties_schema = fgb_header
                .properties_schema(false)
                .expect("file contains column information in metadata.");
            let geometry_type = fgb_header.geoarrow_type(Default::default()).unwrap();

            let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type);
            let selection = fgb_reader.select_all_seq().unwrap();
            let record_batch_reader =
                FlatGeobufRecordBatchIterator::try_new(selection, options).unwrap();

            let schema = record_batch_reader.schema();
            let field = schema.field_with_name("geometry").unwrap();
            assert!(matches!(
                GeoArrowType::try_from(field).unwrap(),
                GeoArrowType::Point(_)
            ));
            assert_eq!(schema, orig_schema);

            let batches = record_batch_reader.collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(batches, orig_batches);
        }
    }

    #[test]
    fn test_sparse_fields() {
        let typ = PointType::new(Dimension::XY, Default::default());
        let example_geometry = PointBuilder::from_points(
            vec![wkt! { POINT (0. 1.) }, wkt! { POINT (1. 2.) }].iter(),
            typ.clone(),
        )
        .finish();
        let fields = vec![
            Arc::new(Field::new("u8", DataType::UInt8, true)),
            Arc::new(Field::new("string1", DataType::Utf8, true)),
            Arc::new(Field::new("string2", DataType::Utf8, true)),
            Arc::new(example_geometry.data_type().to_field("geometry", true)),
        ];
        let schema = Arc::new(Schema::new(fields));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                create_array!(UInt8, [1, 2]),
                create_array!(Utf8, ["a", "b"]),
                create_array!(Utf8, ["zzz", "zzz"]),
                PointBuilder::from_points(
                    vec![wkt! { POINT (0. 1.) }, wkt! { POINT (1. 2.) }].iter(),
                    typ.clone(),
                )
                .finish()
                .into_array_ref(),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                create_array!(UInt8, [3, 4]),
                create_array!(Utf8, [None::<String>, None]),
                create_array!(Utf8, ["zzz", "zzz"]),
                PointBuilder::from_points(
                    vec![wkt! { POINT (3. 4.) }, wkt! { POINT (4. 5.) }].iter(),
                    typ.clone(),
                )
                .finish()
                .into_array_ref(),
            ],
        )
        .unwrap();

        let mut output_buffer = Vec::new();
        {
            let bw = BufWriter::new(&mut output_buffer);
            let options = FlatGeobufWriterOptions::new("test".to_string());
            let mut fgb_writer = FlatGeobufWriter::try_new(bw, schema, options).unwrap();
            fgb_writer.write(&batch1).unwrap();
            fgb_writer.write(&batch2).unwrap();
            fgb_writer.finish().unwrap();
        }

        let reader = Cursor::new(output_buffer);
        let mut fgb_reader = FgbReader::open(reader).unwrap().select_all().unwrap();

        let mut results: Vec<(u8, Option<String>, String)> = vec![];
        while let Some(feature) = fgb_reader.next().unwrap() {
            let u8_val = feature.property::<u8>("u8").unwrap();
            // string1 is null for batch2 features (u8=3,4); FlatGeobuf omits null properties
            let string1 = feature.property::<String>("string1").ok();
            // string2 must be present for all features, even those where string1 is null
            let string2 = feature
                .property::<String>("string2")
                .expect("string2 must be present even when string1 is null");
            results.push((u8_val, string1, string2));
        }

        results.sort_by_key(|(u, _, _)| *u);
        assert_eq!(results.len(), 4);
        assert_eq!(results[0], (1, Some("a".to_string()), "zzz".to_string()));
        assert_eq!(results[1], (2, Some("b".to_string()), "zzz".to_string()));
        assert_eq!(results[2], (3, None, "zzz".to_string()));
        assert_eq!(results[3], (4, None, "zzz".to_string()));
    }

    /// Columns following a 100% null column must not be dropped from the output.
    #[test]
    fn test_all_null_column() {
        let typ = PointType::new(Dimension::XY, Default::default());
        let fields = vec![
            Arc::new(Field::new("u8", DataType::UInt8, true)),
            Arc::new(Field::new("all_null", DataType::Utf8, true)),
            Arc::new(Field::new("after_null", DataType::Utf8, true)),
            Arc::new(typ.to_field("geometry", true)),
        ];
        let schema = Arc::new(Schema::new(fields));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                create_array!(UInt8, [1, 2]),
                create_array!(Utf8, [None::<String>, None]),
                create_array!(Utf8, ["hello", "world"]),
                PointBuilder::from_points(
                    vec![wkt! { POINT (0. 1.) }, wkt! { POINT (1. 2.) }].iter(),
                    typ.clone(),
                )
                .finish()
                .into_array_ref(),
            ],
        )
        .unwrap();

        let mut output_buffer = Vec::new();
        {
            let bw = BufWriter::new(&mut output_buffer);
            let options = FlatGeobufWriterOptions::new("test".to_string());
            let mut fgb_writer = FlatGeobufWriter::try_new(bw, schema, options).unwrap();
            fgb_writer.write(&batch).unwrap();
            fgb_writer.finish().unwrap();
        }

        let reader = Cursor::new(output_buffer);
        let mut fgb_reader = FgbReader::open(reader).unwrap().select_all().unwrap();
        let mut results: Vec<(u8, Option<String>, String)> = vec![];
        while let Some(feature) = fgb_reader.next().unwrap() {
            let u8_val = feature.property::<u8>("u8").unwrap();
            let all_null = feature.property::<String>("all_null").ok();
            let after_null = feature
                .property::<String>("after_null")
                .expect("after_null must be readable even though all_null is 100% null");
            results.push((u8_val, all_null, after_null));
        }

        results.sort_by_key(|(u, _, _)| *u);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (1, None, "hello".to_string()));
        assert_eq!(results[1], (2, None, "world".to_string()));
    }
}
