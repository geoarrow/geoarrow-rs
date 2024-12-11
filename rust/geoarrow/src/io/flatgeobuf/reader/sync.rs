//! Reader for converting FlatGeobuf to GeoArrow tables
//!
//! FlatGeobuf implements
//! [`GeozeroDatasource`](https://docs.rs/geozero/latest/geozero/trait.GeozeroDatasource.html), so
//! it would be _possible_ to implement a fully-naive conversion, where our "GeoArrowTableBuilder"
//! struct has no idea in advance what the schema, geometry type, or number of rows is. But that's
//! inefficient, especially when the input file knows that information!
//!
//! Instead, this takes a hybrid approach. In this case where we _know_ the input format is
//! FlatGeobuf, we can use extra information from the file header to help us plan out the buffers
//! for the conversion. In particular, the header can tell us the number of features in the file
//! and the geometry type contained within. In the majority of cases where these two data points
//! are known, we can be considerably more efficient by instantiating the byte length ahead of
//! time.
//!
//! Additionally, having a known schema in advance makes the non-geometry conversion easier.
//!
//! However we don't re-implement all geometry conversion from scratch! We're able to re-use all
//! the GeomProcessor conversion from geozero, after initializing buffers with a better estimate of
//! the total length.

use crate::array::metadata::ArrayMetadata;
use crate::array::*;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::io::flatgeobuf::reader::common::{infer_schema, parse_crs, FlatGeobufReaderOptions};
use crate::io::geozero::array::MixedGeometryStreamBuilder;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use flatgeobuf::{
    FallibleStreamingIterator, FeatureIter, FgbReader, GeometryType, NotSeekable, Seekable,
};
use geozero::{FeatureProcessor, FeatureProperties};
use std::io::{Read, Seek};
use std::sync::Arc;

pub struct FlatGeobufReaderBuilder<R> {
    reader: FgbReader<R>,
}

impl<R: Read> FlatGeobufReaderBuilder<R> {
    pub fn open(reader: R) -> Result<Self> {
        let reader = FgbReader::open(reader)?;
        Ok(Self { reader })
    }

    fn infer_from_header(&self) -> Result<(NativeType, SchemaRef, Arc<ArrayMetadata>)> {
        use Dimension::*;

        let header = self.reader.header();
        if header.has_m() | header.has_t() | header.has_tm() {
            return Err(GeoArrowError::General(
                "Only XY and XYZ dimensions are supported".to_string(),
            ));
        }
        let has_z = header.has_z();

        let properties_schema = infer_schema(header);
        let geometry_type = header.geometry_type();
        let array_metadata = parse_crs(header.crs());
        // TODO: pass through arg
        let coord_type = CoordType::Interleaved;
        let data_type = match (geometry_type, has_z) {
            (GeometryType::Point, false) => NativeType::Point(coord_type, XY),
            (GeometryType::LineString, false) => NativeType::LineString(coord_type, XY),
            (GeometryType::Polygon, false) => NativeType::Polygon(coord_type, XY),
            (GeometryType::MultiPoint, false) => NativeType::MultiPoint(coord_type, XY),
            (GeometryType::MultiLineString, false) => NativeType::MultiLineString(coord_type, XY),
            (GeometryType::MultiPolygon, false) => NativeType::MultiPolygon(coord_type, XY),
            (GeometryType::Point, true) => NativeType::Point(coord_type, XYZ),
            (GeometryType::LineString, true) => NativeType::LineString(coord_type, XYZ),
            (GeometryType::Polygon, true) => NativeType::Polygon(coord_type, XYZ),
            (GeometryType::MultiPoint, true) => NativeType::MultiPoint(coord_type, XYZ),
            (GeometryType::MultiLineString, true) => NativeType::MultiLineString(coord_type, XYZ),
            (GeometryType::MultiPolygon, true) => NativeType::MultiPolygon(coord_type, XYZ),
            (GeometryType::Unknown, _) => NativeType::Geometry(coord_type),
            _ => panic!("Unsupported type"),
        };
        Ok((data_type, properties_schema, array_metadata))
    }

    pub fn read_seq(
        self,
        options: FlatGeobufReaderOptions,
    ) -> Result<FlatGeobufRecordBatchReader<R, NotSeekable>> {
        let (data_type, properties_schema, array_metadata) = self.infer_from_header()?;
        if let Some((min_x, min_y, max_x, max_y)) = options.bbox {
            let selection = self.reader.select_bbox_seq(min_x, min_y, max_x, max_y)?;
            let num_rows = selection.features_count();
            Ok(FlatGeobufRecordBatchReader {
                selection,
                data_type,
                batch_size: options.batch_size.unwrap_or(65_536),
                properties_schema,
                num_rows_remaining: num_rows,
                array_metadata,
            })
        } else {
            let selection = self.reader.select_all_seq()?;
            let num_rows = selection.features_count();
            Ok(FlatGeobufRecordBatchReader {
                selection,
                data_type,
                batch_size: options.batch_size.unwrap_or(65_536),
                properties_schema,
                num_rows_remaining: num_rows,
                array_metadata,
            })
        }
    }
}

impl<R: Read + Seek> FlatGeobufReaderBuilder<R> {
    pub fn read(
        self,
        options: FlatGeobufReaderOptions,
    ) -> Result<FlatGeobufRecordBatchReader<R, Seekable>> {
        let (data_type, properties_schema, array_metadata) = self.infer_from_header()?;
        if let Some((min_x, min_y, max_x, max_y)) = options.bbox {
            let selection = self.reader.select_bbox(min_x, min_y, max_x, max_y)?;
            let num_rows = selection.features_count();
            Ok(FlatGeobufRecordBatchReader {
                selection,
                data_type,
                batch_size: options.batch_size.unwrap_or(65_536),
                properties_schema,
                num_rows_remaining: num_rows,
                array_metadata,
            })
        } else {
            let selection = self.reader.select_all()?;
            let num_rows = selection.features_count();
            Ok(FlatGeobufRecordBatchReader {
                selection,
                data_type,
                batch_size: options.batch_size.unwrap_or(65_536),
                properties_schema,
                num_rows_remaining: num_rows,
                array_metadata,
            })
        }
    }
}

pub struct FlatGeobufRecordBatchReader<R, S> {
    selection: FeatureIter<R, S>,
    data_type: NativeType,
    batch_size: usize,
    properties_schema: SchemaRef,
    num_rows_remaining: Option<usize>,
    array_metadata: Arc<ArrayMetadata>,
}

impl<R, S> FlatGeobufRecordBatchReader<R, S> {
    fn construct_options(&self) -> GeoTableBuilderOptions {
        let coord_type = self.data_type.coord_type();
        let mut batch_size = self.batch_size;
        if let Some(num_rows_remaining) = self.num_rows_remaining {
            batch_size = batch_size.min(num_rows_remaining);
        }
        GeoTableBuilderOptions::new(
            coord_type,
            false,
            Some(batch_size),
            Some(self.properties_schema.clone()),
            self.num_rows_remaining,
            self.array_metadata.clone(),
        )
    }
}

impl<R: Read> FlatGeobufRecordBatchReader<R, NotSeekable> {
    fn process_batch(&mut self) -> Result<Option<RecordBatch>> {
        let options = self.construct_options();
        macro_rules! impl_read {
            ($builder:ty, $dim:expr) => {{
                let mut builder = GeoTableBuilder::<$builder>::new_with_options($dim, options);
                while let Some(feature) = self.selection.next()? {
                    feature.process_properties(&mut builder)?;
                    builder.properties_end()?;

                    builder.push_geometry(feature.geometry_trait()?.as_ref())?;

                    builder.feature_end(0)?;
                }
                builder.finish()
            }};
        }

        let table = match self.data_type {
            NativeType::Point(_, dim) => {
                impl_read!(PointBuilder, dim)
            }
            NativeType::LineString(_, dim) => {
                impl_read!(LineStringBuilder, dim)
            }
            NativeType::Polygon(_, dim) => {
                impl_read!(PolygonBuilder, dim)
            }
            NativeType::MultiPoint(_, dim) => {
                impl_read!(MultiPointBuilder, dim)
            }
            NativeType::MultiLineString(_, dim) => {
                impl_read!(MultiLineStringBuilder, dim)
            }
            NativeType::MultiPolygon(_, dim) => {
                impl_read!(MultiPolygonBuilder, dim)
            }
            NativeType::Geometry(_) | NativeType::GeometryCollection(_, _) => {
                let mut builder = GeoTableBuilder::<MixedGeometryStreamBuilder>::new_with_options(
                    // TODO: I think this is unused? remove.
                    Dimension::XY,
                    options,
                );
                self.selection.process_features(&mut builder)?;
                builder.finish()
            }
            geom_type => Err(GeoArrowError::NotYetImplemented(format!(
                "Parsing FlatGeobuf from {:?} geometry type not yet supported",
                geom_type
            ))),
        }?;
        let (batches, _schema) = table.into_inner();
        assert_eq!(batches.len(), 1);

        // TODO: need to propagate when we've reached the end of the fgb iterator
        Ok(Some(batches.into_iter().next().unwrap()))
    }
}

impl<R: Read + Seek> FlatGeobufRecordBatchReader<R, Seekable> {
    fn process_batch(&mut self) -> Result<Option<RecordBatch>> {
        let options = self.construct_options();
        macro_rules! impl_read {
            ($builder:ty, $dim:expr) => {{
                let mut builder = GeoTableBuilder::<$builder>::new_with_options($dim, options);
                while let Some(feature) = self.selection.next()? {
                    feature.process_properties(&mut builder)?;
                    builder.properties_end()?;

                    builder.push_geometry(feature.geometry_trait()?.as_ref())?;

                    builder.feature_end(0)?;
                }
                builder.finish()
            }};
        }

        let table = match self.data_type {
            NativeType::Point(_, dim) => {
                impl_read!(PointBuilder, dim)
            }
            NativeType::LineString(_, dim) => {
                impl_read!(LineStringBuilder, dim)
            }
            NativeType::Polygon(_, dim) => {
                impl_read!(PolygonBuilder, dim)
            }
            NativeType::MultiPoint(_, dim) => {
                impl_read!(MultiPointBuilder, dim)
            }
            NativeType::MultiLineString(_, dim) => {
                impl_read!(MultiLineStringBuilder, dim)
            }
            NativeType::MultiPolygon(_, dim) => {
                impl_read!(MultiPolygonBuilder, dim)
            }
            NativeType::Geometry(_) | NativeType::GeometryCollection(_, _) => {
                let mut builder = GeoTableBuilder::<MixedGeometryStreamBuilder>::new_with_options(
                    // TODO: I think this is unused? remove.
                    Dimension::XY,
                    options,
                );
                self.selection.process_features(&mut builder)?;
                builder.finish()
            }
            geom_type => Err(GeoArrowError::NotYetImplemented(format!(
                "Parsing FlatGeobuf from {:?} geometry type not yet supported",
                geom_type
            ))),
        }?;
        let (batches, _schema) = table.into_inner();
        assert_eq!(batches.len(), 1);

        // TODO: need to propagate when we've reached the end of the fgb iterator
        Ok(Some(batches.into_iter().next().unwrap()))
    }
}

impl<R: Read> Iterator for FlatGeobufRecordBatchReader<R, NotSeekable> {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.process_batch()
            .map_err(|err| ArrowError::ExternalError(Box::new(err)))
            .transpose()
    }
}

impl<R: Read> RecordBatchReader for FlatGeobufRecordBatchReader<R, NotSeekable> {
    fn schema(&self) -> SchemaRef {
        let geom_field =
            self.data_type
                .to_field_with_metadata("geometry", true, &self.array_metadata);
        let mut fields = self.properties_schema.fields().to_vec();
        fields.push(Arc::new(geom_field));
        Arc::new(Schema::new_with_metadata(
            fields,
            self.properties_schema.metadata().clone(),
        ))
    }
}

impl<R: Read + Seek> Iterator for FlatGeobufRecordBatchReader<R, Seekable> {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.process_batch()
            .map_err(|err| ArrowError::ExternalError(Box::new(err)))
            .transpose()
    }
}

impl<R: Read + Seek> RecordBatchReader for FlatGeobufRecordBatchReader<R, Seekable> {
    fn schema(&self) -> SchemaRef {
        let geom_field =
            self.data_type
                .to_field_with_metadata("geometry", true, &self.array_metadata);
        let mut fields = self.properties_schema.fields().to_vec();
        fields.push(Arc::new(geom_field));
        Arc::new(Schema::new_with_metadata(
            fields,
            self.properties_schema.metadata().clone(),
        ))
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::BufReader;

    use arrow_schema::DataType;

    use crate::datatypes::NativeType;
    use crate::table::Table;

    use super::*;

    #[test]
    fn test_countries() {
        let filein = BufReader::new(File::open("fixtures/flatgeobuf/countries.fgb").unwrap());
        let reader_builder = FlatGeobufReaderBuilder::open(filein).unwrap();
        let record_batch_reader = reader_builder.read(Default::default()).unwrap();
        let _batches = record_batch_reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
    }

    #[test]
    fn test_nz_buildings() {
        let filein = BufReader::new(
            File::open("fixtures/flatgeobuf/nz-building-outlines-small.fgb").unwrap(),
        );
        let reader_builder = FlatGeobufReaderBuilder::open(filein).unwrap();
        let record_batch_reader = reader_builder.read(Default::default()).unwrap();
        let _batches = record_batch_reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
    }

    #[test]
    fn test_poly() {
        let filein = BufReader::new(File::open("fixtures/flatgeobuf/poly00.fgb").unwrap());

        let reader_builder = FlatGeobufReaderBuilder::open(filein).unwrap();
        let record_batch_reader = reader_builder.read(Default::default()).unwrap();
        let table = Table::try_from(
            Box::new(record_batch_reader) as Box<dyn arrow_array::RecordBatchReader>
        )
        .unwrap();

        let geom_col = table.geometry_column(None).unwrap();
        assert!(matches!(geom_col.data_type(), NativeType::Polygon(_, _)));

        let (batches, schema) = table.into_inner();
        assert_eq!(batches[0].num_rows(), 10);
        assert!(matches!(
            schema.field_with_name("AREA").unwrap().data_type(),
            DataType::Float64
        ));
        assert!(matches!(
            schema.field_with_name("EAS_ID").unwrap().data_type(),
            DataType::Int64
        ));
        assert!(matches!(
            schema.field_with_name("PRFEDEA").unwrap().data_type(),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_all_datatypes() {
        let filein = BufReader::new(File::open("fixtures/flatgeobuf/alldatatypes.fgb").unwrap());
        let reader_builder = FlatGeobufReaderBuilder::open(filein).unwrap();
        let record_batch_reader = reader_builder.read(Default::default()).unwrap();
        let table = Table::try_from(
            Box::new(record_batch_reader) as Box<dyn arrow_array::RecordBatchReader>
        )
        .unwrap();

        let geom_col = table.geometry_column(None).unwrap();
        assert!(matches!(geom_col.data_type(), NativeType::Point(_, _)));

        let (batches, schema) = table.into_inner();
        assert_eq!(batches[0].num_rows(), 1);
        assert!(matches!(
            schema.field_with_name("byte").unwrap().data_type(),
            DataType::Int8
        ));
        assert!(matches!(
            schema.field_with_name("float").unwrap().data_type(),
            DataType::Float32
        ));
        assert!(matches!(
            schema.field_with_name("json").unwrap().data_type(),
            DataType::Utf8
        ));
        assert!(matches!(
            schema.field_with_name("binary").unwrap().data_type(),
            DataType::Binary
        ));
    }
}
