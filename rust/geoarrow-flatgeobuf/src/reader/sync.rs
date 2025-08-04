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

use std::io::{Read, Seek};
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use flatgeobuf::{FallibleStreamingIterator, FeatureIter, NotSeekable, Seekable};
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geozero::FeatureProperties;

use crate::reader::common::FlatGeobufReaderOptions;
use crate::reader::table_builder::{GeoArrowRecordBatchBuilder, GeoArrowRecordBatchBuilderOptions};

/// An iterator over record batches from a FlatGeobuf file.
///
/// This implements [arrow_array::RecordBatchReader], which you can use to access data.
pub struct FlatGeobufRecordBatchIterator<R: Read, S> {
    selection: FeatureIter<R, S>,
    geometry_type: GeoArrowType,
    batch_size: usize,
    properties_schema: SchemaRef,
    num_rows_remaining: Option<usize>,
    read_geometry: bool,
}

impl<R: Read, S> FlatGeobufRecordBatchIterator<R, S> {
    /// Create a new FlatGeobuf record batch iterator from a feature iterator from the
    /// [`flatgeobuf`] crate.
    pub fn try_new(
        selection: FeatureIter<R, S>,
        options: FlatGeobufReaderOptions,
    ) -> GeoArrowResult<Self> {
        let header = selection.header();
        options.validate_against_header(&header)?;

        let num_rows_remaining = selection.features_count();
        Ok(Self {
            selection,
            geometry_type: options.geometry_type,
            batch_size: options.batch_size,
            properties_schema: options.properties_schema,
            num_rows_remaining,
            read_geometry: options.read_geometry,
        })
    }

    fn output_schema(&self) -> SchemaRef {
        let mut fields = self.properties_schema.fields().to_vec();
        if self.read_geometry {
            fields.push(self.geometry_type.to_field("geometry", true).into());
        }
        Arc::new(Schema::new_with_metadata(
            fields,
            self.properties_schema.metadata().clone(),
        ))
    }
}

impl<R: Read> FlatGeobufRecordBatchIterator<R, NotSeekable> {
    fn process_batch(&mut self) -> GeoArrowResult<Option<RecordBatch>> {
        let options = GeoArrowRecordBatchBuilderOptions {
            batch_size: self
                .num_rows_remaining
                .map(|num_rows_remaining| num_rows_remaining.min(self.batch_size)),
            error_on_extra_columns: false,
            read_geometry: self.read_geometry,
        };
        let mut record_batch_builder = GeoArrowRecordBatchBuilder::new(
            self.properties_schema.clone(),
            self.geometry_type.clone(),
            &options,
        );

        let mut row_count = 0;
        loop {
            if row_count >= self.batch_size {
                let batch = record_batch_builder.finish()?;
                return Ok(Some(batch));
            }

            if let Some(feature) = self
                .selection
                .next()
                .map_err(|err| GeoArrowError::External(Box::new(err)))?
            {
                feature
                    .process_properties(&mut record_batch_builder)
                    .map_err(|err| GeoArrowError::External(Box::new(err)))?;
                // record_batch_builder.properties_end()?;

                record_batch_builder.push_geometry(
                    feature
                        .geometry_trait()
                        .map_err(|err| GeoArrowError::External(Box::new(err)))?
                        .as_ref(),
                )?;

                // $builder.feature_end(0)?;
                row_count += 1;
            } else if row_count > 0 {
                return Ok(Some(record_batch_builder.finish()?));
            } else {
                return Ok(None);
            }
        }
    }
}

impl<R: Read> Iterator for FlatGeobufRecordBatchIterator<R, NotSeekable> {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.process_batch().map_err(|err| err.into()).transpose()
    }
}

impl<R: Read> RecordBatchReader for FlatGeobufRecordBatchIterator<R, NotSeekable> {
    fn schema(&self) -> SchemaRef {
        self.output_schema()
    }
}

impl<R: Read + Seek> FlatGeobufRecordBatchIterator<R, Seekable> {
    fn process_batch(&mut self) -> GeoArrowResult<Option<RecordBatch>> {
        let options = GeoArrowRecordBatchBuilderOptions {
            batch_size: self
                .num_rows_remaining
                .map(|num_rows_remaining| num_rows_remaining.min(self.batch_size)),
            error_on_extra_columns: false,
            read_geometry: self.read_geometry,
        };
        let mut record_batch_builder = GeoArrowRecordBatchBuilder::new(
            self.properties_schema.clone(),
            self.geometry_type.clone(),
            &options,
        );

        let mut row_count = 0;
        loop {
            if row_count >= self.batch_size {
                return Ok(Some(record_batch_builder.finish()?));
            }

            if let Some(feature) = self
                .selection
                .next()
                .map_err(|err| GeoArrowError::External(Box::new(err)))?
            {
                feature
                    .process_properties(&mut record_batch_builder)
                    .map_err(|err| GeoArrowError::External(Box::new(err)))?;
                // record_batch_builder.properties_end()?;

                record_batch_builder.push_geometry(
                    feature
                        .geometry_trait()
                        .map_err(|err| GeoArrowError::External(Box::new(err)))?
                        .as_ref(),
                )?;

                // $builder.feature_end(0)?;
                row_count += 1;
            } else if row_count > 0 {
                return Ok(Some(record_batch_builder.finish()?));
            } else {
                return Ok(None);
            }
        }
    }
}

impl<R: Read + Seek> Iterator for FlatGeobufRecordBatchIterator<R, Seekable> {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.process_batch().map_err(|err| err.into()).transpose()
    }
}

impl<R: Read + Seek> RecordBatchReader for FlatGeobufRecordBatchIterator<R, Seekable> {
    fn schema(&self) -> SchemaRef {
        self.output_schema()
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::BufReader;

    use arrow_schema::DataType;
    use flatgeobuf::FgbReader;

    use crate::reader::common::FlatGeobufHeaderExt;

    use super::*;

    #[test]
    fn test_countries() {
        let filein = BufReader::new(File::open("../../fixtures/flatgeobuf/countries.fgb").unwrap());
        let fgb_reader = FgbReader::open(filein).unwrap();
        let fgb_header = fgb_reader.header();

        let properties_schema = fgb_header
            .properties_schema(true)
            .expect("file contains column information in metadata.");
        let geometry_type = fgb_header.geoarrow_type(Default::default()).unwrap();
        let selection = fgb_reader.select_all_seq().unwrap();

        let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type);
        let record_batch_reader =
            FlatGeobufRecordBatchIterator::try_new(selection, options).unwrap();
        let _batches = record_batch_reader.collect::<Result<Vec<_>, _>>().unwrap();
        // println!("{}", pretty_format_batches(&_batches).unwrap());
        // print!(format!(pretty_format_batches(&batches).unwrap()));
        // dbg!(_batches.len());
    }

    #[test]
    fn test_nz_buildings() {
        let filein = BufReader::new(
            File::open("../../fixtures/flatgeobuf/nz-building-outlines-small.fgb").unwrap(),
        );
        let fgb_reader = FgbReader::open(filein).unwrap();
        let fgb_header = fgb_reader.header();

        let properties_schema = fgb_header
            .properties_schema(true)
            .expect("file contains column information in metadata.");
        let geometry_type = fgb_header.geoarrow_type(Default::default()).unwrap();

        let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type);
        let selection = fgb_reader.select_all().unwrap();
        let record_batch_reader =
            FlatGeobufRecordBatchIterator::try_new(selection, options).unwrap();
        let _batches = record_batch_reader.collect::<Result<Vec<_>, _>>().unwrap();
    }

    #[test]
    fn test_poly() {
        let filein = BufReader::new(File::open("../../fixtures/flatgeobuf/poly00.fgb").unwrap());
        let fgb_reader = FgbReader::open(filein).unwrap();
        let fgb_header = fgb_reader.header();

        let properties_schema = fgb_header
            .properties_schema(true)
            .expect("file contains column information in metadata.");
        let geometry_type = fgb_header.geoarrow_type(Default::default()).unwrap();

        let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type);
        let selection = fgb_reader.select_all().unwrap();
        let record_batch_reader =
            FlatGeobufRecordBatchIterator::try_new(selection, options).unwrap();

        let schema = record_batch_reader.schema();
        let field = schema.field_with_name("geometry").unwrap();
        assert!(matches!(
            GeoArrowType::try_from(field).unwrap(),
            GeoArrowType::Polygon(_)
        ));
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
            DataType::Utf8View
        ));

        let batches = record_batch_reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches[0].num_rows(), 10);
    }

    #[test]
    fn test_all_datatypes() {
        let filein =
            BufReader::new(File::open("../../fixtures/flatgeobuf/alldatatypes.fgb").unwrap());
        let fgb_reader = FgbReader::open(filein).unwrap();
        let fgb_header = fgb_reader.header();

        let properties_schema = fgb_header
            .properties_schema(true)
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
            GeoArrowType::Geometry(_)
        ));

        let batches = record_batch_reader.collect::<Result<Vec<_>, _>>().unwrap();
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
            DataType::Utf8View
        ));
        assert!(matches!(
            schema.field_with_name("binary").unwrap().data_type(),
            DataType::BinaryView
        ));
    }
}
