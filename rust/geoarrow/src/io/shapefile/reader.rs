use std::io::{Read, Seek};
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef};
use dbase::{FieldInfo, FieldType, FieldValue, Record};
use geozero::FeatureProcessor;
use shapefile::reader::ShapeRecordIterator;
use shapefile::{Reader, ShapeReader, ShapeType};

use crate::array::metadata::ArrayMetadata;
use crate::array::{
    CoordType, MultiLineStringBuilder, MultiPointBuilder, MultiPolygonBuilder, PointBuilder,
};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::io::geozero::table::builder::anyvalue::AnyBuilder;
use crate::io::geozero::table::builder::properties::PropertiesBatchBuilder;
use crate::io::geozero::table::{GeoTableBuilder, GeoTableBuilderOptions};

/// Options for the Shapefile reader
#[derive(Debug, Clone, Default)]
pub struct ShapefileReaderOptions {
    /// The GeoArrow coordinate type to use in the geometry arrays.
    pub coord_type: CoordType,

    /// The number of rows in each batch.
    pub batch_size: Option<usize>,

    /// The CRS to assign to the file. Read this from the `.prj` file in the same directory with
    /// the same name.
    pub crs: Option<String>,
}

/// A builder for [ShapefileReader]
pub struct ShapefileReaderBuilder<T: Read + Seek + Send> {
    dbf_fields: Vec<FieldInfo>,
    options: GeoTableBuilderOptions,
    reader: Reader<T, T>,
    properties_schema: SchemaRef,
    shape_type: ShapeType,
}

impl<T: Read + Seek + Send> ShapefileReaderBuilder<T> {
    pub fn try_new(shp_reader: T, dbf_reader: T, options: ShapefileReaderOptions) -> Result<Self> {
        let dbf_reader = dbase::Reader::new(dbf_reader).unwrap();
        let shp_reader = ShapeReader::new(shp_reader).unwrap();

        let header = shp_reader.header();

        let dbf_fields = dbf_reader.fields().to_vec();
        let properties_schema = infer_schema(&dbf_fields);
        let shape_type = header.shape_type;

        let features_count = dbf_reader.header().num_records as usize;
        let features_count = if features_count > 0 {
            Some(features_count)
        } else {
            None
        };

        let array_metadata = options
            .crs
            .map(ArrayMetadata::from_unknown_crs_type)
            .unwrap_or_default();

        let table_builder_options = GeoTableBuilderOptions::new(
            options.coord_type,
            true,
            options.batch_size,
            Some(properties_schema.clone()),
            features_count,
            Arc::new(array_metadata),
        );

        let reader = Reader::new(shp_reader, dbf_reader);

        Ok(Self {
            dbf_fields,
            options: table_builder_options,
            reader,
            properties_schema,
            shape_type,
        })
    }

    fn geometry_type(&self) -> Result<NativeType> {
        let coord_type = self.options.coord_type;
        match self.shape_type {
            ShapeType::Point => Ok(NativeType::Point(coord_type, Dimension::XY)),
            ShapeType::PointZ => Ok(NativeType::Point(coord_type, Dimension::XYZ)),
            ShapeType::Multipoint => Ok(NativeType::MultiPoint(coord_type, Dimension::XY)),
            ShapeType::MultipointZ => Ok(NativeType::MultiPoint(coord_type, Dimension::XYZ)),
            ShapeType::Polyline => Ok(NativeType::MultiLineString(coord_type, Dimension::XY)),
            ShapeType::PolylineZ => Ok(NativeType::MultiLineString(coord_type, Dimension::XYZ)),
            ShapeType::Polygon => Ok(NativeType::MultiPolygon(coord_type, Dimension::XY)),
            ShapeType::PolygonZ => Ok(NativeType::MultiPolygon(coord_type, Dimension::XYZ)),
            t => Err(GeoArrowError::General(format!(
                "Unsupported shapefile geometry type: {}",
                t
            ))),
        }
    }

    fn geometry_field(&self) -> Result<FieldRef> {
        Ok(Arc::new(self.geometry_type()?.to_field_with_metadata(
            "geometry",
            true,
            &self.options.metadata,
        )))
    }

    fn schema(&self) -> Result<SchemaRef> {
        let mut fields = self.properties_schema.fields().to_vec();
        fields.push(self.geometry_field()?);
        Ok(Arc::new(Schema::new_with_metadata(
            fields,
            self.properties_schema.metadata().clone(),
        )))
    }

    /// Create a [`RecordBatchReader`] from this Shapefile
    pub fn read<'a>(&'a mut self) -> Result<Box<dyn RecordBatchReader + Send + 'a>> {
        let schema = self.schema()?;
        let reader: Box<dyn RecordBatchReader + Send + 'a> = match self.shape_type {
            ShapeType::Point => Box::new(PointReader {
                iter: self
                    .reader
                    .iter_shapes_and_records_as::<shapefile::Point, dbase::Record>(),
                options: self.options.clone(),
                schema,
                dbf_fields: &self.dbf_fields,
            }),
            ShapeType::PointZ => Box::new(PointZReader {
                iter: self
                    .reader
                    .iter_shapes_and_records_as::<shapefile::PointZ, dbase::Record>(),
                options: self.options.clone(),
                schema,
                dbf_fields: &self.dbf_fields,
            }),
            ShapeType::Multipoint => Box::new(MultipointReader {
                iter: self
                    .reader
                    .iter_shapes_and_records_as::<shapefile::Multipoint, dbase::Record>(),
                options: self.options.clone(),
                schema,
                dbf_fields: &self.dbf_fields,
            }),
            ShapeType::MultipointZ => Box::new(MultipointZReader {
                iter: self
                    .reader
                    .iter_shapes_and_records_as::<shapefile::MultipointZ, dbase::Record>(),
                options: self.options.clone(),
                schema,
                dbf_fields: &self.dbf_fields,
            }),
            ShapeType::Polyline => Box::new(PolylineReader {
                iter: self
                    .reader
                    .iter_shapes_and_records_as::<shapefile::Polyline, dbase::Record>(),
                options: self.options.clone(),
                schema,
                dbf_fields: &self.dbf_fields,
            }),
            ShapeType::PolylineZ => Box::new(PolylineZReader {
                iter: self
                    .reader
                    .iter_shapes_and_records_as::<shapefile::PolylineZ, dbase::Record>(),
                options: self.options.clone(),
                schema,
                dbf_fields: &self.dbf_fields,
            }),
            ShapeType::Polygon => Box::new(PolygonReader {
                iter: self
                    .reader
                    .iter_shapes_and_records_as::<shapefile::Polygon, dbase::Record>(),
                options: self.options.clone(),
                schema,
                dbf_fields: &self.dbf_fields,
            }),
            ShapeType::PolygonZ => Box::new(PolygonZReader {
                iter: self
                    .reader
                    .iter_shapes_and_records_as::<shapefile::PolygonZ, dbase::Record>(),
                options: self.options.clone(),
                schema,
                dbf_fields: &self.dbf_fields,
            }),
            t => {
                return Err(GeoArrowError::General(format!(
                    "Unsupported shapefile geometry type: {}",
                    t
                )))
            }
        };
        Ok(reader)
    }
}

/// Point Reader is infallible when pushing points with `push_point`
macro_rules! impl_point_reader {
    ($reader_name:ident, $shapefile_ty:ty, $builder:ty, $dim:expr, $scalar_ty:ty, $push_func:ident) => {
        struct $reader_name<'a, T: Read + Seek> {
            iter: ShapeRecordIterator<'a, T, T, $shapefile_ty, Record>,
            schema: SchemaRef,
            options: GeoTableBuilderOptions,
            dbf_fields: &'a [FieldInfo],
        }

        impl<T: Read + Seek> $reader_name<'_, T> {
            fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
                let mut builder =
                    GeoTableBuilder::<$builder>::new_with_options($dim, self.options.clone());

                let mut row_count = 0;
                loop {
                    if row_count >= self.options.batch_size {
                        let (batches, _schema) = builder.finish()?.into_inner();
                        assert_eq!(batches.len(), 1);
                        return Ok(Some(batches.into_iter().next().unwrap()));
                    }

                    if let Some(feature) = self.iter.next() {
                        let (geom, record) = feature.unwrap();

                        // Process properties
                        let prop_builder = builder.properties_builder_mut();
                        prop_builder.add_record(record, self.dbf_fields)?;

                        // Hack to advance internal row number
                        builder.properties_end()?;

                        let geom = <$scalar_ty>::new(&geom);
                        builder.geom_builder().$push_func(Some(&geom));

                        // Hack to advance internal row number
                        builder.feature_end(0)?;

                        row_count += 1;
                    } else {
                        return Ok(None);
                    }
                }
            }
        }

        impl<T: Read + Seek> Iterator for $reader_name<'_, T> {
            type Item = std::result::Result<RecordBatch, ArrowError>;

            fn next(&mut self) -> Option<Self::Item> {
                self.next_batch()
                    .map_err(|err| ArrowError::ExternalError(Box::new(err)))
                    .transpose()
            }
        }

        impl<T: Read + Seek> RecordBatchReader for $reader_name<'_, T> {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }
        }
    };
}

impl_point_reader!(
    PointReader,
    shapefile::Point,
    PointBuilder,
    Dimension::XY,
    super::scalar::Point,
    push_point
);
impl_point_reader!(
    PointZReader,
    shapefile::PointZ,
    PointBuilder,
    Dimension::XYZ,
    super::scalar::PointZ,
    push_point
);

macro_rules! impl_multipoint_polyline_reader {
    ($reader_name:ident, $shapefile_ty:ty, $builder:ty, $dim:expr, $scalar_ty:ty, $push_func:ident) => {
        struct $reader_name<'a, T: Read + Seek> {
            iter: ShapeRecordIterator<'a, T, T, $shapefile_ty, Record>,
            schema: SchemaRef,
            options: GeoTableBuilderOptions,
            dbf_fields: &'a [FieldInfo],
        }

        impl<T: Read + Seek> $reader_name<'_, T> {
            fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
                let mut builder =
                    GeoTableBuilder::<$builder>::new_with_options($dim, self.options.clone());

                let mut row_count = 0;
                loop {
                    if row_count >= self.options.batch_size {
                        let (batches, _schema) = builder.finish()?.into_inner();
                        assert_eq!(batches.len(), 1);
                        return Ok(Some(batches.into_iter().next().unwrap()));
                    }

                    if let Some(feature) = self.iter.next() {
                        let (geom, record) = feature.unwrap();

                        // Process properties
                        let prop_builder = builder.properties_builder_mut();
                        prop_builder.add_record(record, self.dbf_fields)?;

                        // Hack to advance internal row number
                        builder.properties_end()?;

                        let geom = <$scalar_ty>::new(&geom);
                        builder.geom_builder().$push_func(Some(&geom))?;

                        // Hack to advance internal row number
                        builder.feature_end(0)?;

                        row_count += 1;
                    } else {
                        return Ok(None);
                    }
                }
            }
        }

        impl<T: Read + Seek> Iterator for $reader_name<'_, T> {
            type Item = std::result::Result<RecordBatch, ArrowError>;

            fn next(&mut self) -> Option<Self::Item> {
                self.next_batch()
                    .map_err(|err| ArrowError::ExternalError(Box::new(err)))
                    .transpose()
            }
        }

        impl<T: Read + Seek> RecordBatchReader for $reader_name<'_, T> {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }
        }
    };
}

impl_multipoint_polyline_reader!(
    MultipointReader,
    shapefile::Multipoint,
    MultiPointBuilder,
    Dimension::XY,
    super::scalar::MultiPoint,
    push_multi_point
);
impl_multipoint_polyline_reader!(
    MultipointZReader,
    shapefile::MultipointZ,
    MultiPointBuilder,
    Dimension::XYZ,
    super::scalar::MultiPointZ,
    push_multi_point
);
impl_multipoint_polyline_reader!(
    PolylineReader,
    shapefile::Polyline,
    MultiLineStringBuilder,
    Dimension::XY,
    super::scalar::Polyline,
    push_multi_line_string
);
impl_multipoint_polyline_reader!(
    PolylineZReader,
    shapefile::PolylineZ,
    MultiLineStringBuilder,
    Dimension::XYZ,
    super::scalar::PolylineZ,
    push_multi_line_string
);

/// Polygon Reader takes `geom` by value in
/// `super::scalar::MultiPolygon::new`
macro_rules! impl_polygon_reader {
    ($reader_name:ident, $shapefile_ty:ty, $builder:ty, $dim:expr, $scalar_ty:ty, $push_func:ident) => {
        struct $reader_name<'a, T: Read + Seek> {
            iter: ShapeRecordIterator<'a, T, T, $shapefile_ty, Record>,
            schema: SchemaRef,
            options: GeoTableBuilderOptions,
            dbf_fields: &'a [FieldInfo],
        }

        impl<T: Read + Seek> $reader_name<'_, T> {
            fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
                let mut builder =
                    GeoTableBuilder::<$builder>::new_with_options($dim, self.options.clone());

                let mut row_count = 0;
                loop {
                    if row_count >= self.options.batch_size {
                        let (batches, _schema) = builder.finish()?.into_inner();
                        assert_eq!(batches.len(), 1);
                        return Ok(Some(batches.into_iter().next().unwrap()));
                    }

                    if let Some(feature) = self.iter.next() {
                        let (geom, record) = feature.unwrap();

                        // Process properties
                        let prop_builder = builder.properties_builder_mut();
                        prop_builder.add_record(record, self.dbf_fields)?;

                        // Hack to advance internal row number
                        builder.properties_end()?;

                        let geom = <$scalar_ty>::new(geom);
                        builder.geom_builder().$push_func(Some(&geom))?;

                        // Hack to advance internal row number
                        builder.feature_end(0)?;

                        row_count += 1;
                    } else {
                        return Ok(None);
                    }
                }
            }
        }

        impl<T: Read + Seek> Iterator for $reader_name<'_, T> {
            type Item = std::result::Result<RecordBatch, ArrowError>;

            fn next(&mut self) -> Option<Self::Item> {
                self.next_batch()
                    .map_err(|err| ArrowError::ExternalError(Box::new(err)))
                    .transpose()
            }
        }

        impl<T: Read + Seek> RecordBatchReader for $reader_name<'_, T> {
            fn schema(&self) -> SchemaRef {
                self.schema.clone()
            }
        }
    };
}

impl_polygon_reader!(
    PolygonReader,
    shapefile::Polygon,
    MultiPolygonBuilder,
    Dimension::XY,
    super::scalar::MultiPolygon,
    push_multi_polygon
);
impl_polygon_reader!(
    PolygonZReader,
    shapefile::PolygonZ,
    MultiPolygonBuilder,
    Dimension::XYZ,
    super::scalar::MultiPolygonZ,
    push_multi_polygon
);

// // TODO:
// // stretch goal: return a record batch reader.
// /// Read a Shapefile into a [Table].
// pub fn read_shapefile<T: Read + Seek>(
//     shp_reader: T,
//     dbf_reader: T,
//     options: ShapefileReaderOptions,
// ) -> Result<Table> {
//     let dbf_reader = dbase::Reader::new(dbf_reader).unwrap();
//     let shp_reader = ShapeReader::new(shp_reader).unwrap();

//     let header = shp_reader.header();

//     let dbf_fields = dbf_reader.fields().to_vec();
//     let schema = infer_schema(&dbf_fields);
//     let geometry_type = header.shape_type;

//     let features_count = dbf_reader.header().num_records as usize;
//     let features_count = if features_count > 0 {
//         Some(features_count)
//     } else {
//         None
//     };

//     let array_metadata = options
//         .crs
//         .map(ArrayMetadata::from_unknown_crs_type)
//         .unwrap_or_default();

//     let table_builder_options = GeoTableBuilderOptions::new(
//         options.coord_type,
//         true,
//         options.batch_size,
//         Some(schema),
//         features_count,
//         Arc::new(array_metadata),
//     );

//     let mut reader = Reader::new(shp_reader, dbf_reader);

//     // TODO: these might work in a macro

//     match geometry_type {
//         ShapeType::Point => {
//             let mut builder = GeoTableBuilder::<PointBuilder>::new_with_options(
//                 Dimension::XY,
//                 table_builder_options,
//             );

//             for geom_and_record in
//                 reader.iter_shapes_and_records_as::<shapefile::Point, dbase::Record>()
//             {
//                 let (geom, record) = geom_and_record.unwrap();

//                 // Process properties
//                 let prop_builder = builder.properties_builder_mut();
//                 prop_builder.add_record(record, &dbf_fields)?;

//                 // Hack to advance internal row number
//                 builder.properties_end()?;

//                 let geom = super::scalar::Point::new(&geom);
//                 builder.geom_builder().push_point(Some(&geom));

//                 // Hack to advance internal row number
//                 builder.feature_end(0)?;
//             }
//             builder.finish()
//         }
//         ShapeType::PointZ => {
//             let mut builder = GeoTableBuilder::<PointBuilder>::new_with_options(
//                 Dimension::XYZ,
//                 table_builder_options,
//             );

//             for geom_and_record in
//                 reader.iter_shapes_and_records_as::<shapefile::PointZ, dbase::Record>()
//             {
//                 let (geom, record) = geom_and_record.unwrap();

//                 // Process properties
//                 let prop_builder = builder.properties_builder_mut();
//                 prop_builder.add_record(record, &dbf_fields)?;

//                 // Hack to advance internal row number
//                 builder.properties_end()?;

//                 let geom = super::scalar::PointZ::new(&geom);
//                 builder.geom_builder().push_point(Some(&geom));

//                 // Hack to advance internal row number
//                 builder.feature_end(0)?;
//             }
//             builder.finish()
//         }
//         ShapeType::Multipoint => {
//             let mut builder = GeoTableBuilder::<MultiPointBuilder>::new_with_options(
//                 Dimension::XY,
//                 table_builder_options,
//             );

//             for geom_and_record in
//                 reader.iter_shapes_and_records_as::<shapefile::Multipoint, dbase::Record>()
//             {
//                 let (geom, record) = geom_and_record.unwrap();

//                 // Process properties
//                 let prop_builder = builder.properties_builder_mut();
//                 prop_builder.add_record(record, &dbf_fields)?;

//                 // Hack to advance internal row number
//                 builder.properties_end()?;

//                 let geom = super::scalar::MultiPoint::new(&geom);
//                 builder.geom_builder().push_multi_point(Some(&geom))?;

//                 // Hack to advance internal row number
//                 builder.feature_end(0)?;
//             }
//             builder.finish()
//         }
//         ShapeType::MultipointZ => {
//             let mut builder = GeoTableBuilder::<MultiPointBuilder>::new_with_options(
//                 Dimension::XYZ,
//                 table_builder_options,
//             );

//             for geom_and_record in
//                 reader.iter_shapes_and_records_as::<shapefile::MultipointZ, dbase::Record>()
//             {
//                 let (geom, record) = geom_and_record.unwrap();

//                 // Process properties
//                 let prop_builder = builder.properties_builder_mut();
//                 prop_builder.add_record(record, &dbf_fields)?;

//                 // Hack to advance internal row number
//                 builder.properties_end()?;

//                 let geom = super::scalar::MultiPointZ::new(&geom);
//                 builder.geom_builder().push_multi_point(Some(&geom))?;

//                 // Hack to advance internal row number
//                 builder.feature_end(0)?;
//             }
//             builder.finish()
//         }
//         ShapeType::Polyline => {
//             let mut builder = GeoTableBuilder::<MultiLineStringBuilder>::new_with_options(
//                 Dimension::XY,
//                 table_builder_options,
//             );

//             for geom_and_record in
//                 reader.iter_shapes_and_records_as::<shapefile::Polyline, dbase::Record>()
//             {
//                 let (geom, record) = geom_and_record.unwrap();

//                 // Process properties
//                 let prop_builder = builder.properties_builder_mut();
//                 prop_builder.add_record(record, &dbf_fields)?;

//                 // Hack to advance internal row number
//                 builder.properties_end()?;

//                 let geom = super::scalar::Polyline::new(&geom);
//                 builder.geom_builder().push_multi_line_string(Some(&geom))?;

//                 // Hack to advance internal row number
//                 builder.feature_end(0)?;
//             }
//             builder.finish()
//         }
//         ShapeType::PolylineZ => {
//             let mut builder = GeoTableBuilder::<MultiLineStringBuilder>::new_with_options(
//                 Dimension::XYZ,
//                 table_builder_options,
//             );

//             for geom_and_record in
//                 reader.iter_shapes_and_records_as::<shapefile::PolylineZ, dbase::Record>()
//             {
//                 let (geom, record) = geom_and_record.unwrap();

//                 // Process properties
//                 let prop_builder = builder.properties_builder_mut();
//                 prop_builder.add_record(record, &dbf_fields)?;

//                 // Hack to advance internal row number
//                 builder.properties_end()?;

//                 let geom = super::scalar::PolylineZ::new(&geom);
//                 builder.geom_builder().push_multi_line_string(Some(&geom))?;

//                 // Hack to advance internal row number
//                 builder.feature_end(0)?;
//             }
//             builder.finish()
//         }
//         ShapeType::Polygon => {
//             let mut builder = GeoTableBuilder::<MultiPolygonBuilder>::new_with_options(
//                 Dimension::XY,
//                 table_builder_options,
//             );

//             for geom_and_record in
//                 reader.iter_shapes_and_records_as::<shapefile::Polygon, dbase::Record>()
//             {
//                 let (geom, record) = geom_and_record.unwrap();

//                 // Process properties
//                 let prop_builder = builder.properties_builder_mut();
//                 prop_builder.add_record(record, &dbf_fields)?;

//                 // Hack to advance internal row number
//                 builder.properties_end()?;

//                 let geom = super::scalar::MultiPolygon::new(geom);
//                 builder.geom_builder().push_multi_polygon(Some(&geom))?;

//                 // Hack to advance internal row number
//                 builder.feature_end(0)?;
//             }
//             builder.finish()
//         }
//         ShapeType::PolygonZ => {
//             let mut builder = GeoTableBuilder::<MultiPolygonBuilder>::new_with_options(
//                 Dimension::XYZ,
//                 table_builder_options,
//             );

//             for geom_and_record in
//                 reader.iter_shapes_and_records_as::<shapefile::PolygonZ, dbase::Record>()
//             {
//                 let (geom, record) = geom_and_record.unwrap();

//                 // Process properties
//                 let prop_builder = builder.properties_builder_mut();
//                 prop_builder.add_record(record, &dbf_fields)?;

//                 // Hack to advance internal row number
//                 builder.properties_end()?;

//                 let geom = super::scalar::MultiPolygonZ::new(geom);
//                 builder.geom_builder().push_multi_polygon(Some(&geom))?;

//                 // Hack to advance internal row number
//                 builder.feature_end(0)?;
//             }
//             builder.finish()
//         }
//         t => Err(GeoArrowError::General(format!(
//             "Unsupported shapefile geometry type: {}",
//             t
//         ))),
//     }
// }

impl PropertiesBatchBuilder {
    fn add_record(&mut self, record: Record, fields: &[FieldInfo]) -> Result<()> {
        for field_info in fields {
            let field_name = field_info.name();
            let builder = self
                .columns
                .get_mut(field_name)
                .ok_or(GeoArrowError::General(format!(
                    "Builder with name {} does not exist",
                    field_name
                )))?;
            let field_value = record
                .get(field_name)
                .ok_or(GeoArrowError::General(format!(
                    "Field value with name {} does not exist",
                    field_name
                )))?;
            builder.add_field_value(field_value)?;
        }

        Ok(())
    }
}

impl AnyBuilder {
    fn add_field_value(&mut self, value: &FieldValue) -> Result<()> {
        match value {
            FieldValue::Character(v) => {
                if let Some(v) = v {
                    self.as_string_mut().unwrap().append_value(v);
                } else {
                    self.append_null();
                }
            }
            FieldValue::Currency(v) | FieldValue::Double(v) => {
                self.as_float64_mut().unwrap().append_value(*v);
            }
            FieldValue::Date(v) => {
                if let Some(v) = v {
                    let unix_days = v.to_unix_days();
                    self.as_date32_mut().unwrap().append_value(unix_days);
                } else {
                    self.append_null();
                }
            }
            FieldValue::DateTime(v) => {
                let unix_timestamp_s = v.to_unix_timestamp();
                // seconds to microseconds
                let unix_timestamp_us = unix_timestamp_s * 1000 * 1000;
                self.as_date_time_mut()
                    .unwrap()
                    .0
                    .append_value(unix_timestamp_us);
            }
            FieldValue::Float(v) => {
                if let Some(v) = v {
                    self.as_float32_mut().unwrap().append_value(*v);
                } else {
                    self.append_null();
                }
            }
            FieldValue::Integer(v) => {
                self.as_int32_mut().unwrap().append_value(*v);
            }
            FieldValue::Logical(v) => {
                if let Some(v) = v {
                    self.as_bool_mut().unwrap().append_value(*v);
                } else {
                    self.append_null();
                }
            }
            FieldValue::Memo(v) => {
                self.as_string_mut().unwrap().append_value(v);
            }
            FieldValue::Numeric(v) => {
                if let Some(v) = v {
                    self.as_float64_mut().unwrap().append_value(*v);
                } else {
                    self.append_null();
                }
            }
        };
        Ok(())
    }
}

fn infer_schema(fields: &[FieldInfo]) -> SchemaRef {
    let mut out_fields = Vec::with_capacity(fields.len());

    for field in fields {
        let name = field.name().to_string();
        let field = match field.field_type() {
            FieldType::Numeric | FieldType::Double | FieldType::Currency => {
                Field::new(name, DataType::Float64, true)
            }
            FieldType::Character | FieldType::Memo => Field::new(name, DataType::Utf8, true),
            FieldType::Float => Field::new(name, DataType::Float32, true),
            FieldType::Integer => Field::new(name, DataType::Int32, true),
            FieldType::Logical => Field::new(name, DataType::Boolean, true),
            FieldType::Date => Field::new(name, DataType::Date32, true),
            FieldType::DateTime => Field::new(
                name,
                // The dbase DateTime only stores data at second precision, but we currently build
                // millisecond arrays, because that's our existing code path
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                true,
            ),
        };
        out_fields.push(Arc::new(field));
    }

    Arc::new(Schema::new(out_fields))
}
