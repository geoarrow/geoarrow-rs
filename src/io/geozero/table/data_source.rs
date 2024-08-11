#![allow(deprecated)]

use std::str::FromStr;
use std::sync::Arc;

use crate::array::{from_arrow_array, AsGeometryArray};
use crate::datatypes::{Dimension, GeoDataType};
use crate::io::geozero::scalar::process_geometry;
use crate::io::geozero::table::json_encoder::{make_encoder, EncoderOptions};
use crate::io::stream::RecordBatchReader;
use crate::schema::GeoSchemaExt;
use crate::table::Table;
use crate::trait_::{GeometryArrayAccessor, GeometryScalarTrait};
use crate::GeometryArrayTrait;
use arrow::array::AsArray;
use arrow::datatypes::*;
use arrow_array::timezone::Tz;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema};
use geozero::error::GeozeroError;
use geozero::{ColumnValue, FeatureProcessor, GeomProcessor, GeozeroDatasource, PropertyProcessor};

impl GeozeroDatasource for RecordBatchReader {
    fn process<P: FeatureProcessor>(&mut self, processor: &mut P) -> Result<(), GeozeroError> {
        let reader = self.take().ok_or(GeozeroError::Dataset(
            "Cannot read from closed RecordBatchReader".to_string(),
        ))?;
        let schema = reader.schema();
        let geom_indices = schema.as_ref().geometry_columns();
        let geometry_column_index = if geom_indices.len() != 1 {
            Err(GeozeroError::Dataset(
                "Writing through geozero not supported with multiple geometries".to_string(),
            ))?
        } else {
            geom_indices[0]
        };

        processor.dataset_begin(None)?;

        let mut overall_row_idx = 0;
        for batch in reader.into_iter() {
            let batch = batch.map_err(|err| GeozeroError::Dataset(err.to_string()))?;
            process_batch(
                &batch,
                &schema,
                geometry_column_index,
                overall_row_idx,
                processor,
            )?;
            overall_row_idx += batch.num_rows();
        }

        processor.dataset_end()?;

        Ok(())
    }
}

impl GeozeroDatasource for Table {
    fn process<P: FeatureProcessor>(&mut self, processor: &mut P) -> Result<(), GeozeroError> {
        process_geotable(self, processor)
    }
}

fn process_geotable<P: FeatureProcessor>(
    table: &mut Table,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    let schema = table.schema();
    let batches = table.batches();
    let geometry_column_index = table.default_geometry_column_idx().map_err(|_err| {
        GeozeroError::Dataset(
            "Writing through geozero not supported with multiple geometries".to_string(),
        )
    })?;

    processor.dataset_begin(None)?;

    let mut overall_row_idx = 0;
    for batch in batches {
        process_batch(
            batch,
            schema,
            geometry_column_index,
            overall_row_idx,
            processor,
        )?;
        overall_row_idx += batch.num_rows();
    }

    processor.dataset_end()?;

    Ok(())
}

fn process_batch<P: FeatureProcessor>(
    batch: &RecordBatch,
    schema: &Schema,
    geometry_column_index: usize,
    batch_start_idx: usize,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    let num_rows = batch.num_rows();
    let geometry_field = schema.field(geometry_column_index);
    let geometry_column_box = &batch.columns()[geometry_column_index];
    let geometry_column = from_arrow_array(&geometry_column_box, geometry_field)
        .map_err(|err| GeozeroError::Dataset(err.to_string()))?;

    for within_batch_row_idx in 0..num_rows {
        processor.feature_begin((within_batch_row_idx + batch_start_idx) as u64)?;

        processor.properties_begin()?;
        process_properties(
            batch,
            schema,
            within_batch_row_idx,
            geometry_column_index,
            processor,
        )?;
        processor.properties_end()?;

        processor.geometry_begin()?;
        process_geometry_n(&geometry_column, within_batch_row_idx, processor)?;
        processor.geometry_end()?;

        processor.feature_end((within_batch_row_idx + batch_start_idx) as u64)?;
    }

    Ok(())
}

fn process_properties<P: PropertyProcessor>(
    batch: &RecordBatch,
    schema: &Schema,
    within_batch_row_idx: usize,
    geometry_column_index: usize,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    // Note: the `column_idx` will be off by one if the geometry column is not the last column in
    // the table, so we maintain a separate property index counter
    let mut property_idx = 0;
    for (column_idx, (field, array)) in schema.fields.iter().zip(batch.columns().iter()).enumerate()
    {
        // Don't include geometry column in properties
        if column_idx == geometry_column_index {
            continue;
        }
        let name = field.name();

        // Don't pass null properties to geozero
        if array.is_null(within_batch_row_idx) {
            continue;
        }

        match field.data_type() {
            DataType::Boolean => {
                let arr = array.as_boolean();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Bool(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::UInt8 => {
                let arr = array.as_primitive::<UInt8Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::UByte(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int8 => {
                let arr = array.as_primitive::<Int8Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Byte(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::UInt16 => {
                let arr = array.as_primitive::<UInt16Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::UShort(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int16 => {
                let arr = array.as_primitive::<Int16Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Short(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::UInt32 => {
                let arr = array.as_primitive::<UInt32Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::UInt(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int32 => {
                let arr = array.as_primitive::<Int32Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Int(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::UInt64 => {
                let arr = array.as_primitive::<UInt64Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::ULong(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int64 => {
                let arr = array.as_primitive::<Int64Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Long(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Float16 => {
                let arr = array.as_primitive::<Float16Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Float(arr.value(within_batch_row_idx).to_f32()),
                )?;
            }
            DataType::Float32 => {
                let arr = array.as_primitive::<Float32Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Float(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Float64 => {
                let arr = array.as_primitive::<Float64Type>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Double(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Utf8 => {
                let arr = array.as_string::<i32>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::String(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::LargeUtf8 => {
                let arr = array.as_string::<i64>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::String(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Binary => {
                let arr = array.as_binary::<i32>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Binary(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::LargeBinary => {
                let arr = array.as_binary::<i64>();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Binary(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) => {
                if array.is_valid(within_batch_row_idx) {
                    let mut encoder = make_encoder(
                        array,
                        &EncoderOptions {
                            explicit_nulls: false,
                        },
                    )
                    .map_err(|err| GeozeroError::Property(err.to_string()))?;
                    let mut buf = vec![];
                    encoder.encode(within_batch_row_idx, &mut buf);
                    let json_string = String::from_utf8(buf)
                        .map_err(|err| GeozeroError::Property(err.to_string()))?;
                    processor.property(property_idx, name, &ColumnValue::Json(&json_string))?;
                }
            }
            DataType::Date32 => {
                let arr = array.as_primitive::<Date32Type>();
                if arr.is_valid(within_batch_row_idx) {
                    let datetime = arr.value_as_datetime(within_batch_row_idx).unwrap();
                    let dt_str = datetime.and_utc().to_rfc3339();
                    processor.property(property_idx, name, &ColumnValue::DateTime(&dt_str))?;
                }
            }
            DataType::Date64 => {
                let arr = array.as_primitive::<Date64Type>();
                if arr.is_valid(within_batch_row_idx) {
                    let datetime = arr.value_as_datetime(within_batch_row_idx).unwrap();
                    let dt_str = datetime.and_utc().to_rfc3339();
                    processor.property(property_idx, name, &ColumnValue::DateTime(&dt_str))?;
                }
            }
            DataType::Timestamp(unit, tz) => {
                let arrow_tz = if let Some(tz) = tz {
                    Some(Tz::from_str(tz).map_err(|err| GeozeroError::Property(err.to_string()))?)
                } else {
                    None
                };

                macro_rules! impl_timestamp {
                    ($arrow_type:ty) => {{
                        let arr = array.as_primitive::<$arrow_type>();
                        let dt_str = if let Some(arrow_tz) = arrow_tz {
                            arr.value_as_datetime_with_tz(within_batch_row_idx, arrow_tz)
                                .unwrap()
                                .to_rfc3339()
                        } else {
                            arr.value_as_datetime(within_batch_row_idx)
                                .unwrap()
                                .and_utc()
                                .to_rfc3339()
                        };
                        processor.property(property_idx, name, &ColumnValue::DateTime(&dt_str))?;
                    }};
                }

                if array.is_valid(within_batch_row_idx) {
                    match unit {
                        TimeUnit::Microsecond => impl_timestamp!(TimestampMicrosecondType),
                        TimeUnit::Millisecond => impl_timestamp!(TimestampMillisecondType),
                        TimeUnit::Nanosecond => impl_timestamp!(TimestampNanosecondType),
                        TimeUnit::Second => impl_timestamp!(TimestampSecondType),
                    }
                }
            }
            dt => todo!("unsupported type: {:?}", dt),
        }
        property_idx += 1;
    }

    Ok(())
}

fn process_geometry_n<P: GeomProcessor>(
    geometry_column: &Arc<dyn GeometryArrayTrait>,
    within_batch_row_idx: usize,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    let geom = get_geo_geometry(geometry_column, within_batch_row_idx);
    // I think this index is 0 because it's not a multi-geometry?
    process_geometry(&geom, 0, processor)?;
    Ok(())
}

// TODO: don't go through geo geometry; keep as geoarrow geom.
fn get_geo_geometry(arr: &Arc<dyn GeometryArrayTrait>, i: usize) -> geo::Geometry {
    match arr.data_type() {
        GeoDataType::Point(_, Dimension::XY) => {
            arr.as_ref().as_point_2d().value(i).to_geo_geometry()
        }
        GeoDataType::LineString(_, Dimension::XY) => {
            arr.as_ref().as_line_string_2d().value(i).to_geo_geometry()
        }
        GeoDataType::LargeLineString(_, Dimension::XY) => arr
            .as_ref()
            .as_large_line_string_2d()
            .value(i)
            .to_geo_geometry(),
        GeoDataType::Polygon(_, Dimension::XY) => {
            arr.as_ref().as_polygon_2d().value(i).to_geo_geometry()
        }
        GeoDataType::LargePolygon(_, Dimension::XY) => arr
            .as_ref()
            .as_large_polygon_2d()
            .value(i)
            .to_geo_geometry(),
        GeoDataType::MultiPoint(_, Dimension::XY) => {
            arr.as_ref().as_multi_point_2d().value(i).to_geo_geometry()
        }
        GeoDataType::LargeMultiPoint(_, Dimension::XY) => arr
            .as_ref()
            .as_large_multi_point_2d()
            .value(i)
            .to_geo_geometry(),
        GeoDataType::MultiLineString(_, Dimension::XY) => arr
            .as_ref()
            .as_multi_line_string_2d()
            .value(i)
            .to_geo_geometry(),
        GeoDataType::LargeMultiLineString(_, Dimension::XY) => arr
            .as_ref()
            .as_large_multi_line_string_2d()
            .value(i)
            .to_geo_geometry(),
        GeoDataType::MultiPolygon(_, Dimension::XY) => arr
            .as_ref()
            .as_multi_polygon_2d()
            .value(i)
            .to_geo_geometry(),
        GeoDataType::LargeMultiPolygon(_, Dimension::XY) => arr
            .as_ref()
            .as_large_multi_polygon_2d()
            .value(i)
            .to_geo_geometry(),
        GeoDataType::Mixed(_, Dimension::XY) => {
            arr.as_ref().as_mixed_2d().value(i).to_geo_geometry()
        }
        GeoDataType::LargeMixed(_, Dimension::XY) => {
            arr.as_ref().as_large_mixed_2d().value(i).to_geo_geometry()
        }
        GeoDataType::GeometryCollection(_, Dimension::XY) => arr
            .as_ref()
            .as_geometry_collection_2d()
            .value(i)
            .to_geo_geometry(),
        GeoDataType::LargeGeometryCollection(_, Dimension::XY) => arr
            .as_ref()
            .as_large_geometry_collection_2d()
            .value(i)
            .to_geo_geometry(),
        GeoDataType::WKB => arr.as_ref().as_wkb().value(i).to_geo_geometry(),
        GeoDataType::LargeWKB => arr.as_ref().as_large_wkb().value(i).to_geo_geometry(),
        GeoDataType::Rect(Dimension::XY) => arr.as_ref().as_rect_2d().value(i).to_geo_geometry(),
        _ => todo!("3d support"),
    }
}
