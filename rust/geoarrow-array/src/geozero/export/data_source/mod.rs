mod record_batch_reader;

pub use record_batch_reader::GeozeroRecordBatchReader;

use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::timezone::Tz;
use arrow_array::types::*;
use arrow_array::{Array, RecordBatch};
use arrow_json::writer::make_encoder;
use arrow_schema::{DataType, Schema, TimeUnit};
use geozero::error::GeozeroError;
use geozero::{ColumnValue, FeatureProcessor, GeomProcessor, GeozeroDatasource, PropertyProcessor};

use crate::GeoArrowArray;
use crate::array::from_arrow_array;
use crate::cast::AsGeoArrowArray;
use crate::datatypes::GeoArrowType;
use crate::geozero::export::scalar::{
    process_geometry, process_geometry_collection, process_line_string, process_multi_line_string,
    process_multi_point, process_multi_polygon, process_point, process_polygon, process_rect,
};
use crate::trait_::ArrayAccessor;

impl GeozeroDatasource for GeozeroRecordBatchReader {
    fn process<P: FeatureProcessor>(&mut self, processor: &mut P) -> Result<(), GeozeroError> {
        let reader = self.as_mut();
        let schema = reader.schema();
        let geom_indices = geometry_columns(&schema);
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
            DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Map(_, _) => {
                // TODO(Perf): refactor so that we don't make a new encoder on every row
                let options = Default::default();
                let mut enc = make_encoder(field, array, &options)
                    .map_err(|err| GeozeroError::Property(err.to_string()))?;
                let mut out = vec![];
                enc.encode(within_batch_row_idx, &mut out);
                let json_string = String::from_utf8(out)
                    .map_err(|err| GeozeroError::Property(err.to_string()))?;
                processor.property(property_idx, name, &ColumnValue::Json(&json_string))?;
            }
            DataType::Date32 => {
                let arr = array.as_primitive::<Date32Type>();
                let datetime = arr.value_as_datetime(within_batch_row_idx).unwrap();
                let dt_str = datetime.and_utc().to_rfc3339();
                processor.property(property_idx, name, &ColumnValue::DateTime(&dt_str))?;
            }
            DataType::Date64 => {
                let arr = array.as_primitive::<Date64Type>();
                let datetime = arr.value_as_datetime(within_batch_row_idx).unwrap();
                let dt_str = datetime.and_utc().to_rfc3339();
                processor.property(property_idx, name, &ColumnValue::DateTime(&dt_str))?;
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

                match unit {
                    TimeUnit::Microsecond => impl_timestamp!(TimestampMicrosecondType),
                    TimeUnit::Millisecond => impl_timestamp!(TimestampMillisecondType),
                    TimeUnit::Nanosecond => impl_timestamp!(TimestampNanosecondType),
                    TimeUnit::Second => impl_timestamp!(TimestampSecondType),
                }
            }
            dt => todo!("unsupported type: {:?}", dt),
        }
        property_idx += 1;
    }

    Ok(())
}

fn process_geometry_n<P: GeomProcessor>(
    geometry_column: &Arc<dyn GeoArrowArray>,
    within_batch_row_idx: usize,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    let arr = geometry_column.as_ref();
    let i = within_batch_row_idx;

    use GeoArrowType::*;
    // TODO: should we be passing the geom_idx down into these process* functions?
    match arr.data_type() {
        Point(_) => {
            let geom = arr.as_point().value(i).unwrap();
            process_point(&geom, 0, processor)?;
        }
        LineString(_) => {
            let geom = arr.as_line_string().value(i).unwrap();
            process_line_string(&geom, 0, processor)?;
        }
        Polygon(_) => {
            let geom = arr.as_polygon().value(i).unwrap();
            process_polygon(&geom, true, 0, processor)?;
        }
        MultiPoint(_) => {
            let geom = arr.as_multi_point().value(i).unwrap();
            process_multi_point(&geom, 0, processor)?;
        }
        MultiLineString(_) => {
            let geom = arr.as_multi_line_string().value(i).unwrap();
            process_multi_line_string(&geom, 0, processor)?;
        }
        MultiPolygon(_) => {
            let geom = arr.as_multi_polygon().value(i).unwrap();
            process_multi_polygon(&geom, 0, processor)?;
        }
        GeometryCollection(_) => {
            let geom = arr.as_geometry_collection().value(i).unwrap();
            process_geometry_collection(&geom, 0, processor)?;
        }
        Wkb(_) => {
            let geom = arr
                .as_wkb::<i32>()
                .value(i)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(&geom, 0, processor)?;
        }
        LargeWkb(_) => {
            let geom = arr
                .as_wkb::<i64>()
                .value(i)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(&geom, 0, processor)?;
        }
        Wkt(_) => {
            let geom = arr
                .as_wkt::<i32>()
                .value(i)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(&geom, 0, processor)?;
        }
        LargeWkt(_) => {
            let geom = arr
                .as_wkt::<i64>()
                .value(i)
                .map_err(|err| GeozeroError::Geometry(err.to_string()))?;
            process_geometry(&geom, 0, processor)?;
        }
        Rect(_) => {
            let geom = arr.as_rect().value(i).unwrap();
            process_rect(&geom, 0, processor)?;
        }
        Geometry(_) => {
            let geom = arr.as_geometry().value(i).unwrap();
            process_geometry(&geom, 0, processor)?;
        }
    }

    Ok(())
}

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
