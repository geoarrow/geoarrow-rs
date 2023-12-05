use crate::array::GeometryArray;
use crate::io::geozero::scalar::geometry::process_geometry;
use crate::table::GeoTable;
use crate::trait_::GeometryArrayAccessor;
use arrow_array::{
    BinaryArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Schema};
use geozero::error::GeozeroError;
use geozero::{ColumnValue, FeatureProcessor, GeomProcessor, GeozeroDatasource, PropertyProcessor};

impl GeozeroDatasource for GeoTable {
    fn process<P: FeatureProcessor>(&mut self, processor: &mut P) -> Result<(), GeozeroError> {
        process_geotable(self, processor)
    }
}

fn process_geotable<P: FeatureProcessor>(
    table: &mut GeoTable,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    let schema = table.schema();
    let batches = table.batches();
    let geometry_column_index = table.geometry_column_index();

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
    let geometry_column: GeometryArray<i32> =
        (geometry_field, &**geometry_column_box).try_into().unwrap();

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

        match field.data_type() {
            DataType::UInt8 => {
                let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::UByte(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Byte(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::UInt16 => {
                let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::UShort(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Short(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::UInt32 => {
                let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::UInt(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Int(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::ULong(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Long(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Float16 => {
                let arr = array.as_any().downcast_ref::<Float16Array>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Float(arr.value(within_batch_row_idx).to_f32()),
                )?;
            }
            DataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Float(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Double(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::String(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::String(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::Binary => {
                let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Binary(arr.value(within_batch_row_idx)),
                )?;
            }
            DataType::LargeBinary => {
                let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                processor.property(
                    property_idx,
                    name,
                    &ColumnValue::Binary(arr.value(within_batch_row_idx)),
                )?;
            }
            // geozero type system also supports json and datetime
            _ => todo!("json and datetime types"),
        }
        property_idx += 1;
    }

    Ok(())
}

fn process_geometry_n<P: GeomProcessor>(
    geometry_column: &GeometryArray<i32>,
    within_batch_row_idx: usize,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    let geom = geometry_column.value(within_batch_row_idx);
    // I think this index is 0 because it's not a multi-geometry?
    process_geometry(&geom, 0, processor)?;
    Ok(())
}
