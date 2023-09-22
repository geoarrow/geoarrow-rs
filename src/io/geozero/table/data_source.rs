use crate::io::geozero::scalar::geometry::process_geometry;
use crate::GeometryArrayTrait;
use arrow2::array::{Array, BinaryArray, PrimitiveArray, Utf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Schema};
use arrow2::types::f16;
use geozero::error::GeozeroError;
use geozero::{ColumnValue, FeatureProcessor, GeomProcessor, GeozeroDatasource, PropertyProcessor};

use crate::array::GeometryArray;
use crate::table::GeoTable;

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
        overall_row_idx += batch.len();
    }

    processor.dataset_end()?;

    Ok(())
}

fn process_batch<P: FeatureProcessor>(
    batch: &Chunk<Box<dyn Array>>,
    schema: &Schema,
    geometry_column_index: usize,
    overall_row_idx: usize,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    let num_rows = batch.len();
    let geometry_column_box = &batch.columns()[geometry_column_index];
    let geometry_column: GeometryArray<i32> = (&**geometry_column_box).try_into().unwrap();

    for i in 0..num_rows {
        processor.feature_begin((i + overall_row_idx) as u64)?;

        processor.properties_begin()?;
        process_properties(batch, schema, i, geometry_column_index, processor)?;
        processor.properties_end()?;

        processor.geometry_begin()?;
        process_geometry_n(&geometry_column, i, processor)?;
        processor.geometry_end()?;

        processor.feature_end((i + overall_row_idx) as u64)?;
    }

    Ok(())
}

// TODO: need to check that this is valid row index _within_ a single chunk or across the entire
// table. Not sure when to use which
fn process_properties<P: PropertyProcessor>(
    batch: &Chunk<Box<dyn Array>>,
    schema: &Schema,
    row_idx: usize,
    geometry_column_index: usize,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    // TODO: have to process all columns **except** geometry column
    // processor.property(idx, name, value)

    for (column_idx, (field, array)) in schema.fields.iter().zip(batch.arrays().iter()).enumerate()
    {
        if column_idx == geometry_column_index {
            continue;
        }
        let name = &field.name;

        match field.data_type().to_logical_type() {
            DataType::UInt8 => {
                let arr = array.as_any().downcast_ref::<PrimitiveArray<u8>>().unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::UByte(arr.value(row_idx)))?;
            }
            DataType::Int8 => {
                let arr = array.as_any().downcast_ref::<PrimitiveArray<i8>>().unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::Byte(arr.value(row_idx)))?;
            }
            DataType::UInt16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u16>>()
                    .unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::UShort(arr.value(row_idx)))?;
            }
            DataType::Int16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i16>>()
                    .unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::Short(arr.value(row_idx)))?;
            }
            DataType::UInt32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u32>>()
                    .unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::UInt(arr.value(row_idx)))?;
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::Int(arr.value(row_idx)))?;
            }
            DataType::UInt64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::ULong(arr.value(row_idx)))?;
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::Long(arr.value(row_idx)))?;
            }
            DataType::Float16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<f16>>()
                    .unwrap();
                // TODO: fix to global row idx
                processor.property(
                    row_idx,
                    name,
                    &ColumnValue::Float(arr.value(row_idx).to_f32()),
                )?;
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<f32>>()
                    .unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::Float(arr.value(row_idx)))?;
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<f64>>()
                    .unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::Double(arr.value(row_idx)))?;
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::String(arr.value(row_idx)))?;
            }
            DataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::String(arr.value(row_idx)))?;
            }
            DataType::Binary => {
                let arr = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::Binary(arr.value(row_idx)))?;
            }
            DataType::LargeBinary => {
                let arr = array.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
                // TODO: fix to global row idx
                processor.property(row_idx, name, &ColumnValue::Binary(arr.value(row_idx)))?;
            }
            // geozero type system also supports json and datetime
            _ => todo!("json and datetime types"),
        }
    }

    todo!()
}

// TODO: need to check that this is valid row index _within_ a single chunk or across the entire
// table. Not sure when to use which
fn process_geometry_n<P: GeomProcessor>(
    geometry_column: &GeometryArray<i32>,
    row_idx: usize,
    processor: &mut P,
) -> Result<(), GeozeroError> {
    let geom = geometry_column.value(row_idx);
    process_geometry(&geom, row_idx, processor)?;
    Ok(())
}
