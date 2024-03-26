use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{Field, Schema};

use crate::algorithm::native::bounding_rect::BoundingRect;
use crate::algorithm::native::TotalBounds;
use crate::array::{from_arrow_array, CoordType};
use crate::datatypes::GeoDataType;
use crate::error::Result;
use crate::io::parquet::metadata::GeoParquetMetadata;
use crate::io::parquet::writer::options::GeoParquetWriterEncoding;
use crate::io::wkb::ToWKB;
use crate::table::GeoTable;
use crate::GeometryArrayTrait;

fn encode_table(
    table: &GeoTable,
    encoding: GeoParquetWriterEncoding,
) -> Result<(Vec<RecordBatch>, GeoParquetMetadata)> {
    let mut column_encodings = HashMap::with_capacity(1);
    column_encodings.insert(table.geometry_column_index(), encoding);

    let mut column_bounds = HashMap::with_capacity(1);
    let new_batches = Vec::with_capacity(table.batches().len());
    for batch in table.batches() {
        let (encoded_batch, array_bounds) =
            encode_record_batch(batch, column_encodings, output_schema)?;
        new_batches.push(encoded_batch);

        // TODO: update column_bounds with array_bounds
    }

    Ok((new_batches, _))
}

// fn create_column_metadata()

fn create_output_schema(table: &GeoTable, encoding: GeoParquetWriterEncoding) {}

fn create_output_field(field: &Field, encoding: GeoParquetWriterEncoding) -> Result<Field> {
    match encoding {
        GeoParquetWriterEncoding::WKB => {
            Ok(GeoDataType::WKB.to_field(field.name(), field.is_nullable()))
        }
        GeoParquetWriterEncoding::Native => {
            let geo_data_type: GeoDataType = field.try_into()?;
            Ok(geo_data_type
                .with_coord_type(CoordType::Separated)
                .to_field(field.name(), field.is_nullable()))
        }
    }
}

// TODO: should the encoding be inferred from the new schema?
fn encode_record_batch(
    batch: &RecordBatch,
    column_encodings: HashMap<usize, GeoParquetWriterEncoding>,
    output_schema: Arc<Schema>,
) -> Result<(RecordBatch, HashMap<usize, BoundingRect>)> {
    let mut new_columns = batch.columns().to_vec();
    let mut array_bounds = HashMap::with_capacity(column_encodings.len());
    for (column_idx, column_encoding) in column_encodings.iter() {
        let array = batch.column(*column_idx);
        let field = batch.schema_ref().field(*column_idx);
        let (encoded_column, array_bounds) = encode_column(array, field, *column_encoding)?;
        new_columns[*column_idx] = encoded_column;
        array_bounds.insert(*column_idx, array_bounds);
    }
    Ok((
        RecordBatch::try_new(output_schema, new_columns)?,
        array_bounds,
    ))
}

fn encode_column(
    array: &dyn Array,
    field: &Field,
    encoding: GeoParquetWriterEncoding,
) -> Result<(Arc<dyn Array>, BoundingRect)> {
    let geo_arr = from_arrow_array(array, field)?;
    let array_bounds = geo_arr.as_ref().total_bounds();
    let encoded_array = match encoding {
        GeoParquetWriterEncoding::Native => encode_native_column(geo_arr.as_ref(), field)?,
        GeoParquetWriterEncoding::WKB => encode_wkb_column(geo_arr.as_ref(), field)?,
    };
    Ok((encoded_array, array_bounds))
}

/// Encode column as WKB
fn encode_wkb_column(geo_arr: &dyn GeometryArrayTrait, field: &Field) -> Result<Arc<dyn Array>> {
    Ok(geo_arr.as_ref().to_wkb::<i32>().to_array_ref())
}

/// Encode column as GeoArrow.
///
/// Note that the GeoParquet specification requires separated coord type!
fn encode_native_column(geo_arr: &dyn GeometryArrayTrait, field: &Field) -> Result<Arc<dyn Array>> {
    Ok(geo_arr.to_coord_type(CoordType::Separated).to_array_ref())
}
