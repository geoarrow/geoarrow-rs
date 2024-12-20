use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::Field;

use crate::algorithm::native::bounding_rect::BoundingRect;
use crate::algorithm::native::TotalBounds;
use crate::array::{CoordType, NativeArrayDyn};
use crate::error::Result;
use crate::io::parquet::metadata::GeoParquetColumnEncoding;
use crate::io::parquet::writer::metadata::{ColumnInfo, GeoParquetMetadataBuilder};
use crate::io::wkb::ToWKB;
use crate::{ArrayBase, NativeArray};

pub(super) fn encode_record_batch(
    batch: &RecordBatch,
    metadata_builder: &mut GeoParquetMetadataBuilder,
) -> Result<RecordBatch> {
    let mut new_columns = batch.columns().to_vec();
    for (column_idx, column_info) in metadata_builder.columns.iter_mut() {
        let array = batch.column(*column_idx);
        let field = batch.schema_ref().field(*column_idx);
        column_info.update_geometry_types(array, field)?;

        let (encoded_column, array_bounds) = encode_column(array, field, column_info)?;
        new_columns[*column_idx] = encoded_column;

        column_info.update_bbox(&array_bounds);
    }

    Ok(RecordBatch::try_new(
        metadata_builder.output_schema.clone(),
        new_columns,
    )?)
}

fn encode_column(
    array: &dyn Array,
    field: &Field,
    column_info: &mut ColumnInfo,
) -> Result<(ArrayRef, BoundingRect)> {
    let geo_arr = NativeArrayDyn::from_arrow_array(array, field)?.into_inner();
    let array_bounds = geo_arr.as_ref().total_bounds();
    let encoded_array = match column_info.encoding {
        GeoParquetColumnEncoding::WKB => encode_wkb_column(geo_arr.as_ref())?,
        _ => encode_native_column(geo_arr.as_ref())?,
    };
    Ok((encoded_array, array_bounds))
}

/// Encode column as WKB
fn encode_wkb_column(geo_arr: &dyn NativeArray) -> Result<ArrayRef> {
    Ok(geo_arr.as_ref().to_wkb::<i32>().to_array_ref())
}

/// Encode column as GeoArrow.
///
/// Note that the GeoParquet specification requires separated coord type!
fn encode_native_column(geo_arr: &dyn NativeArray) -> Result<ArrayRef> {
    Ok(geo_arr.to_coord_type(CoordType::Separated).to_array_ref())
}
