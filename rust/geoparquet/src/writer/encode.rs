use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::Field;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::{AsGeoArrowArray, to_wkb};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, GeoArrowType};

use crate::metadata::GeoParquetColumnEncoding;
use crate::total_bounds::{BoundingRect, total_bounds};
use crate::writer::metadata::{ColumnInfo, GeoParquetMetadataBuilder};

pub(super) fn encode_record_batch(
    batch: &RecordBatch,
    metadata_builder: &mut GeoParquetMetadataBuilder,
) -> GeoArrowResult<RecordBatch> {
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
) -> GeoArrowResult<(ArrayRef, BoundingRect)> {
    let geo_arr = from_arrow_array(array, field)?;
    let array_bounds = total_bounds(geo_arr.as_ref())?;
    let encoded_array = match column_info.encoding {
        GeoParquetColumnEncoding::WKB => encode_wkb_column(geo_arr.as_ref())?,
        _ => encode_native_column(geo_arr.as_ref()),
    };
    Ok((encoded_array, array_bounds))
}

/// Encode column as WKB
fn encode_wkb_column(geo_arr: &dyn GeoArrowArray) -> GeoArrowResult<ArrayRef> {
    Ok(to_wkb::<i32>(geo_arr)?.to_array_ref())
}

/// Encode column as GeoArrow.
///
/// Note that the GeoParquet specification requires separated coord type!
fn encode_native_column(geo_arr: &dyn GeoArrowArray) -> ArrayRef {
    macro_rules! impl_into_coord_type {
        ($cast_func:ident) => {
            geo_arr
                .$cast_func()
                .clone()
                .into_coord_type(CoordType::Separated)
                .to_array_ref()
        };
    }
    match geo_arr.data_type() {
        GeoArrowType::Point(_) => impl_into_coord_type!(as_point),
        GeoArrowType::LineString(_) => impl_into_coord_type!(as_line_string),
        GeoArrowType::Polygon(_) => impl_into_coord_type!(as_polygon),
        GeoArrowType::MultiPoint(_) => impl_into_coord_type!(as_multi_point),
        GeoArrowType::MultiLineString(_) => impl_into_coord_type!(as_multi_line_string),
        GeoArrowType::MultiPolygon(_) => impl_into_coord_type!(as_multi_polygon),
        GeoArrowType::Geometry(_) => impl_into_coord_type!(as_geometry),
        GeoArrowType::GeometryCollection(_) => impl_into_coord_type!(as_geometry_collection),
        _ => geo_arr.to_array_ref(),
    }
}
