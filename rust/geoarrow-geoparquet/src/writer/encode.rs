use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::Field;
use geoarrow_array::array::{WKBArray, from_arrow_array};
use geoarrow_array::builder::WKBBuilder;
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::error::Result;
use geoarrow_array::{ArrayAccessor, GeoArrowArray, GeoArrowType};
use geoarrow_schema::{CoordType, WkbType};

use crate::metadata::GeoParquetColumnEncoding;
use crate::total_bounds::{BoundingRect, total_bounds};
use crate::writer::metadata::{ColumnInfo, GeoParquetMetadataBuilder};

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
    let geo_arr = from_arrow_array(array, field)?;
    let array_bounds = total_bounds(geo_arr.as_ref())?;
    let encoded_array = match column_info.encoding {
        GeoParquetColumnEncoding::WKB => encode_wkb_column(geo_arr.as_ref())?,
        _ => encode_native_column(geo_arr.as_ref())?,
    };
    Ok((encoded_array, array_bounds))
}

/// Encode column as WKB
fn encode_wkb_column(geo_arr: &dyn GeoArrowArray) -> Result<ArrayRef> {
    Ok(to_wkb(geo_arr)?.to_array_ref())
}

/// Encode column as GeoArrow.
///
/// Note that the GeoParquet specification requires separated coord type!
fn encode_native_column(geo_arr: &dyn GeoArrowArray) -> Result<ArrayRef> {
    Ok(geo_arr.to_coord_type(CoordType::Separated).to_array_ref())
}

/// Convert to WKB
fn to_wkb(arr: &dyn GeoArrowArray) -> Result<WKBArray<i32>> {
    use GeoArrowType::*;
    match arr.data_type() {
        Point(_) => impl_to_wkb(arr.as_point()),
        LineString(_) => impl_to_wkb(arr.as_line_string()),
        Polygon(_) => impl_to_wkb(arr.as_polygon()),
        MultiPoint(_) => impl_to_wkb(arr.as_multi_point()),
        MultiLineString(_) => impl_to_wkb(arr.as_multi_line_string()),
        MultiPolygon(_) => impl_to_wkb(arr.as_multi_polygon()),
        Geometry(_) => impl_to_wkb(arr.as_geometry()),
        GeometryCollection(_) => impl_to_wkb(arr.as_geometry_collection()),
        Rect(_) => impl_to_wkb(arr.as_rect()),
        WKB(_) => impl_to_wkb(arr.as_wkb()),
        LargeWKB(_) => impl_to_wkb(arr.as_large_wkb()),
        WKT(_) => todo!(),      // impl_to_wkb(arr.as_wkt()),
        LargeWKT(_) => todo!(), // impl_to_wkb(arr.as_wkt()),
    }
}

fn impl_to_wkb<'a>(geo_arr: &'a impl ArrayAccessor<'a>) -> Result<WKBArray<i32>> {
    // let metadata = geo_arr.metadata().clone();

    let geoms = geo_arr
        .iter()
        .map(|x| x.transpose())
        .collect::<Result<Vec<_>>>()?;
    let wkb_type = WkbType::new(Default::default());
    Ok(WKBBuilder::from_nullable_geometries(geoms.as_slice(), wkb_type).finish())
}
