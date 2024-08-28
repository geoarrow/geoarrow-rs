//! Parse an Arrow record batch given GeoParquet metadata

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};

use crate::array::{
    from_arrow_array, CoordType, LineStringArray, MultiLineStringArray, MultiPointArray,
    MultiPolygonArray, PointArray, PolygonArray, WKBArray,
};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::io::parquet::metadata::{
    infer_geo_data_type, GeoParquetColumnEncoding, GeoParquetColumnMetadata,
    GeoParquetGeometryType, GeoParquetMetadata,
};
use crate::io::wkb::from_wkb;
use crate::GeometryArrayTrait;

pub fn infer_target_schema(
    existing_schema: &Schema,
    geo_meta: &GeoParquetMetadata,
    coord_type: CoordType,
) -> Result<SchemaRef> {
    let mut new_fields: Vec<FieldRef> = Vec::with_capacity(existing_schema.fields().len());
    for existing_field in existing_schema.fields() {
        if let Some(column_meta) = geo_meta.columns.get(existing_field.name()) {
            new_fields.push(infer_target_field(existing_field, column_meta, coord_type)?)
        } else {
            new_fields.push(existing_field.clone());
        }
    }

    Ok(Arc::new(Schema::new_with_metadata(
        new_fields,
        existing_schema.metadata().clone(),
    )))
}

fn infer_target_field(
    existing_field: &Field,
    column_meta: &GeoParquetColumnMetadata,
    coord_type: CoordType,
) -> Result<FieldRef> {
    let target_geo_data_type: GeoDataType = match column_meta.encoding {
        GeoParquetColumnEncoding::WKB => {
            infer_target_wkb_type(&column_meta.geometry_types, coord_type)?
        }
        GeoParquetColumnEncoding::Point => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::PointZ)
            {
                GeoDataType::Point(CoordType::Separated, Dimension::XYZ)
            } else {
                GeoDataType::Point(CoordType::Separated, Dimension::XY)
            }
        }
        GeoParquetColumnEncoding::LineString => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::LineStringZ)
            {
                GeoDataType::LineString(CoordType::Separated, Dimension::XYZ)
            } else {
                GeoDataType::LineString(CoordType::Separated, Dimension::XY)
            }
        }
        GeoParquetColumnEncoding::Polygon => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::LineStringZ)
            {
                GeoDataType::Polygon(CoordType::Separated, Dimension::XYZ)
            } else {
                GeoDataType::Polygon(CoordType::Separated, Dimension::XY)
            }
        }
        GeoParquetColumnEncoding::MultiPoint => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::PointZ)
                || column_meta
                    .geometry_types
                    .contains(&GeoParquetGeometryType::MultiPointZ)
            {
                GeoDataType::MultiPoint(CoordType::Separated, Dimension::XYZ)
            } else {
                GeoDataType::MultiPoint(CoordType::Separated, Dimension::XY)
            }
        }
        GeoParquetColumnEncoding::MultiLineString => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::LineStringZ)
                || column_meta
                    .geometry_types
                    .contains(&GeoParquetGeometryType::MultiLineStringZ)
            {
                GeoDataType::MultiLineString(CoordType::Separated, Dimension::XYZ)
            } else {
                GeoDataType::MultiLineString(CoordType::Separated, Dimension::XY)
            }
        }
        GeoParquetColumnEncoding::MultiPolygon => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::PolygonZ)
                || column_meta
                    .geometry_types
                    .contains(&GeoParquetGeometryType::MultiPolygonZ)
            {
                GeoDataType::MultiPolygon(CoordType::Separated, Dimension::XYZ)
            } else {
                GeoDataType::MultiPolygon(CoordType::Separated, Dimension::XY)
            }
        }
    };
    Ok(Arc::new(target_geo_data_type.to_field_with_metadata(
        existing_field.name(),
        existing_field.is_nullable(),
        &column_meta.into(),
    )))
}

fn infer_target_wkb_type(
    geometry_types: &HashSet<GeoParquetGeometryType>,
    coord_type: CoordType,
) -> Result<GeoDataType> {
    Ok(infer_geo_data_type(geometry_types, coord_type)?
        .unwrap_or(GeoDataType::Mixed(coord_type, Dimension::XY)))
}

/// Parse a record batch to a GeoArrow record batch.
pub fn parse_record_batch(batch: RecordBatch, target_schema: SchemaRef) -> Result<RecordBatch> {
    let orig_columns = batch.columns().to_vec();
    let mut output_columns = Vec::with_capacity(orig_columns.len());

    for ((orig_field, target_field), column) in batch
        .schema_ref()
        .fields()
        .iter()
        .zip(target_schema.fields())
        .zip(orig_columns)
    {
        // Invariant: the target schema has the same column ordering as the original, just that
        // some fields are desired to be parsed.
        assert_eq!(orig_field.name(), target_field.name());

        if orig_field.data_type() != target_field.data_type()
            || orig_field.metadata() != target_field.metadata()
        {
            let output_column = parse_array(column, orig_field, target_field)?;
            output_columns.push(output_column);
        } else {
            output_columns.push(column);
        }
    }

    Ok(RecordBatch::try_new(target_schema, output_columns)?)
}

/// Parse a single column based on provided GeoParquet metadata and target field
fn parse_array(
    array: ArrayRef,
    orig_field: &Field,
    target_field: &Field,
) -> Result<Arc<dyn Array>> {
    use GeoDataType::*;
    let geo_arr = from_arrow_array(array.as_ref(), orig_field)?;
    let target_geo_data_type: GeoDataType = target_field.try_into()?;
    let arr = array.as_ref();

    use Dimension::*;
    match geo_arr.data_type() {
        WKB | LargeWKB => parse_wkb_column(arr, target_geo_data_type),
        Point(_, XY) => parse_point_column::<2>(arr),
        LineString(_, XY) | LargeLineString(_, XY) => parse_line_string_column::<2>(arr),
        Polygon(_, XY) | LargePolygon(_, XY) => parse_polygon_column::<2>(arr),
        MultiPoint(_, XY) | LargeMultiPoint(_, XY) => parse_multi_point_column::<2>(arr),
        MultiLineString(_, XY) | LargeMultiLineString(_, XY) => {
            parse_multi_line_string_column::<2>(arr)
        }
        MultiPolygon(_, XY) | LargeMultiPolygon(_, XY) => parse_multi_polygon_column::<2>(arr),
        Point(_, XYZ) => parse_point_column::<3>(arr),
        LineString(_, XYZ) | LargeLineString(_, XYZ) => parse_line_string_column::<3>(arr),
        Polygon(_, XYZ) | LargePolygon(_, XYZ) => parse_polygon_column::<3>(arr),
        MultiPoint(_, XYZ) | LargeMultiPoint(_, XYZ) => parse_multi_point_column::<3>(arr),
        MultiLineString(_, XYZ) | LargeMultiLineString(_, XYZ) => {
            parse_multi_line_string_column::<3>(arr)
        }
        MultiPolygon(_, XYZ) | LargeMultiPolygon(_, XYZ) => parse_multi_polygon_column::<3>(arr),
        other => Err(GeoArrowError::General(format!(
            "Unexpected geometry encoding: {:?}",
            other
        ))),
    }
}

fn parse_wkb_column(arr: &dyn Array, target_geo_data_type: GeoDataType) -> Result<Arc<dyn Array>> {
    match arr.data_type() {
        DataType::Binary => {
            let wkb_arr = WKBArray::<i32>::try_from(arr)?;
            let geom_arr = from_wkb(&wkb_arr, target_geo_data_type, true)?;
            Ok(geom_arr.to_array_ref())
        }
        DataType::LargeBinary => {
            let wkb_arr = WKBArray::<i64>::try_from(arr)?;
            let geom_arr = from_wkb(&wkb_arr, target_geo_data_type, true)?;
            Ok(geom_arr.to_array_ref())
        }
        dt => Err(GeoArrowError::General(format!(
            "Expected WKB array to have binary data type, got {}",
            dt
        ))),
    }
}

fn parse_point_column<const D: usize>(array: &dyn Array) -> Result<Arc<dyn Array>> {
    let geom_arr: PointArray<D> = array.try_into()?;
    Ok(geom_arr.into_array_ref())
}

macro_rules! impl_parse_fn {
    ($fn_name:ident, $small_geoarrow_type:ty, $large_geoarrow_type:ty) => {
        fn $fn_name<const D: usize>(array: &dyn Array) -> Result<Arc<dyn Array>> {
            match array.data_type() {
                DataType::List(_) => {
                    let geom_arr: $small_geoarrow_type = array.try_into()?;
                    Ok(geom_arr.into_array_ref())
                }
                DataType::LargeList(_) => {
                    let geom_arr: $large_geoarrow_type = array.try_into()?;
                    Ok(geom_arr.into_array_ref())
                }
                dt => Err(GeoArrowError::General(format!(
                    "Unexpected Arrow data type: {}",
                    dt
                ))),
            }
        }
    };
}

impl_parse_fn!(
    parse_line_string_column,
    LineStringArray<i32, D>,
    LineStringArray<i64, D>
);
impl_parse_fn!(parse_polygon_column, PolygonArray<i32, D>, PolygonArray<i64, D>);
impl_parse_fn!(
    parse_multi_point_column,
    MultiPointArray<i32, D>,
    MultiPointArray<i64, D>
);
impl_parse_fn!(
    parse_multi_line_string_column,
    MultiLineStringArray<i32, D>,
    MultiLineStringArray<i64, D>
);
impl_parse_fn!(
    parse_multi_polygon_column,
    MultiPolygonArray<i32, D>,
    MultiPolygonArray<i64, D>
);
