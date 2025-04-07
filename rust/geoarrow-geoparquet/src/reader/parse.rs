//! Parse an Arrow record batch given GeoParquet metadata

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use geoarrow_array::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray, WKBArray,
};
use geoarrow_array::{GeoArrowArray, GeoArrowType};
use geoarrow_schema::{
    CoordType, Dimension, GeometryType, LineStringType, Metadata, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType,
};

use crate::io::wkb::from_wkb;
use crate::metadata::{
    infer_geo_data_type, GeoParquetColumnEncoding, GeoParquetColumnMetadata,
    GeoParquetGeometryType, GeoParquetMetadata,
};
use geoarrow_array::error::{GeoArrowError, Result};

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

/// For native encodings we always load to the separated encoding so that we don't need an extra
/// copy.
fn infer_target_field(
    existing_field: &Field,
    column_meta: &GeoParquetColumnMetadata,
    coord_type: CoordType,
) -> Result<FieldRef> {
    let metadata = Arc::new(Metadata::from(column_meta.clone()));

    let target_geo_data_type: GeoArrowType = match column_meta.encoding {
        GeoParquetColumnEncoding::WKB => {
            infer_target_wkb_type(&column_meta.geometry_types, coord_type)?
        }
        GeoParquetColumnEncoding::Point => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::PointZ)
            {
                GeoArrowType::Point(PointType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::Point(PointType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
            }
        }
        GeoParquetColumnEncoding::LineString => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::LineStringZ)
            {
                GeoArrowType::LineString(LineStringType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::LineString(LineStringType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
            }
        }
        GeoParquetColumnEncoding::Polygon => {
            if column_meta
                .geometry_types
                .contains(&GeoParquetGeometryType::LineStringZ)
            {
                GeoArrowType::Polygon(PolygonType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::Polygon(PolygonType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
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
                GeoArrowType::MultiPoint(MultiPointType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::MultiPoint(MultiPointType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
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
                GeoArrowType::MultiLineString(MultiLineStringType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::MultiLineString(MultiLineStringType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
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
                GeoArrowType::MultiPolygon(MultiPolygonType::new(
                    CoordType::Separated,
                    Dimension::XYZ,
                    metadata,
                ))
            } else {
                GeoArrowType::MultiPolygon(MultiPolygonType::new(
                    CoordType::Separated,
                    Dimension::XY,
                    metadata,
                ))
            }
        }
    };

    Ok(Arc::new(target_geo_data_type.to_field(
        existing_field.name(),
        existing_field.is_nullable(),
    )))
}

fn infer_target_wkb_type(
    geometry_types: &HashSet<GeoParquetGeometryType>,
    coord_type: CoordType,
) -> Result<GeoArrowType> {
    Ok(
        infer_geo_data_type(geometry_types, coord_type)?.unwrap_or(GeoArrowType::Geometry(
            GeometryType::new(coord_type, Default::default()),
        )),
    )
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
fn parse_array(array: ArrayRef, orig_field: &Field, target_field: &Field) -> Result<ArrayRef> {
    let orig_type = GeoArrowType::try_from(orig_field)?;
    let arr = array.as_ref();
    match orig_type {
        GeoArrowType::Point(t) => parse_point_column(arr, t.dimension()),
        GeoArrowType::LineString(t) => parse_line_string_column(arr, t.dimension()),
        GeoArrowType::Polygon(t) => parse_polygon_column(arr, t.dimension()),
        GeoArrowType::MultiPoint(t) => parse_multi_point_column(arr, t.dimension()),
        GeoArrowType::MultiLineString(t) => parse_multi_line_string_column(arr, t.dimension()),
        GeoArrowType::MultiPolygon(t) => parse_multi_polygon_column(arr, t.dimension()),
        GeoArrowType::WKB(_) | GeoArrowType::LargeWKB(_) => {
            parse_wkb_column(arr, target_field.try_into()?)
        }
        GeoArrowType::WKT(_) | GeoArrowType::LargeWKT(_) => Err(GeoArrowError::General(
            "WKT input not supported in GeoParquet.".to_string(),
        )),
        other => Err(GeoArrowError::General(format!(
            "Unexpected geometry encoding: {:?}",
            other
        ))),
    }
}

fn parse_wkb_column(arr: &dyn Array, target_geo_data_type: GeoArrowType) -> Result<ArrayRef> {
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

fn parse_point_column(array: &dyn Array, dim: Dimension) -> Result<ArrayRef> {
    let geom_arr: PointArray = (array, dim).try_into()?;
    Ok(geom_arr.into_array_ref())
}

macro_rules! impl_parse_fn {
    ($fn_name:ident, $geoarrow_type:ty) => {
        fn $fn_name(array: &dyn Array, dim: Dimension) -> Result<ArrayRef> {
            match array.data_type() {
                DataType::List(_) | DataType::LargeList(_) => {
                    let geom_arr: $geoarrow_type = (array, dim).try_into()?;
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

impl_parse_fn!(parse_line_string_column, LineStringArray);
impl_parse_fn!(parse_polygon_column, PolygonArray);
impl_parse_fn!(parse_multi_point_column, MultiPointArray);
impl_parse_fn!(parse_multi_line_string_column, MultiLineStringArray);
impl_parse_fn!(parse_multi_polygon_column, MultiPolygonArray);
