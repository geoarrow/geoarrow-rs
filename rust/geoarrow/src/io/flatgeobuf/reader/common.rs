use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, SchemaBuilder, SchemaRef, TimeUnit};
use flatgeobuf::{ColumnType, Crs, GeometryType, Header};
use serde_json::Value;

use crate::array::metadata::{ArrayMetadata, CRSType};
use crate::array::CoordType;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};

/// Options for the FlatGeobuf reader
#[derive(Debug, Clone)]
pub struct FlatGeobufReaderOptions {
    /// The GeoArrow coordinate type to use in the geometry arrays.
    pub coord_type: CoordType,

    /// The number of rows in each batch.
    pub batch_size: Option<usize>,

    /// A spatial filter for reading rows.
    ///
    /// If set to `None`, no spatial filtering will be performed.
    pub bbox: Option<(f64, f64, f64, f64)>,
}

impl Default for FlatGeobufReaderOptions {
    fn default() -> Self {
        Self {
            coord_type: Default::default(),
            batch_size: Some(65_536),
            bbox: None,
        }
    }
}

pub(super) fn infer_schema(header: Header<'_>) -> SchemaRef {
    let columns = header.columns().unwrap();
    let mut schema = SchemaBuilder::with_capacity(columns.len());

    for col in columns.into_iter() {
        let field = match col.type_() {
            ColumnType::Bool => Field::new(col.name(), DataType::Boolean, col.nullable()),
            ColumnType::Byte => Field::new(col.name(), DataType::Int8, col.nullable()),
            ColumnType::UByte => Field::new(col.name(), DataType::UInt8, col.nullable()),
            ColumnType::Short => Field::new(col.name(), DataType::Int16, col.nullable()),
            ColumnType::UShort => Field::new(col.name(), DataType::UInt16, col.nullable()),
            ColumnType::Int => Field::new(col.name(), DataType::Int32, col.nullable()),
            ColumnType::UInt => Field::new(col.name(), DataType::UInt32, col.nullable()),
            ColumnType::Long => Field::new(col.name(), DataType::Int64, col.nullable()),
            ColumnType::ULong => Field::new(col.name(), DataType::UInt64, col.nullable()),
            ColumnType::Float => Field::new(col.name(), DataType::Float32, col.nullable()),
            ColumnType::Double => Field::new(col.name(), DataType::Float64, col.nullable()),
            ColumnType::String => Field::new(col.name(), DataType::Utf8, col.nullable()),
            ColumnType::Json => {
                let mut metadata = HashMap::with_capacity(1);
                metadata.insert("ARROW:extension:name".to_string(), "arrow.json".to_string());
                Field::new(col.name(), DataType::Utf8, col.nullable()).with_metadata(metadata)
            }
            ColumnType::DateTime => Field::new(
                col.name(),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                col.nullable(),
            ),
            ColumnType::Binary => Field::new(col.name(), DataType::Binary, col.nullable()),
            // ColumnType is actually a struct, not an enum, so the rust compiler doesn't know
            // we've matched all types
            _ => unreachable!(),
        };
        schema.push(field);
    }

    Arc::new(schema.finish())
}

/// Parse CRS information provided by FlatGeobuf into an [ArrayMetadata].
///
/// WKT is preferred if it exists. Otherwise, authority code will be used as a fallback.
pub(super) fn parse_crs(crs: Option<Crs<'_>>) -> Arc<ArrayMetadata> {
    if let Some(crs) = crs {
        let mut meta = ArrayMetadata::default();
        if let Some(wkt) = crs.wkt() {
            meta.crs = Some(Value::String(wkt.to_string()));
            return Arc::new(meta);
        }

        if let Some(org) = crs.org() {
            let code = crs.code();
            if code != 0 {
                meta.crs = Some(Value::String(format!("{org}:{code}")));
                meta.crs_type = Some(CRSType::AuthorityCode);
                return Arc::new(meta);
            }

            if let Some(code) = crs.code_string() {
                meta.crs = Some(Value::String(format!("{org}:{code}")));
                meta.crs_type = Some(CRSType::AuthorityCode);
                return Arc::new(meta);
            }
        };
    };

    Default::default()
}

pub(super) fn infer_from_header(
    header: Header<'_>,
) -> Result<(NativeType, SchemaRef, Arc<ArrayMetadata>)> {
    use Dimension::*;

    if header.has_m() | header.has_t() | header.has_tm() {
        return Err(GeoArrowError::General(
            "Only XY and XYZ dimensions are supported".to_string(),
        ));
    }
    let has_z = header.has_z();

    let properties_schema = infer_schema(header);
    let geometry_type = header.geometry_type();
    let array_metadata = parse_crs(header.crs());
    // TODO: pass through arg
    let coord_type = CoordType::Interleaved;
    let data_type = match (geometry_type, has_z) {
        (GeometryType::Point, false) => NativeType::Point(coord_type, XY),
        (GeometryType::LineString, false) => NativeType::LineString(coord_type, XY),
        (GeometryType::Polygon, false) => NativeType::Polygon(coord_type, XY),
        (GeometryType::MultiPoint, false) => NativeType::MultiPoint(coord_type, XY),
        (GeometryType::MultiLineString, false) => NativeType::MultiLineString(coord_type, XY),
        (GeometryType::MultiPolygon, false) => NativeType::MultiPolygon(coord_type, XY),
        (GeometryType::Point, true) => NativeType::Point(coord_type, XYZ),
        (GeometryType::LineString, true) => NativeType::LineString(coord_type, XYZ),
        (GeometryType::Polygon, true) => NativeType::Polygon(coord_type, XYZ),
        (GeometryType::MultiPoint, true) => NativeType::MultiPoint(coord_type, XYZ),
        (GeometryType::MultiLineString, true) => NativeType::MultiLineString(coord_type, XYZ),
        (GeometryType::MultiPolygon, true) => NativeType::MultiPolygon(coord_type, XYZ),
        (GeometryType::Unknown, _) => NativeType::Geometry(coord_type),
        _ => panic!("Unsupported type"),
    };
    Ok((data_type, properties_schema, array_metadata))
}
