use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
use arrow_schema::{DataType, Field, SchemaBuilder, SchemaRef, TimeUnit};
use flatgeobuf::{ColumnType, Crs, GeometryType, Header};
use geoarrow_schema::{
    CoordType, Dimension, LineStringType, Metadata, MultiLineStringType, MultiPointType,
    MultiPolygonType, PointType, PolygonType,
};

use crate::datatypes::NativeType;
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
            coord_type: CoordType::Interleaved,
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
                metadata.insert(
                    EXTENSION_TYPE_NAME_KEY.to_string(),
                    "arrow.json".to_string(),
                );
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

/// Parse CRS information provided by FlatGeobuf into a [Metadata].
///
/// WKT is preferred if it exists. Otherwise, authority code will be used as a fallback.
pub(super) fn parse_crs(crs: Option<Crs<'_>>) -> Arc<Metadata> {
    if let Some(crs) = crs {
        if let Some(wkt) = crs.wkt() {
            // We use unknown CRS because we don't know for sure it's WKT 2019
            let crs = geoarrow_schema::Crs::from_unknown_crs_type(wkt.to_string());
            return Arc::new(Metadata::new(crs, None));
        }

        if let Some(org) = crs.org() {
            let code = crs.code();
            if code != 0 {
                let crs = geoarrow_schema::Crs::from_authority_code(format!("{org}:{code}"));
                return Arc::new(Metadata::new(crs, None));
            }

            if let Some(code) = crs.code_string() {
                let crs = geoarrow_schema::Crs::from_authority_code(format!("{org}:{code}"));
                return Arc::new(Metadata::new(crs, None));
            }
        };
    };

    Default::default()
}

pub(super) fn infer_from_header(header: Header<'_>) -> Result<(NativeType, SchemaRef)> {
    use Dimension::*;

    if header.has_m() | header.has_t() | header.has_tm() {
        return Err(GeoArrowError::General(
            "Only XY and XYZ dimensions are supported".to_string(),
        ));
    }
    let has_z = header.has_z();

    let properties_schema = infer_schema(header);
    let geometry_type = header.geometry_type();
    let metadata = parse_crs(header.crs());
    // TODO: pass through arg
    let coord_type = CoordType::Interleaved;
    let data_type = match (geometry_type, has_z) {
        (GeometryType::Point, false) => NativeType::Point(PointType::new(coord_type, XY, metadata)),
        (GeometryType::LineString, false) => {
            NativeType::LineString(LineStringType::new(coord_type, XY).with_metadata(metadata))
        }
        (GeometryType::Polygon, false) => {
            NativeType::Polygon(PolygonType::new(coord_type, XY).with_metadata(metadata))
        }
        (GeometryType::MultiPoint, false) => {
            NativeType::MultiPoint(MultiPointType::new(coord_type, XY).with_metadata(metadata))
        }
        (GeometryType::MultiLineString, false) => NativeType::MultiLineString(
            MultiLineStringType::new(coord_type, XY).with_metadata(metadata),
        ),
        (GeometryType::MultiPolygon, false) => {
            NativeType::MultiPolygon(MultiPolygonType::new(coord_type, XY).with_metadata(metadata))
        }
        (GeometryType::Point, true) => {
            NativeType::Point(PointType::new(coord_type, XYZ).with_metadata(metadata))
        }
        (GeometryType::LineString, true) => {
            NativeType::LineString(LineStringType::new(coord_type, XYZ).with_metadata(metadata))
        }
        (GeometryType::Polygon, true) => {
            NativeType::Polygon(PolygonType::new(coord_type, XYZ).with_metadata(metadata))
        }
        (GeometryType::MultiPoint, true) => {
            NativeType::MultiPoint(MultiPointType::new(coord_type, XYZ).with_metadata(metadata))
        }
        (GeometryType::MultiLineString, true) => NativeType::MultiLineString(
            MultiLineStringType::new(coord_type, XYZ).with_metadata(metadata),
        ),
        (GeometryType::MultiPolygon, true) => {
            NativeType::MultiPolygon(MultiPolygonType::new(coord_type, XYZ).with_metadata(metadata))
        }
        (GeometryType::Unknown, _) => NativeType::Geometry(
            geoarrow_schema::GeometryType::new(coord_type).with_metadata(metadata),
        ),
        _ => panic!("Unsupported type"),
    };
    Ok((data_type, properties_schema))
}
