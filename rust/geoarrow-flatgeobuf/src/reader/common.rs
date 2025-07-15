use std::sync::Arc;

use arrow_schema::{DataType, Field, SchemaBuilder, SchemaRef, TimeUnit};
use flatgeobuf::{ColumnType, Crs, Header};
use geoarrow_schema::{
    CoordType, Dimension, GeometryCollectionType, GeometryType, LineStringType, Metadata,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
};

use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

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

/// Parse the FlatGeobuf header to infer the [SchemaRef] of the property columns.
///
/// Note that this does not include the geometry column, which is handled separately.
pub(super) fn infer_properties_schema(header: Header<'_>, prefer_view_types: bool) -> SchemaRef {
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
            ColumnType::String => {
                let data_type = if prefer_view_types {
                    DataType::Utf8View
                } else {
                    DataType::Utf8
                };
                Field::new(col.name(), data_type, col.nullable())
            }
            ColumnType::Json => {
                let data_type = if prefer_view_types {
                    DataType::Utf8View
                } else {
                    DataType::Utf8
                };
                Field::new(col.name(), data_type, col.nullable())
                    .with_extension_type(arrow_schema::extension::Json::default())
            }
            ColumnType::DateTime => Field::new(
                col.name(),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                col.nullable(),
            ),
            ColumnType::Binary => {
                let data_type = if prefer_view_types {
                    DataType::BinaryView
                } else {
                    DataType::Binary
                };
                Field::new(col.name(), data_type, col.nullable())
            }
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

/// Parse the FlatGeobuf header to infer the [GeoArrowType] of the geometry column and [SchemaRef]
/// of the properties.
pub(super) fn parse_header(
    header: Header<'_>,
    coord_type: CoordType,
    prefer_view_types: bool,
) -> GeoArrowResult<(GeoArrowType, SchemaRef)> {
    if header.has_t() | header.has_tm() {
        return Err(GeoArrowError::General(
            "FlatGeobuf t dimension is not supported".to_string(),
        ));
    }
    let dim = match (header.has_z(), header.has_m()) {
        (false, false) => Dimension::XY,
        (true, false) => Dimension::XYZ,
        (false, true) => Dimension::XYM,
        (true, true) => Dimension::XYZM,
    };

    let properties_schema = infer_properties_schema(header, prefer_view_types);
    let geometry_type = header.geometry_type();
    let metadata = parse_crs(header.crs());

    let data_type = match geometry_type {
        flatgeobuf::GeometryType::Point => PointType::new(dim, metadata)
            .with_coord_type(coord_type)
            .into(),
        flatgeobuf::GeometryType::LineString => LineStringType::new(dim, metadata)
            .with_coord_type(coord_type)
            .into(),
        flatgeobuf::GeometryType::Polygon => PolygonType::new(dim, metadata)
            .with_coord_type(coord_type)
            .into(),
        flatgeobuf::GeometryType::MultiPoint => MultiPointType::new(dim, metadata)
            .with_coord_type(coord_type)
            .into(),
        flatgeobuf::GeometryType::MultiLineString => MultiLineStringType::new(dim, metadata)
            .with_coord_type(coord_type)
            .into(),
        flatgeobuf::GeometryType::MultiPolygon => MultiPolygonType::new(dim, metadata)
            .with_coord_type(coord_type)
            .into(),
        flatgeobuf::GeometryType::GeometryCollection => GeometryCollectionType::new(dim, metadata)
            .with_coord_type(coord_type)
            .into(),
        flatgeobuf::GeometryType::Unknown => GeometryType::new(metadata)
            .with_coord_type(coord_type)
            .into(),
        _ => GeoArrowError::FlatGeobuf(format!(
            "Unsupported FlatGeobuf geometry type: {geometry_type:?}",
        )),
    };
    Ok((data_type, properties_schema))
}
