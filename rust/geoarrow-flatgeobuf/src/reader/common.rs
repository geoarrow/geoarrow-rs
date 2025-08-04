use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, SchemaBuilder, SchemaRef, TimeUnit};
use flatgeobuf::{ColumnType, Crs, Header};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{
    CoordType, Dimension, GeoArrowType, GeometryCollectionType, GeometryType, LineStringType,
    Metadata, MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
};

/// Parameters for the FlatGeobuf reader
#[derive(Debug, Clone)]
pub struct FlatGeobufReaderOptions {
    // Schema can either be inferred from the file or scanned.
    // The user can customize view types themeselves
    pub(crate) properties_schema: SchemaRef,

    // User can specify whether to read in to WKB, WKT
    // TODO: allow reading in to GeoArrowArray WKB/WKT
    pub(crate) geometry_type: GeoArrowType,

    /// The number of rows in each batch.
    pub(crate) batch_size: usize,

    /// If `true`, read the geometry column.
    pub(crate) read_geometry: bool,
}

impl FlatGeobufReaderOptions {
    /// Create a new FlatGeobuf reader options.
    ///
    /// The properties schema and geometry type are **required**. Refer to [`FlatGeobufHeaderExt`]
    /// for methods to obtain these from a FlatGeobuf file header.
    pub fn new(properties_schema: SchemaRef, geometry_type: GeoArrowType) -> Self {
        Self {
            batch_size: 1024,
            read_geometry: true,
            properties_schema,
            geometry_type,
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set whether to read the geometry column.
    pub fn with_read_geometry(mut self, read_geometry: bool) -> Self {
        self.read_geometry = read_geometry;
        self
    }

    pub(crate) fn validate_against_header(&self, header: &Header<'_>) -> GeoArrowResult<()> {
        self.validate_properties_schema(header)?;
        self.validate_geometry_type(header)?;
        Ok(())
    }

    fn validate_properties_schema(&self, header: &Header<'_>) -> GeoArrowResult<()> {
        if let Some(columns) = header.columns() {
            let column_map: HashMap<_, _> =
                HashMap::from_iter(columns.iter().map(|col| (col.name(), col)));
            for field in self.properties_schema.fields() {
                let fgb_col =
                    column_map
                        .get(field.name().as_str())
                        .ok_or(GeoArrowError::FlatGeobuf(format!(
                            "Column '{}' provided in schema but not found in FlatGeobuf header",
                            field.name()
                        )))?;

                if fgb_col.nullable() != field.is_nullable() {
                    return Err(GeoArrowError::FlatGeobuf(format!(
                        "Column '{}' provided in schema has nullable = {}, but FlatGeobuf header has nullable = {}",
                        field.name(),
                        field.is_nullable(),
                        fgb_col.nullable()
                    )));
                }

                match (fgb_col.type_(), field.data_type()) {
                    (ColumnType::Bool, DataType::Boolean)
                    | (ColumnType::Byte, DataType::Int8)
                    | (ColumnType::UByte, DataType::UInt8)
                    | (ColumnType::Short, DataType::Int16)
                    | (ColumnType::UShort, DataType::UInt16)
                    | (ColumnType::Int, DataType::Int32)
                    | (ColumnType::UInt, DataType::UInt32)
                    | (ColumnType::Long, DataType::Int64)
                    | (ColumnType::ULong, DataType::UInt64)
                    | (ColumnType::Float, DataType::Float32)
                    | (ColumnType::Double, DataType::Float64)
                    | (
                        ColumnType::String | ColumnType::Json,
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                    )
                    | (
                        ColumnType::Binary,
                        DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                    ) => {}
                    (ColumnType::DateTime, DataType::Timestamp(_, _)) => {}
                    (col_type, data_type) => {
                        return Err(GeoArrowError::FlatGeobuf(format!(
                            "Column '{}' provided in schema has incompatible type: FlatGeobuf column type is '{col_type:?}', but schema has '{data_type:?}'",
                            field.name(),
                        )));
                    }
                };
            }

            Ok(())
        } else {
            // Nothing to validate if the metadata doesn't contain column info
            Ok(())
        }
    }

    fn validate_geometry_type(&self, header: &Header<'_>) -> GeoArrowResult<()> {
        if let Some(expected_dim) = self.geometry_type.dimension() {
            match expected_dim {
                Dimension::XY => {
                    if header.has_z() || header.has_m() {
                        return Err(GeoArrowError::FlatGeobuf(
                            "Expected 2D geometry, but FlatGeobuf has Z or M dimension".to_string(),
                        ));
                    }
                }
                Dimension::XYZ => {
                    if !header.has_z() || header.has_m() {
                        return Err(GeoArrowError::FlatGeobuf(
                            "Expected 3D geometry with Z dimension, but FlatGeobuf does not have Z"
                                .to_string(),
                        ));
                    }
                }
                Dimension::XYM => {
                    if header.has_z() || !header.has_m() {
                        return Err(GeoArrowError::FlatGeobuf(
                            "Expected 3D geometry with M dimension, but FlatGeobuf does not have M"
                                .to_string(),
                        ));
                    }
                }
                Dimension::XYZM => {
                    if !header.has_z() || !header.has_m() {
                        return Err(GeoArrowError::FlatGeobuf(
                            "Expected 4D geometry with Z and M dimensions, but FlatGeobuf does not have both"
                                .to_string(),
                        ));
                    }
                }
            }
        };

        match (&self.geometry_type, header.geometry_type()) {
            (GeoArrowType::Point(_), flatgeobuf::GeometryType::Point)
            | (GeoArrowType::LineString(_), flatgeobuf::GeometryType::LineString)
            | (GeoArrowType::Polygon(_), flatgeobuf::GeometryType::Polygon)
            | (
                GeoArrowType::MultiPoint(_),
                flatgeobuf::GeometryType::Point | flatgeobuf::GeometryType::MultiPoint,
            )
            | (
                GeoArrowType::MultiLineString(_),
                flatgeobuf::GeometryType::LineString | flatgeobuf::GeometryType::MultiLineString,
            )
            | (
                GeoArrowType::MultiPolygon(_),
                flatgeobuf::GeometryType::Polygon | flatgeobuf::GeometryType::MultiPolygon,
            )
            | (GeoArrowType::GeometryCollection(_), flatgeobuf::GeometryType::GeometryCollection)
            | (
                GeoArrowType::Geometry(_) | GeoArrowType::Wkb(_) | GeoArrowType::LargeWkb(_),
                // We don't currently have builders for these types
                // | GeoArrowType::WkbView(_),
                // | GeoArrowType::Wkt(_)
                // | GeoArrowType::LargeWkt(_)
                // | GeoArrowType::WktView(_),
                flatgeobuf::GeometryType::Point
                | flatgeobuf::GeometryType::LineString
                | flatgeobuf::GeometryType::Polygon
                | flatgeobuf::GeometryType::MultiPoint
                | flatgeobuf::GeometryType::MultiLineString
                | flatgeobuf::GeometryType::MultiPolygon
                | flatgeobuf::GeometryType::GeometryCollection
                | flatgeobuf::GeometryType::Unknown,
            ) => Ok(()),
            (expected, actual) => Err(GeoArrowError::FlatGeobuf(format!(
                "FlatGeobuf geometry type '{actual:?}' does not support expected geometry type '{expected:?}'",
            ))),
        }
    }
}

/// Parse CRS information provided by FlatGeobuf into a [Metadata].
///
/// WKT is preferred if it exists. Otherwise, authority code will be used as a fallback.
fn parse_crs(crs: Crs<'_>) -> Arc<Metadata> {
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

    Default::default()
}

/// Extension trait to add convenience methods to FlatGeobuf's [Header].
pub trait FlatGeobufHeaderExt {
    /// Returns the GeoArrow geometry type for the geometry type encoded in the FlatGeobuf header.
    ///
    /// Pass this geometry type to [FlatGeobufReaderOptions::new] when creating reader options.
    fn geoarrow_type(&self, coord_type: CoordType) -> GeoArrowResult<GeoArrowType>;

    /// Returns the schema of the properties columns, if known.
    ///
    /// If the FlatGeobuf file header does not contain information about property columns, this
    /// will be `None`. In that case, you should use
    /// [`FlatGeobufSchemaScanner`][crate::reader::schema::FlatGeobufSchemaScanner] to infer the
    /// schema by scanning the first `N`` features.
    ///
    /// Pass the resulting schema to [FlatGeobufReaderOptions::new] when creating reader options.
    fn properties_schema(&self, prefer_view_types: bool) -> Option<SchemaRef>;
}

impl FlatGeobufHeaderExt for Header<'_> {
    fn geoarrow_type(&self, coord_type: CoordType) -> GeoArrowResult<GeoArrowType> {
        if self.has_t() | self.has_tm() {
            return Err(GeoArrowError::FlatGeobuf(
                "FlatGeobuf t dimension is not supported".to_string(),
            ));
        }

        let fgb_geometry_type = self.geometry_type();
        let metadata = self.crs().map(parse_crs).unwrap_or_default();

        let dim = match (self.has_z(), self.has_m()) {
            (false, false) => Dimension::XY,
            (true, false) => Dimension::XYZ,
            (false, true) => Dimension::XYM,
            (true, true) => Dimension::XYZM,
        };

        let data_type = match fgb_geometry_type {
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
            flatgeobuf::GeometryType::GeometryCollection => {
                GeometryCollectionType::new(dim, metadata)
                    .with_coord_type(coord_type)
                    .into()
            }
            flatgeobuf::GeometryType::Unknown => GeometryType::new(metadata)
                .with_coord_type(coord_type)
                .into(),
            _ => {
                return Err(GeoArrowError::FlatGeobuf(format!(
                    "Unsupported FlatGeobuf geometry type: {fgb_geometry_type:?}",
                )));
            }
        };
        Ok(data_type)
    }

    /// Returns the schema of the properties columns, if known.
    ///
    /// If the FlatGeobuf file header does not contain information about property columns, this
    /// will be `None`.
    fn properties_schema(&self, prefer_view_types: bool) -> Option<SchemaRef> {
        let columns = self.columns()?;
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

        Some(Arc::new(schema.finish()))
    }
}
