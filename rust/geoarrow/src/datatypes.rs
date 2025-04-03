//! Contains the implementation of [`NativeType`], which defines all geometry arrays in this
//! crate.

use std::sync::Arc;

use arrow_schema::extension::{
    ExtensionType, EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY,
};
use arrow_schema::{DataType, Field};
use geoarrow_schema::{
    BoxType, CoordType, Dimension, GeometryCollectionType, GeometryType, LineStringType, Metadata,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType,
    WktType,
};

use crate::error::{GeoArrowError, Result};

/// A type enum representing "native" GeoArrow geometry types.
///
/// This is designed to aid in downcasting from dynamically-typed geometry arrays.
///
/// This type uniquely identifies the physical buffer layout of each geometry array type.
/// It must always be possible to accurately downcast from a `dyn &NativeArray` or `dyn
/// &ChunkedNativeArray` to a unique concrete array type using this enum.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NativeType {
    /// Represents a [PointArray][crate::array::PointArray] or
    /// [ChunkedPointArray][crate::chunked_array::ChunkedPointArray].
    Point(PointType),

    /// Represents a [LineStringArray][crate::array::LineStringArray] or
    /// [ChunkedLineStringArray][crate::chunked_array::ChunkedLineStringArray] with `i32` offsets.
    LineString(LineStringType),

    /// Represents a [PolygonArray][crate::array::PolygonArray] or
    /// [ChunkedPolygonArray][crate::chunked_array::ChunkedPolygonArray] with `i32` offsets.
    Polygon(PolygonType),

    /// Represents a [MultiPointArray][crate::array::MultiPointArray] or
    /// [ChunkedMultiPointArray][crate::chunked_array::ChunkedMultiPointArray] with `i32` offsets.
    MultiPoint(MultiPointType),

    /// Represents a [MultiLineStringArray][crate::array::MultiLineStringArray] or
    /// [ChunkedMultiLineStringArray][crate::chunked_array::ChunkedMultiLineStringArray] with `i32`
    /// offsets.
    MultiLineString(MultiLineStringType),

    /// Represents a [MultiPolygonArray][crate::array::MultiPolygonArray] or
    /// [ChunkedMultiPolygonArray][crate::chunked_array::ChunkedMultiPolygonArray] with `i32`
    /// offsets.
    MultiPolygon(MultiPolygonType),

    /// Represents a [GeometryCollectionArray][crate::array::GeometryCollectionArray] or
    /// [ChunkedGeometryCollectionArray][crate::chunked_array::ChunkedGeometryCollectionArray] with
    /// `i32` offsets.
    GeometryCollection(GeometryCollectionType),

    /// Represents a [RectArray][crate::array::RectArray] or
    /// [ChunkedRectArray][crate::chunked_array::ChunkedRectArray].
    Rect(BoxType),

    /// Represents a mixed geometry array of unknown types or dimensions
    Geometry(GeometryType),
}

impl From<NativeType> for DataType {
    fn from(value: NativeType) -> Self {
        value.to_data_type()
    }
}

/// A type enum representing "serialized" GeoArrow geometry types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SerializedType {
    /// Represents a [WKBArray][crate::array::WKBArray] or
    /// [ChunkedWKBArray][crate::chunked_array::ChunkedWKBArray] with `i32` offsets.
    WKB(WkbType),

    /// Represents a [WKBArray][crate::array::WKBArray] or
    /// [ChunkedWKBArray][crate::chunked_array::ChunkedWKBArray] with `i64` offsets.
    LargeWKB(WkbType),

    /// Represents a [WKTArray][crate::array::WKTArray] or
    /// [ChunkedWKTArray][crate::chunked_array::ChunkedWKTArray] with `i32` offsets.
    WKT(WktType),

    /// Represents a [WKTArray][crate::array::WKTArray] or
    /// [ChunkedWKTArray][crate::chunked_array::ChunkedWKTArray] with `i64` offsets.
    LargeWKT(WktType),
}

/// A type enum representing all possible GeoArrow geometry types, including both "native" and
/// "serialized" encodings.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AnyType {
    /// A "native" GeoArrow encoding
    Native(NativeType),

    /// A "serialized" GeoArrow encoding, such as WKB or WKT
    Serialized(SerializedType),
}

impl NativeType {
    /// Get the [`CoordType`] of this data type.
    pub fn coord_type(&self) -> CoordType {
        use NativeType::*;
        match self {
            Point(t) => t.coord_type(),
            LineString(t) => t.coord_type(),
            Polygon(t) => t.coord_type(),
            MultiPoint(t) => t.coord_type(),
            MultiLineString(t) => t.coord_type(),
            MultiPolygon(t) => t.coord_type(),
            GeometryCollection(t) => t.coord_type(),
            Rect(_) => CoordType::Separated,
            Geometry(t) => t.coord_type(),
        }
    }

    /// Get the [`Dimension`] of this data type, if it has one.
    ///
    /// "Unknown" native arrays can hold all dimensions.
    pub fn dimension(&self) -> Option<Dimension> {
        use NativeType::*;
        match self {
            Point(t) => Some(t.dimension()),
            LineString(t) => Some(t.dimension()),
            Polygon(t) => Some(t.dimension()),
            MultiPoint(t) => Some(t.dimension()),
            MultiLineString(t) => Some(t.dimension()),
            MultiPolygon(t) => Some(t.dimension()),
            GeometryCollection(t) => Some(t.dimension()),
            Rect(t) => Some(t.dimension()),
            Geometry(_) => None,
        }
    }

    /// Returns this geodata type with the provided [Metadata].
    pub fn metadata(&self) -> &Arc<Metadata> {
        use NativeType::*;
        match self {
            Point(t) => t.metadata(),
            LineString(t) => t.metadata(),
            Polygon(t) => t.metadata(),
            MultiPoint(t) => t.metadata(),
            MultiLineString(t) => t.metadata(),
            MultiPolygon(t) => t.metadata(),
            GeometryCollection(t) => t.metadata(),
            Rect(t) => t.metadata(),
            Geometry(t) => t.metadata(),
        }
    }
    /// Converts a [`NativeType`] into the relevant arrow [`DataType`].
    ///
    /// Note that an arrow [`DataType`] will lose the accompanying GeoArrow metadata if it is not
    /// part of a [`Field`] with GeoArrow extension metadata in its field metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::CoordType, datatypes::{NativeType, Dimension}};
    /// use arrow_schema::DataType;
    ///
    /// let data_type = NativeType::Point(CoordType::Interleaved, Dimension::XY).to_data_type();
    /// assert!(matches!(data_type, DataType::FixedSizeList(_, _)));
    /// ```
    pub fn to_data_type(&self) -> DataType {
        use NativeType::*;
        match self {
            Point(t) => t.data_type(),
            LineString(t) => t.data_type(),
            Polygon(t) => t.data_type(),
            MultiPoint(t) => t.data_type(),
            MultiLineString(t) => t.data_type(),
            MultiPolygon(t) => t.data_type(),
            GeometryCollection(t) => t.data_type(),
            Rect(t) => t.data_type(),
            Geometry(t) => t.data_type(),
        }
    }

    /// Returns the GeoArrow extension name pertaining to this data type.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::datatypes::NativeType;
    ///
    /// let geo_data_type = NativeType::Point(Default::default(), 2.try_into().unwrap());
    /// assert_eq!(geo_data_type.extension_name(), "geoarrow.point")
    /// ```
    pub fn extension_name(&self) -> &'static str {
        use NativeType::*;
        match self {
            Point(_) => PointType::NAME,
            LineString(_) => LineStringType::NAME,
            Polygon(_) => PolygonType::NAME,
            MultiPoint(_) => MultiPointType::NAME,
            MultiLineString(_) => MultiLineStringType::NAME,
            MultiPolygon(_) => MultiPolygonType::NAME,
            GeometryCollection(_) => GeometryCollectionType::NAME,
            Rect(_) => BoxType::NAME,
            Geometry(_) => GeometryType::NAME,
        }
    }

    /// Converts this [`NativeType`] into an arrow [`Field`], maintaining GeoArrow extension
    /// metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::datatypes::NativeType;
    ///
    /// let geo_data_type = NativeType::Point(Default::default(), 2.try_into().unwrap());
    /// let field = geo_data_type.to_field("geometry", false);
    /// assert_eq!(field.name(), "geometry");
    /// assert!(!field.is_nullable());
    /// assert_eq!(field.metadata()["ARROW:extension:name"], "geoarrow.point");
    /// ```
    pub fn to_field<N: Into<String>>(&self, name: N, nullable: bool) -> Field {
        use NativeType::*;
        match self {
            Point(t) => t.to_field(name, nullable),
            LineString(t) => t.to_field(name, nullable),
            Polygon(t) => t.to_field(name, nullable),
            MultiPoint(t) => t.to_field(name, nullable),
            MultiLineString(t) => t.to_field(name, nullable),
            MultiPolygon(t) => t.to_field(name, nullable),
            GeometryCollection(t) => t.to_field(name, nullable),
            Rect(t) => t.to_field(name, nullable),
            Geometry(t) => t.to_field(name, nullable),
        }
    }

    /// Returns this geodata type with the provided [CoordType].
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::CoordType, datatypes::NativeType};
    ///
    /// let geo_data_type = NativeType::Point(CoordType::Interleaved, 2.try_into().unwrap());
    /// let separated_geo_data_type = geo_data_type.with_coord_type(CoordType::Separated);
    /// ```
    pub fn with_coord_type(self, coord_type: CoordType) -> NativeType {
        use NativeType::*;
        match self {
            Point(t) => Point(t.with_coord_type(coord_type)),
            LineString(t) => LineString(t.with_coord_type(coord_type)),
            Polygon(t) => Polygon(t.with_coord_type(coord_type)),
            MultiPoint(t) => MultiPoint(t.with_coord_type(coord_type)),
            MultiLineString(t) => MultiLineString(t.with_coord_type(coord_type)),
            MultiPolygon(t) => MultiPolygon(t.with_coord_type(coord_type)),
            GeometryCollection(t) => GeometryCollection(t.with_coord_type(coord_type)),
            Rect(t) => Rect(t),
            Geometry(t) => Geometry(t.with_coord_type(coord_type)),
        }
    }

    /// Returns this geodata type with the provided [Dimension].
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::datatypes::NativeType;
    ///
    /// let geo_data_type = NativeType::Point(Default::default(), 2.try_into().unwrap());
    /// let geo_data_type_3d = geo_data_type.with_dimension(3.try_into().unwrap());
    /// ```
    pub fn with_dimension(self, dim: Dimension) -> NativeType {
        use NativeType::*;
        match self {
            Point(t) => Point(t.with_dimension(dim)),
            LineString(t) => LineString(t.with_dimension(dim)),
            Polygon(t) => Polygon(t.with_dimension(dim)),
            MultiPoint(t) => MultiPoint(t.with_dimension(dim)),
            MultiLineString(t) => MultiLineString(t.with_dimension(dim)),
            MultiPolygon(t) => MultiPolygon(t.with_dimension(dim)),
            GeometryCollection(t) => GeometryCollection(t.with_dimension(dim)),
            Rect(t) => Rect(t.with_dimension(dim)),
            Geometry(t) => Geometry(t),
        }
    }

    /// Returns this geodata type with the provided [Metadata].
    pub fn with_metadata(self, meta: Arc<Metadata>) -> NativeType {
        use NativeType::*;
        match self {
            Point(t) => Point(t.with_metadata(meta)),
            LineString(t) => LineString(t.with_metadata(meta)),
            Polygon(t) => Polygon(t.with_metadata(meta)),
            MultiPoint(t) => MultiPoint(t.with_metadata(meta)),
            MultiLineString(t) => MultiLineString(t.with_metadata(meta)),
            MultiPolygon(t) => MultiPolygon(t.with_metadata(meta)),
            GeometryCollection(t) => GeometryCollection(t.with_metadata(meta)),
            Rect(t) => Rect(t.with_metadata(meta)),
            Geometry(t) => Geometry(t.with_metadata(meta)),
        }
    }
}

impl SerializedType {
    /// Converts a [`SerializedType`] into the relevant arrow [`DataType`].
    ///
    /// Note that an arrow [`DataType`] will lose the accompanying GeoArrow metadata if it is not
    /// part of a [`Field`] with GeoArrow extension metadata in its field metadata.
    pub fn to_data_type(&self) -> DataType {
        use SerializedType::*;
        match self {
            WKB(t) => t.data_type(false),
            LargeWKB(t) => t.data_type(true),
            WKT(t) => t.data_type(false),
            LargeWKT(t) => t.data_type(true),
        }
    }

    /// Returns the GeoArrow extension name pertaining to this data type.
    pub fn extension_name(&self) -> &'static str {
        use SerializedType::*;
        match self {
            WKB(_) | LargeWKB(_) => WkbType::NAME,
            WKT(_) | LargeWKT(_) => WktType::NAME,
        }
    }

    /// Converts this [`SerializedType`] into an arrow [`Field`], maintaining GeoArrow extension
    /// metadata.
    pub fn to_field<N: Into<String>>(&self, name: N, nullable: bool) -> Field {
        match self {
            Self::WKB(t) => t.to_field(name, nullable, false),
            Self::LargeWKB(t) => t.to_field(name, nullable, true),
            Self::WKT(t) => t.to_field(name, nullable, false),
            Self::LargeWKT(t) => t.to_field(name, nullable, true),
        }
    }
}

impl AnyType {
    /// Converts a [`AnyType`] into the relevant arrow [`DataType`].
    ///
    /// Note that an arrow [`DataType`] will lose the accompanying GeoArrow metadata if it is not
    /// part of a [`Field`] with GeoArrow extension metadata in its field metadata.
    pub fn to_data_type(&self) -> DataType {
        match self {
            Self::Native(x) => x.to_data_type(),
            Self::Serialized(x) => x.to_data_type(),
        }
    }

    /// Returns the GeoArrow extension name pertaining to this data type.
    pub fn extension_name(&self) -> &'static str {
        match self {
            Self::Native(x) => x.extension_name(),
            Self::Serialized(x) => x.extension_name(),
        }
    }

    /// Converts this [`SerializedType`] into an arrow [`Field`], maintaining GeoArrow extension
    /// metadata.
    pub fn to_field<N: Into<String>>(&self, name: N, nullable: bool) -> Field {
        match self {
            Self::Native(x) => x.to_field(name, nullable),
            Self::Serialized(x) => x.to_field(name, nullable),
        }
    }
}

impl TryFrom<&Field> for NativeType {
    type Error = GeoArrowError;

    fn try_from(field: &Field) -> Result<Self> {
        // TODO: should we make Metadata::deserialize public?
        let metadata: Metadata =
            if let Some(ext_meta) = field.metadata().get(EXTENSION_TYPE_METADATA_KEY) {
                serde_json::from_str(ext_meta)?
            } else {
                Default::default()
            };

        use NativeType::*;
        if let Some(extension_name) = field.metadata().get(EXTENSION_TYPE_NAME_KEY) {
            let data_type = match extension_name.as_str() {
                "geoarrow.point" => Point(PointType::try_new(field.data_type(), metadata)?),
                "geoarrow.linestring" => LineString(LineStringType::try_new(field.data_type(), metadata)?),
                "geoarrow.polygon" => Polygon(PolygonType::try_new(field.data_type(), metadata) ?) ,
                "geoarrow.multipoint" => MultiPoint(MultiPointType::try_new(field.data_type(), metadata) ?),
                "geoarrow.multilinestring" => MultiLineString(MultiLineStringType::try_new(field.data_type(), metadata) ?),
                "geoarrow.multipolygon" => MultiPolygon(MultiPolygonType::try_new(field.data_type(), metadata) ?),
                "geoarrow.geometrycollection" => GeometryCollection(GeometryCollectionType::try_new(field.data_type(), metadata) ?),
                "geoarrow.box" => Rect(BoxType::try_new(field.data_type(), metadata) ?),
                "geoarrow.geometry" => Geometry(GeometryType::try_new(field.data_type(), metadata) ?),
                // We always parse geoarrow.geometry to a GeometryArray
                // "geoarrow.geometry" => parse_mixed(field)?,
                name => return Err(GeoArrowError::General(format!("Expected GeoArrow native type, got '{}'.\nIf you're passing a serialized GeoArrow type like 'geoarrow.wkb' or 'geoarrow.wkt', you need to parse to a native representation.", name))),
            };
            Ok(data_type)
        } else {
            // TODO: better error here, and document that arrays without geoarrow extension
            // metadata should use TryFrom for a specific geometry type directly, instead of using
            // GeometryArray
            let data_type = match field.data_type() {
                DataType::Struct(struct_fields) => match struct_fields.len() {
                    2 =>  NativeType::Point(PointType::new(CoordType::Separated , Dimension::XY, metadata.into())),
                    3 => NativeType::Point(PointType::new(CoordType::Separated , Dimension::XYZ, metadata.into())),
                    l => return Err(GeoArrowError::General(format!("incorrect number of struct fields {l}"))),
                },
                DataType::FixedSizeList(_, _list_size) => {
                    todo!("Restore parsing of FixedSizeList to PointType");
                    // NativeType::Point(PointType::new(CoordType::Interleaved , (*list_size as usize).try_into()?, metadata) )
                },
                _ => return Err(GeoArrowError::General("Only FixedSizeList and Struct arrays are unambigously typed for a GeoArrow native type and can be used without extension metadata.".to_string())),
            };
            Ok(data_type)
        }
    }
}

impl TryFrom<&Field> for SerializedType {
    type Error = GeoArrowError;

    fn try_from(field: &Field) -> Result<Self> {
        // TODO: should we make Metadata::deserialize public?
        let metadata: Metadata =
            if let Some(ext_meta) = field.metadata().get(EXTENSION_TYPE_METADATA_KEY) {
                serde_json::from_str(ext_meta)?
            } else {
                Default::default()
            };

        if let Some(extension_name) = field.metadata().get(EXTENSION_TYPE_NAME_KEY) {
            let data_type = match extension_name.as_str() {
                "geoarrow.wkb" | "ogc.wkb" => match field.data_type() {
                    DataType::Binary => SerializedType::WKB(WkbType::new(metadata.into())),
                    DataType::LargeBinary => {
                        SerializedType::LargeWKB(WkbType::new(metadata.into()))
                    }
                    _ => {
                        return Err(GeoArrowError::General(format!(
                            "Expected binary type for geoarrow.wkb, got '{}'",
                            field.data_type()
                        )))
                    }
                },
                "geoarrow.wkt" => match field.data_type() {
                    DataType::Utf8 => SerializedType::WKT(WktType::new(metadata.into())),
                    DataType::LargeUtf8 => SerializedType::LargeWKT(WktType::new(metadata.into())),
                    _ => {
                        return Err(GeoArrowError::General(format!(
                            "Expected string type for geoarrow.wkt, got '{}'",
                            field.data_type()
                        )))
                    }
                },
                name => {
                    return Err(GeoArrowError::General(format!(
                        "Expected GeoArrow serialized type, got '{}'",
                        name
                    )))
                }
            };
            Ok(data_type)
        } else {
            // TODO: better error here, and document that arrays without geoarrow extension
            // metadata should use TryFrom for a specific geometry type directly, instead of using
            // GeometryArray
            let data_type = match field.data_type() {
                DataType::Binary => SerializedType::WKB(WkbType::new(metadata.into())),
                DataType::LargeBinary => SerializedType::LargeWKB(WkbType::new(metadata.into())),
                DataType::Utf8 => SerializedType::WKT(WktType::new(metadata.into())),
                DataType::LargeUtf8 => SerializedType::LargeWKT(WktType::new(metadata.into())),
                _ => return Err(GeoArrowError::General("Only Binary, LargeBinary, String, and LargeString arrays are unambigously typed for a GeoArrow serialized type and can be used without extension metadata.".to_string())),
            };
            Ok(data_type)
        }
    }
}

impl TryFrom<&Field> for AnyType {
    type Error = GeoArrowError;

    fn try_from(value: &Field) -> std::result::Result<Self, Self::Error> {
        if let Ok(t) = NativeType::try_from(value) {
            Ok(AnyType::Native(t))
        } else {
            Ok(AnyType::Serialized(value.try_into()?))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::array::GeometryBuilder;
    use crate::{ArrayBase, NativeArray};

    #[test]
    fn native_type_round_trip() {
        let point_array = crate::test::point::point_array();
        let field = point_array.extension_field();
        let data_type: NativeType = field.as_ref().try_into().unwrap();
        assert_eq!(point_array.data_type(), data_type);

        let ml_array = crate::test::multilinestring::ml_array();
        let field = ml_array.extension_field();
        let data_type: NativeType = field.as_ref().try_into().unwrap();
        assert_eq!(ml_array.data_type(), data_type);

        let mut builder = GeometryBuilder::new();
        builder.push_point(Some(&crate::test::point::p0())).unwrap();
        builder.push_point(Some(&crate::test::point::p1())).unwrap();
        builder.push_point(Some(&crate::test::point::p2())).unwrap();
        builder
            .push_multi_line_string(Some(&crate::test::multilinestring::ml0()))
            .unwrap();
        builder
            .push_multi_line_string(Some(&crate::test::multilinestring::ml1()))
            .unwrap();
        let geom_array = builder.finish();
        let field = geom_array.extension_field();
        let data_type: NativeType = field.as_ref().try_into().unwrap();
        assert_eq!(geom_array.data_type(), data_type);
    }
}
