//! Contains the implementation of [`GeoArrowType`], which defines all geometry arrays in this
//! crate.

use std::sync::Arc;

use arrow_schema::extension::ExtensionType;
use arrow_schema::{DataType, Field};
use geoarrow_schema::{
    BoxType, CoordType, Dimension, GeometryCollectionType, GeometryType, LineStringType, Metadata,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType,
    WktType,
};

use crate::error::{GeoArrowError, Result};

/// A type enum representing all possible GeoArrow geometry types, including both "native" and
/// "serialized" encodings.
///
/// This is designed to aid in downcasting from dynamically-typed geometry arrays in combination
/// with the [`AsGeoArrowArray`][crate::AsGeoArrowArray] trait.
///
/// This type uniquely identifies the physical buffer layout of each geometry array type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GeoArrowType {
    /// Represents a [PointArray][crate::array::PointArray].
    Point(PointType),

    /// Represents a [LineStringArray][crate::array::LineStringArray].
    LineString(LineStringType),

    /// Represents a [PolygonArray][crate::array::PolygonArray].
    Polygon(PolygonType),

    /// Represents a [MultiPointArray][crate::array::MultiPointArray].
    MultiPoint(MultiPointType),

    /// Represents a [MultiLineStringArray][crate::array::MultiLineStringArray].
    MultiLineString(MultiLineStringType),

    /// Represents a [MultiPolygonArray][crate::array::MultiPolygonArray].
    MultiPolygon(MultiPolygonType),

    /// Represents a [GeometryCollectionArray][crate::array::GeometryCollectionArray].
    GeometryCollection(GeometryCollectionType),

    /// Represents a [RectArray][crate::array::RectArray].
    Rect(BoxType),

    /// Represents a mixed geometry array of unknown types or dimensions
    Geometry(GeometryType),

    /// Represents a [WkbArray][crate::array::WkbArray] with `i32` offsets.
    Wkb(WkbType),

    /// Represents a [WkbArray][crate::array::WkbArray] with `i64` offsets.
    LargeWkb(WkbType),

    /// Represents a [WktArray][crate::array::WktArray] with `i32` offsets.
    Wkt(WktType),

    /// Represents a [WktArray][crate::array::WktArray] with `i64` offsets.
    LargeWkt(WktType),
}

impl From<GeoArrowType> for DataType {
    fn from(value: GeoArrowType) -> Self {
        value.to_data_type()
    }
}

impl GeoArrowType {
    /// Get the [`CoordType`] of this data type.
    ///
    /// WKB and WKT arrays will return `None`.
    pub fn coord_type(&self) -> Option<CoordType> {
        use GeoArrowType::*;
        match self {
            Point(t) => Some(t.coord_type()),
            LineString(t) => Some(t.coord_type()),
            Polygon(t) => Some(t.coord_type()),
            MultiPoint(t) => Some(t.coord_type()),
            MultiLineString(t) => Some(t.coord_type()),
            MultiPolygon(t) => Some(t.coord_type()),
            GeometryCollection(t) => Some(t.coord_type()),
            Rect(_) => Some(CoordType::Separated),
            Geometry(t) => Some(t.coord_type()),
            Wkb(_) | LargeWkb(_) | Wkt(_) | LargeWkt(_) => None,
        }
    }

    /// Get the [`Dimension`] of this data type, if it has one.
    ///
    /// "Unknown" native arrays can hold all dimensions.
    ///
    /// WKB and WKT arrays will return `None`.
    pub fn dimension(&self) -> Option<Dimension> {
        use GeoArrowType::*;
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
            Wkb(_) | LargeWkb(_) | Wkt(_) | LargeWkt(_) => None,
        }
    }

    /// Returns this geodata type with the provided [Metadata].
    pub fn metadata(&self) -> &Arc<Metadata> {
        use GeoArrowType::*;
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
            Wkb(t) | LargeWkb(t) => t.metadata(),
            Wkt(t) | LargeWkt(t) => t.metadata(),
        }
    }
    /// Converts a [`GeoArrowType`] into the relevant arrow [`DataType`].
    ///
    /// Note that an arrow [`DataType`] will lose the accompanying GeoArrow metadata if it is not
    /// part of a [`Field`] with GeoArrow extension metadata in its field metadata.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{array::CoordType, datatypes::{GeoArrowType, Dimension}};
    /// use arrow_schema::DataType;
    ///
    /// let data_type = GeoArrowType::Point(CoordType::Interleaved, Dimension::XY).to_data_type();
    /// assert!(matches!(data_type, DataType::FixedSizeList(_, _)));
    /// ```
    pub fn to_data_type(&self) -> DataType {
        use GeoArrowType::*;
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
            Wkb(t) => t.data_type(false),
            LargeWkb(t) => t.data_type(true),
            Wkt(t) => t.data_type(false),
            LargeWkt(t) => t.data_type(true),
        }
    }

    /// Converts this [`GeoArrowType`] into an arrow [`Field`], maintaining GeoArrow extension
    /// metadata.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::datatypes::GeoArrowType;
    ///
    /// let geo_data_type = GeoArrowType::Point(Default::default(), 2.try_into().unwrap());
    /// let field = geo_data_type.to_field("geometry", false);
    /// assert_eq!(field.name(), "geometry");
    /// assert!(!field.is_nullable());
    /// assert_eq!(field.metadata()["ARROW:extension:name"], "geoarrow.point");
    /// ```
    pub fn to_field<N: Into<String>>(&self, name: N, nullable: bool) -> Field {
        use GeoArrowType::*;
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
            Wkb(t) => t.to_field(name, nullable, false),
            LargeWkb(t) => t.to_field(name, nullable, true),
            Wkt(t) => t.to_field(name, nullable, false),
            LargeWkt(t) => t.to_field(name, nullable, true),
        }
    }

    /// Returns this geodata type with the provided [CoordType].
    ///
    /// WKB and WKT arrays will return the same type.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::{array::CoordType, datatypes::GeoArrowType};
    ///
    /// let geo_data_type = GeoArrowType::Point(CoordType::Interleaved, 2.try_into().unwrap());
    /// let separated_geo_data_type = geo_data_type.with_coord_type(CoordType::Separated);
    /// ```
    pub fn with_coord_type(self, coord_type: CoordType) -> GeoArrowType {
        use GeoArrowType::*;
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
            _ => self,
        }
    }

    /// Returns this geodata type with the provided [Dimension].
    ///
    /// WKB and WKT arrays will return the same type.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use geoarrow::datatypes::GeoArrowType;
    ///
    /// let geo_data_type = GeoArrowType::Point(Default::default(), 2.try_into().unwrap());
    /// let geo_data_type_3d = geo_data_type.with_dimension(3.try_into().unwrap());
    /// ```
    pub fn with_dimension(self, dim: Dimension) -> GeoArrowType {
        use GeoArrowType::*;
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
            _ => self,
        }
    }

    /// Returns this geodata type with the provided [Metadata].
    pub fn with_metadata(self, meta: Arc<Metadata>) -> GeoArrowType {
        use GeoArrowType::*;
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
            Wkb(t) => Wkb(t.with_metadata(meta)),
            LargeWkb(t) => LargeWkb(t.with_metadata(meta)),
            Wkt(t) => Wkt(t.with_metadata(meta)),
            LargeWkt(t) => LargeWkt(t.with_metadata(meta)),
        }
    }
}

impl TryFrom<&Field> for GeoArrowType {
    type Error = GeoArrowError;

    fn try_from(field: &Field) -> Result<Self> {
        // TODO: should we make Metadata::deserialize public?
        let metadata: Metadata = if let Some(ext_meta) = field.extension_type_metadata() {
            serde_json::from_str(ext_meta)?
        } else {
            Default::default()
        };

        use GeoArrowType::*;
        if let Some(extension_name) = field.extension_type_name() {
            let data_type = match extension_name {
                PointType::NAME => Point(PointType::try_new(field.data_type(), metadata)?),
                LineStringType::NAME => {
                    LineString(LineStringType::try_new(field.data_type(), metadata)?)
                }
                PolygonType::NAME => Polygon(PolygonType::try_new(field.data_type(), metadata)?),
                MultiPointType::NAME => {
                    MultiPoint(MultiPointType::try_new(field.data_type(), metadata)?)
                }
                MultiLineStringType::NAME => {
                    MultiLineString(MultiLineStringType::try_new(field.data_type(), metadata)?)
                }
                MultiPolygonType::NAME => {
                    MultiPolygon(MultiPolygonType::try_new(field.data_type(), metadata)?)
                }
                GeometryCollectionType::NAME => GeometryCollection(
                    GeometryCollectionType::try_new(field.data_type(), metadata)?,
                ),
                BoxType::NAME => Rect(BoxType::try_new(field.data_type(), metadata)?),
                GeometryType::NAME => Geometry(GeometryType::try_new(field.data_type(), metadata)?),
                WkbType::NAME | "ogc.wkb" => match field.data_type() {
                    DataType::Binary => Wkb(WkbType::new(metadata.into())),
                    DataType::LargeBinary => LargeWkb(WkbType::new(metadata.into())),
                    _ => {
                        return Err(GeoArrowError::General(format!(
                            "Expected binary type for geoarrow.wkb, got '{}'",
                            field.data_type()
                        )));
                    }
                },
                WktType::NAME => match field.data_type() {
                    DataType::Utf8 => Wkt(WktType::new(metadata.into())),
                    DataType::LargeUtf8 => LargeWkt(WktType::new(metadata.into())),
                    _ => {
                        return Err(GeoArrowError::General(format!(
                            "Expected string type for geoarrow.wkt, got '{}'",
                            field.data_type()
                        )));
                    }
                },

                // We always parse geoarrow.geometry to a GeometryArray
                // "geoarrow.geometry" => parse_mixed(field)?,
                name => {
                    return Err(GeoArrowError::General(format!(
                        "Expected GeoArrow native type, got '{}'.\nIf you're passing a serialized GeoArrow type like 'geoarrow.wkb' or 'geoarrow.wkt', you need to parse to a native representation.",
                        name
                    )));
                }
            };
            Ok(data_type)
        } else {
            // TODO: better error here, and document that arrays without geoarrow extension
            // metadata should use TryFrom for a specific geometry type directly, instead of using
            // GeometryArray
            let data_type = match field.data_type() {
                DataType::Struct(struct_fields) => match struct_fields.len() {
                    2 =>  GeoArrowType::Point(PointType::new(CoordType::Separated , Dimension::XY, metadata.into())),
                    3 => GeoArrowType::Point(PointType::new(CoordType::Separated , Dimension::XYZ, metadata.into())),
                    l => return Err(GeoArrowError::General(format!("incorrect number of struct fields {l}"))),
                },
                DataType::FixedSizeList(_, _list_size) => {
                    todo!("Restore parsing of FixedSizeList to PointType");
                    // GeoArrowType::Point(PointType::new(CoordType::Interleaved , (*list_size as usize).try_into()?, metadata) )
                },
                DataType::Binary => Wkb(WkbType::new(metadata.into())),
                DataType::LargeBinary => LargeWkb(WkbType::new(metadata.into())),
                DataType::Utf8 => Wkt(WktType::new(metadata.into())),
                DataType::LargeUtf8 => LargeWkt(WktType::new(metadata.into())),
                _ => return Err(GeoArrowError::General("Only FixedSizeList, Struct, Binary, LargeBinary, String, and LargeString arrays are unambigously typed for a GeoArrow type and can be used without extension metadata.".to_string())),
            };
            Ok(data_type)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::builder::GeometryBuilder;
    use crate::trait_::GeoArrowArray;

    #[test]
    fn native_type_round_trip() {
        let point_array = crate::test::point::point_array(CoordType::Interleaved);
        let field = point_array.data_type.to_field("geometry", true);
        let data_type: GeoArrowType = (&field).try_into().unwrap();
        assert_eq!(point_array.data_type(), data_type);

        let ml_array = crate::test::multilinestring::ml_array(CoordType::Interleaved);
        let field = ml_array.data_type.to_field("geometry", true);
        let data_type: GeoArrowType = (&field).try_into().unwrap();
        assert_eq!(ml_array.data_type(), data_type);

        let mut builder = GeometryBuilder::new(
            GeometryType::new(CoordType::Interleaved, Default::default()),
            true,
        );
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
        let field = geom_array.data_type.to_field("geometry", true);
        let data_type: GeoArrowType = (&field).try_into().unwrap();
        assert_eq!(geom_array.data_type(), data_type);
    }
}
