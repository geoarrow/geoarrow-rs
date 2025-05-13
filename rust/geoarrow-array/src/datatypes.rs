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

    /// Represents a [GenericWkbArray][crate::array::GenericWkbArray] with `i32` offsets.
    Wkb(WkbType),

    /// Represents a [GenericWkbArray][crate::array::GenericWkbArray] with `i64` offsets.
    LargeWkb(WkbType),

    /// Represents a [WkbViewArray][crate::array::WkbViewArray].
    WkbView(WkbType),

    /// Represents a [GenericWktArray][crate::array::GenericWktArray] with `i32` offsets.
    Wkt(WktType),

    /// Represents a [GenericWktArray][crate::array::GenericWktArray] with `i64` offsets.
    LargeWkt(WktType),

    /// Represents a [WktViewArray][crate::array::WktViewArray].
    WktView(WktType),
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
            Wkb(_) | LargeWkb(_) | WkbView(_) | Wkt(_) | LargeWkt(_) | WktView(_) => None,
        }
    }

    /// Get the [`Dimension`] of this data type, if it has one.
    ///
    /// [`GeometryArray`][crate::array::GeometryArray], WKB and WKT arrays will return `None`.
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
            Geometry(_) | Wkb(_) | LargeWkb(_) | WkbView(_) | Wkt(_) | LargeWkt(_) | WktView(_) => {
                None
            }
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
            Wkb(t) | LargeWkb(t) | WkbView(t) => t.metadata(),
            Wkt(t) | LargeWkt(t) | WktView(t) => t.metadata(),
        }
    }
    /// Converts a [`GeoArrowType`] into the relevant arrow [`DataType`].
    ///
    /// Note that an arrow [`DataType`] will lose the accompanying GeoArrow metadata if it is not
    /// part of a [`Field`] with GeoArrow extension metadata in its field metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// # use arrow_schema::DataType;
    /// # use geoarrow_array::GeoArrowType;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point_type = PointType::new(CoordType::Interleaved, Dimension::XY, Default::default());
    /// let data_type = GeoArrowType::Point(point_type).to_data_type();
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
            Wkb(_) => DataType::Binary,
            LargeWkb(_) => DataType::LargeBinary,
            WkbView(_) => DataType::BinaryView,
            Wkt(_) => DataType::Utf8,
            LargeWkt(_) => DataType::LargeUtf8,
            WktView(_) => DataType::Utf8View,
        }
    }

    /// Converts this [`GeoArrowType`] into an arrow [`Field`], maintaining GeoArrow extension
    /// metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// # use geoarrow_array::GeoArrowType;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point_type = PointType::new(CoordType::Interleaved, Dimension::XY, Default::default());
    /// let geoarrow_type = GeoArrowType::Point(point_type);
    /// let field = geoarrow_type.to_field("geometry", true);
    /// assert_eq!(field.name(), "geometry");
    /// assert!(field.is_nullable());
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
            Wkb(t) | LargeWkb(t) | WkbView(t) => {
                Field::new(name, self.to_data_type(), nullable).with_extension_type(t.clone())
            }
            Wkt(t) | LargeWkt(t) | WktView(t) => {
                Field::new(name, self.to_data_type(), nullable).with_extension_type(t.clone())
            }
        }
    }

    /// Returns this geodata type with the provided [CoordType].
    ///
    /// WKB and WKT arrays will return the same type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use geoarrow_array::GeoArrowType;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point_type = PointType::new(CoordType::Interleaved, Dimension::XY, Default::default());
    /// let geoarrow_type = GeoArrowType::Point(point_type);
    /// let new_type = geoarrow_type.with_coord_type(CoordType::Separated);
    ///
    /// assert_eq!(new_type.coord_type(), Some(CoordType::Separated));
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
    /// ```
    /// # use geoarrow_array::GeoArrowType;
    /// # use geoarrow_schema::{CoordType, Dimension, PointType};
    /// #
    /// let point_type = PointType::new(CoordType::Interleaved, Dimension::XY, Default::default());
    /// let geoarrow_type = GeoArrowType::Point(point_type);
    /// let new_type = geoarrow_type.with_dimension(Dimension::XYZ);
    ///
    /// assert_eq!(new_type.dimension(), Some(Dimension::XYZ));
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
            WkbView(t) => WkbView(t.with_metadata(meta)),
            Wkt(t) => Wkt(t.with_metadata(meta)),
            LargeWkt(t) => LargeWkt(t.with_metadata(meta)),
            WktView(t) => WktView(t.with_metadata(meta)),
        }
    }
}

macro_rules! impl_into_geoarrowtype {
    ($source_type:ident, $variant:expr) => {
        impl From<$source_type> for GeoArrowType {
            fn from(value: $source_type) -> Self {
                $variant(value)
            }
        }
    };
}

impl_into_geoarrowtype!(PointType, GeoArrowType::Point);
impl_into_geoarrowtype!(LineStringType, GeoArrowType::LineString);
impl_into_geoarrowtype!(PolygonType, GeoArrowType::Polygon);
impl_into_geoarrowtype!(MultiPointType, GeoArrowType::MultiPoint);
impl_into_geoarrowtype!(MultiLineStringType, GeoArrowType::MultiLineString);
impl_into_geoarrowtype!(MultiPolygonType, GeoArrowType::MultiPolygon);
impl_into_geoarrowtype!(GeometryCollectionType, GeoArrowType::GeometryCollection);
impl_into_geoarrowtype!(BoxType, GeoArrowType::Rect);
impl_into_geoarrowtype!(GeometryType, GeoArrowType::Geometry);

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
                name => {
                    return Err(GeoArrowError::General(format!(
                        "Expected GeoArrow type, got '{}'.",
                        name
                    )));
                }
            };
            Ok(data_type)
        } else {
            let metadata = Arc::new(metadata);
            let data_type = match field.data_type() {
                DataType::Struct(struct_fields) => {
                    if !struct_fields.iter().all(|f| matches!(f.data_type(), DataType::Float64) ) {
                        return Err(GeoArrowError::General("all struct fields must be Float64 when inferring point type.".to_string()));
                    }

                    match struct_fields.len() {
                        2 =>  GeoArrowType::Point(PointType::new(CoordType::Separated , Dimension::XY, metadata)),
                        3 => GeoArrowType::Point(PointType::new(CoordType::Separated , Dimension::XYZ, metadata)),
                        4 => GeoArrowType::Point(PointType::new(CoordType::Separated , Dimension::XYZM, metadata)),
                        l => return Err(GeoArrowError::General(format!("invalid number of struct fields: {l}"))),
                    }
                },
                DataType::FixedSizeList(inner_field, list_size) => {
                    if !matches!(inner_field.data_type(), DataType::Float64 )  {
                        return Err(GeoArrowError::General(format!("invalid inner field type of fixed size list: {}", inner_field.data_type())));
                    }

                    match list_size {
                        2 => GeoArrowType::Point(PointType::new(CoordType::Interleaved , Dimension::XY, metadata)),
                        3 => GeoArrowType::Point(PointType::new(CoordType::Interleaved , Dimension::XYZ, metadata)),
                        4 => GeoArrowType::Point(PointType::new(CoordType::Interleaved , Dimension::XYZM, metadata)),
                        _ => return Err(GeoArrowError::General(format!("invalid list_size: {list_size}"))),
                    }
                },
                DataType::Binary => Wkb(WkbType::new(metadata)),
                DataType::LargeBinary => LargeWkb(WkbType::new(metadata)),
                DataType::BinaryView => WkbView(WkbType::new(metadata)),
                DataType::Utf8 => Wkt(WktType::new(metadata)),
                DataType::LargeUtf8 => LargeWkt(WktType::new(metadata)),
                DataType::Utf8View => WktView(WktType::new(metadata)),
                _ => return Err(GeoArrowError::General("Only FixedSizeList, Struct, Binary, LargeBinary, BinaryView, String, LargeString, and StringView arrays are unambigously typed for a GeoArrow type and can be used without extension metadata.\nEnsure your array input has GeoArrow metadata.".to_string())),
            };
            Ok(data_type)
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::Array;
    use arrow_array::builder::{ArrayBuilder, FixedSizeListBuilder, Float64Builder, StructBuilder};

    use super::*;
    use crate::builder::GeometryBuilder;
    use crate::trait_::GeoArrowArray;

    #[test]
    fn infer_type_interleaved_point() {
        let test_cases = [
            (2, Dimension::XY),
            (3, Dimension::XYZ),
            (4, Dimension::XYZM),
        ];
        for (list_size, dim) in test_cases.into_iter() {
            let array = FixedSizeListBuilder::new(Float64Builder::new(), list_size).finish();
            let t =
                GeoArrowType::try_from(&Field::new("", array.data_type().clone(), true)).unwrap();
            assert_eq!(
                t,
                GeoArrowType::Point(PointType::new(
                    CoordType::Interleaved,
                    dim,
                    Default::default()
                ))
            );
        }
    }

    #[test]
    fn infer_type_separated_point() {
        let test_cases = [
            (
                vec![
                    Arc::new(Field::new("x", DataType::Float64, true)),
                    Arc::new(Field::new("y", DataType::Float64, true)),
                ],
                vec![
                    Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                    Box::new(Float64Builder::new()),
                ],
                Dimension::XY,
            ),
            (
                vec![
                    Arc::new(Field::new("x", DataType::Float64, true)),
                    Arc::new(Field::new("y", DataType::Float64, true)),
                    Arc::new(Field::new("z", DataType::Float64, true)),
                ],
                vec![
                    Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                    Box::new(Float64Builder::new()),
                    Box::new(Float64Builder::new()),
                ],
                Dimension::XYZ,
            ),
            (
                vec![
                    Arc::new(Field::new("x", DataType::Float64, true)),
                    Arc::new(Field::new("y", DataType::Float64, true)),
                    Arc::new(Field::new("z", DataType::Float64, true)),
                    Arc::new(Field::new("m", DataType::Float64, true)),
                ],
                vec![
                    Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                    Box::new(Float64Builder::new()),
                    Box::new(Float64Builder::new()),
                    Box::new(Float64Builder::new()),
                ],
                Dimension::XYZM,
            ),
        ];
        for (fields, builders, dim) in test_cases.into_iter() {
            let array = StructBuilder::new(fields, builders).finish();
            let t =
                GeoArrowType::try_from(&Field::new("", array.data_type().clone(), true)).unwrap();
            assert_eq!(
                t,
                GeoArrowType::Point(PointType::new(
                    CoordType::Separated,
                    dim,
                    Default::default()
                ))
            );
        }
    }

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

        let mut builder = GeometryBuilder::new(GeometryType::new(
            CoordType::Interleaved,
            Default::default(),
        ));
        builder
            .push_geometry(Some(&crate::test::point::p0()))
            .unwrap();
        builder
            .push_geometry(Some(&crate::test::point::p1()))
            .unwrap();
        builder
            .push_geometry(Some(&crate::test::point::p2()))
            .unwrap();
        builder
            .push_geometry(Some(&crate::test::multilinestring::ml0()))
            .unwrap();
        builder
            .push_geometry(Some(&crate::test::multilinestring::ml1()))
            .unwrap();
        let geom_array = builder.finish();
        let field = geom_array.data_type.to_field("geometry", true);
        let data_type: GeoArrowType = (&field).try_into().unwrap();
        assert_eq!(geom_array.data_type(), data_type);
    }
}
