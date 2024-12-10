//! Contains the implementation of [`NativeType`], which defines all geometry arrays in this
//! crate.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_schema::{DataType, Field, Fields, UnionFields, UnionMode};

use crate::array::metadata::ArrayMetadata;
use crate::array::CoordType;
use crate::error::{GeoArrowError, Result};

/// The dimension of the geometry array.
///
/// [Dimension] implements [TryFrom] for integers:
///
/// ```
/// use geoarrow::datatypes::Dimension;
///
/// assert_eq!(Dimension::try_from(2).unwrap(), Dimension::XY);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dimension {
    /// Two-dimensional.
    XY,

    /// Three-dimensional.
    XYZ,
}

impl Dimension {
    /// Returns the size of this dimension.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::datatypes::Dimension;
    ///
    /// assert_eq!(Dimension::XY.size(), 2);
    /// assert_eq!(Dimension::XYZ.size(), 3);
    /// ```
    pub fn size(&self) -> usize {
        match self {
            Dimension::XY => 2,
            Dimension::XYZ => 3,
        }
    }
}

impl TryFrom<usize> for Dimension {
    type Error = GeoArrowError;

    fn try_from(value: usize) -> std::result::Result<Self, Self::Error> {
        match value {
            2 => Ok(Dimension::XY),
            3 => Ok(Dimension::XYZ),
            v => Err(GeoArrowError::General(format!("Unexpected array size {v}"))),
        }
    }
}

impl TryFrom<i32> for Dimension {
    type Error = GeoArrowError;

    fn try_from(value: i32) -> std::result::Result<Self, Self::Error> {
        let usize_num =
            usize::try_from(value).map_err(|err| GeoArrowError::General(err.to_string()))?;
        Dimension::try_from(usize_num)
    }
}

impl From<Dimension> for geo_traits::Dimensions {
    fn from(value: Dimension) -> Self {
        match value {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
        }
    }
}

impl TryFrom<geo_traits::Dimensions> for Dimension {
    type Error = GeoArrowError;

    fn try_from(value: geo_traits::Dimensions) -> std::result::Result<Self, Self::Error> {
        match value {
            geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => Ok(Dimension::XY),
            geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Unknown(3) => Ok(Dimension::XYZ),
            _ => Err(GeoArrowError::General(format!(
                "Unsupported dimension {:?}",
                value
            ))),
        }
    }
}

/// A type enum representing "native" GeoArrow geometry types.
///
/// This is designed to aid in downcasting from dynamically-typed geometry arrays.
///
/// This type uniquely identifies the physical buffer layout of each geometry array type.
/// It must always be possible to accurately downcast from a `dyn &NativeArray` or `dyn
/// &ChunkedNativeArray` to a unique concrete array type using this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NativeType {
    /// Represents a [PointArray][crate::array::PointArray] or
    /// [ChunkedPointArray][crate::chunked_array::ChunkedPointArray].
    Point(CoordType, Dimension),

    /// Represents a [LineStringArray][crate::array::LineStringArray] or
    /// [ChunkedLineStringArray][crate::chunked_array::ChunkedLineStringArray] with `i32` offsets.
    LineString(CoordType, Dimension),

    /// Represents a [PolygonArray][crate::array::PolygonArray] or
    /// [ChunkedPolygonArray][crate::chunked_array::ChunkedPolygonArray] with `i32` offsets.
    Polygon(CoordType, Dimension),

    /// Represents a [MultiPointArray][crate::array::MultiPointArray] or
    /// [ChunkedMultiPointArray][crate::chunked_array::ChunkedMultiPointArray] with `i32` offsets.
    MultiPoint(CoordType, Dimension),

    /// Represents a [MultiLineStringArray][crate::array::MultiLineStringArray] or
    /// [ChunkedMultiLineStringArray][crate::chunked_array::ChunkedMultiLineStringArray] with `i32`
    /// offsets.
    MultiLineString(CoordType, Dimension),

    /// Represents a [MultiPolygonArray][crate::array::MultiPolygonArray] or
    /// [ChunkedMultiPolygonArray][crate::chunked_array::ChunkedMultiPolygonArray] with `i32`
    /// offsets.
    MultiPolygon(CoordType, Dimension),

    // Represents a [MixedGeometryArray][crate::array::MixedGeometryArray] or
    // [ChunkedMixedGeometryArray][crate::chunked_array::ChunkedMixedGeometryArray] with `i32`
    // offsets.
    // Mixed(CoordType, Dimension),
    /// Represents a [GeometryCollectionArray][crate::array::GeometryCollectionArray] or
    /// [ChunkedGeometryCollectionArray][crate::chunked_array::ChunkedGeometryCollectionArray] with
    /// `i32` offsets.
    GeometryCollection(CoordType, Dimension),

    /// Represents a [RectArray][crate::array::RectArray] or
    /// [ChunkedRectArray][crate::chunked_array::ChunkedRectArray].
    Rect(Dimension),

    /// Represents a mixed geometry array of unknown types or dimensions
    Geometry(CoordType),
}

/// A type enum representing "serialized" GeoArrow geometry types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SerializedType {
    /// Represents a [WKBArray][crate::array::WKBArray] or
    /// [ChunkedWKBArray][crate::chunked_array::ChunkedWKBArray] with `i32` offsets.
    WKB,

    /// Represents a [WKBArray][crate::array::WKBArray] or
    /// [ChunkedWKBArray][crate::chunked_array::ChunkedWKBArray] with `i64` offsets.
    LargeWKB,

    /// Represents a [WKTArray][crate::array::WKTArray] or
    /// [ChunkedWKTArray][crate::chunked_array::ChunkedWKTArray] with `i32` offsets.
    WKT,

    /// Represents a [WKTArray][crate::array::WKTArray] or
    /// [ChunkedWKTArray][crate::chunked_array::ChunkedWKTArray] with `i64` offsets.
    LargeWKT,
}

/// A type enum representing all possible GeoArrow geometry types, including both "native" and
/// "serialized" encodings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AnyType {
    /// A "native" GeoArrow encoding
    Native(NativeType),

    /// A "serialized" GeoArrow encoding, such as WKB or WKT
    Serialized(SerializedType),
}

pub(crate) fn coord_type_to_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    match (coord_type, dim) {
        (CoordType::Interleaved, Dimension::XY) => {
            let values_field = Field::new("xy", DataType::Float64, false);
            DataType::FixedSizeList(Arc::new(values_field), 2)
        }
        (CoordType::Interleaved, Dimension::XYZ) => {
            let values_field = Field::new("xyz", DataType::Float64, false);
            DataType::FixedSizeList(Arc::new(values_field), 3)
        }
        (CoordType::Separated, Dimension::XY) => {
            let values_fields = vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
            ];
            DataType::Struct(values_fields.into())
        }
        (CoordType::Separated, Dimension::XYZ) => {
            let values_fields = vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
                Field::new("z", DataType::Float64, false),
            ];
            DataType::Struct(values_fields.into())
        }
    }
}

fn point_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    coord_type_to_data_type(coord_type, dim)
}

fn line_string_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type, dim);
    let vertices_field = Field::new("vertices", coords_type, false).into();
    DataType::List(vertices_field)
}

fn polygon_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type, dim);
    let vertices_field = Field::new("vertices", coords_type, false);
    let rings_field = Field::new_list("rings", vertices_field, false).into();
    DataType::List(rings_field)
}

fn multi_point_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type, dim);
    let vertices_field = Field::new("points", coords_type, false).into();
    DataType::List(vertices_field)
}

fn multi_line_string_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type, dim);
    let vertices_field = Field::new("vertices", coords_type, false);
    let linestrings_field = Field::new_list("linestrings", vertices_field, false).into();
    DataType::List(linestrings_field)
}

fn multi_polygon_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type, dim);
    let vertices_field = Field::new("vertices", coords_type, false);
    let rings_field = Field::new_list("rings", vertices_field, false);
    let polygons_field = Field::new_list("polygons", rings_field, false).into();
    DataType::List(polygons_field)
}

pub(crate) fn mixed_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    let mut fields = vec![];
    let mut type_ids = vec![];

    match dim {
        Dimension::XY => type_ids.extend([1, 2, 3, 4, 5, 6]),
        Dimension::XYZ => type_ids.extend([11, 12, 13, 14, 15, 16]),
    }

    // Note: we manually construct the fields because these fields shouldn't have their own
    // GeoArrow extension metadata
    fields.push(Field::new(
        "",
        NativeType::Point(coord_type, dim).to_data_type(),
        true,
    ));

    let linestring = NativeType::LineString(coord_type, dim);
    fields.push(Field::new("", linestring.to_data_type(), true));

    let polygon = NativeType::Polygon(coord_type, dim);
    fields.push(Field::new("", polygon.to_data_type(), true));

    let multi_point = NativeType::MultiPoint(coord_type, dim);
    fields.push(Field::new("", multi_point.to_data_type(), true));

    let multi_line_string = NativeType::MultiLineString(coord_type, dim);
    fields.push(Field::new("", multi_line_string.to_data_type(), true));

    let multi_polygon = NativeType::MultiPolygon(coord_type, dim);
    fields.push(Field::new("", multi_polygon.to_data_type(), true));

    let union_fields = UnionFields::new(type_ids, fields);
    DataType::Union(union_fields, UnionMode::Dense)
}

fn geometry_collection_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    let geometries_field = Field::new("geometries", mixed_data_type(coord_type, dim), false).into();
    DataType::List(geometries_field)
}

fn wkb_data_type<O: OffsetSizeTrait>() -> DataType {
    match O::IS_LARGE {
        true => DataType::LargeBinary,
        false => DataType::Binary,
    }
}

fn wkt_data_type<O: OffsetSizeTrait>() -> DataType {
    match O::IS_LARGE {
        true => DataType::LargeUtf8,
        false => DataType::Utf8,
    }
}

pub(crate) fn rect_fields(dim: Dimension) -> Fields {
    let values_fields = match dim {
        Dimension::XY => {
            vec![
                Field::new("xmin", DataType::Float64, false),
                Field::new("ymin", DataType::Float64, false),
                Field::new("xmax", DataType::Float64, false),
                Field::new("ymax", DataType::Float64, false),
            ]
        }
        Dimension::XYZ => {
            vec![
                Field::new("xmin", DataType::Float64, false),
                Field::new("ymin", DataType::Float64, false),
                Field::new("zmin", DataType::Float64, false),
                Field::new("xmax", DataType::Float64, false),
                Field::new("ymax", DataType::Float64, false),
                Field::new("zmax", DataType::Float64, false),
            ]
        }
    };

    values_fields.into()
}

fn rect_data_type(dim: Dimension) -> DataType {
    DataType::Struct(rect_fields(dim))
}

fn unknown_data_type(coord_type: CoordType) -> DataType {
    let mut fields = vec![];
    let type_ids = vec![1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16];

    // Note: we manually construct the fields because these fields shouldn't have their own
    // GeoArrow extension metadata
    fields.push(Field::new(
        "",
        NativeType::Point(coord_type, Dimension::XY).to_data_type(),
        true,
    ));

    let linestring = NativeType::LineString(coord_type, Dimension::XY);
    fields.push(Field::new("", linestring.to_data_type(), true));

    let polygon = NativeType::Polygon(coord_type, Dimension::XY);
    fields.push(Field::new("", polygon.to_data_type(), true));

    let multi_point = NativeType::MultiPoint(coord_type, Dimension::XY);
    fields.push(Field::new("", multi_point.to_data_type(), true));

    let multi_line_string = NativeType::MultiLineString(coord_type, Dimension::XY);
    fields.push(Field::new("", multi_line_string.to_data_type(), true));

    let multi_polygon = NativeType::MultiPolygon(coord_type, Dimension::XY);
    fields.push(Field::new("", multi_polygon.to_data_type(), true));

    fields.push(Field::new(
        "",
        NativeType::Point(coord_type, Dimension::XYZ).to_data_type(),
        true,
    ));

    let linestring = NativeType::LineString(coord_type, Dimension::XYZ);
    fields.push(Field::new("", linestring.to_data_type(), true));

    let polygon = NativeType::Polygon(coord_type, Dimension::XYZ);
    fields.push(Field::new("", polygon.to_data_type(), true));

    let multi_point = NativeType::MultiPoint(coord_type, Dimension::XYZ);
    fields.push(Field::new("", multi_point.to_data_type(), true));

    let multi_line_string = NativeType::MultiLineString(coord_type, Dimension::XYZ);
    fields.push(Field::new("", multi_line_string.to_data_type(), true));

    let multi_polygon = NativeType::MultiPolygon(coord_type, Dimension::XYZ);
    fields.push(Field::new("", multi_polygon.to_data_type(), true));

    let union_fields = UnionFields::new(type_ids, fields);
    DataType::Union(union_fields, UnionMode::Dense)
}

impl NativeType {
    /// Get the [`CoordType`] of this data type.
    pub fn coord_type(&self) -> CoordType {
        use NativeType::*;
        match self {
            Point(ct, _) => *ct,
            LineString(ct, _) => *ct,
            Polygon(ct, _) => *ct,
            MultiPoint(ct, _) => *ct,
            MultiLineString(ct, _) => *ct,
            MultiPolygon(ct, _) => *ct,
            GeometryCollection(ct, _) => *ct,
            Rect(_) => CoordType::Separated,
            Geometry(ct) => *ct,
        }
    }

    /// Get the [`Dimension`] of this data type, if it has one.
    ///
    /// "Unknown" native arrays can hold all dimensions.
    pub fn dimension(&self) -> Option<Dimension> {
        use NativeType::*;
        match self {
            Point(_, dim) => Some(*dim),
            LineString(_, dim) => Some(*dim),
            Polygon(_, dim) => Some(*dim),
            MultiPoint(_, dim) => Some(*dim),
            MultiLineString(_, dim) => Some(*dim),
            MultiPolygon(_, dim) => Some(*dim),
            GeometryCollection(_, dim) => Some(*dim),
            Rect(dim) => Some(*dim),
            Geometry(_) => None,
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
            Point(coord_type, dim) => point_data_type(*coord_type, *dim),
            LineString(coord_type, dim) => line_string_data_type(*coord_type, *dim),
            Polygon(coord_type, dim) => polygon_data_type(*coord_type, *dim),
            MultiPoint(coord_type, dim) => multi_point_data_type(*coord_type, *dim),
            MultiLineString(coord_type, dim) => multi_line_string_data_type(*coord_type, *dim),
            MultiPolygon(coord_type, dim) => multi_polygon_data_type(*coord_type, *dim),
            GeometryCollection(coord_type, dim) => geometry_collection_data_type(*coord_type, *dim),
            Rect(dim) => rect_data_type(*dim),
            Geometry(coord_type) => unknown_data_type(*coord_type),
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
            Point(_, _) => "geoarrow.point",
            LineString(_, _) => "geoarrow.linestring",
            Polygon(_, _) => "geoarrow.polygon",
            MultiPoint(_, _) => "geoarrow.multipoint",
            MultiLineString(_, _) => "geoarrow.multilinestring",
            MultiPolygon(_, _) => "geoarrow.multipolygon",
            GeometryCollection(_, _) => "geoarrow.geometrycollection",
            Rect(_) => "geoarrow.box",
            Geometry(_) => "geoarrow.geometry",
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
        let extension_name = self.extension_name();
        let mut metadata = HashMap::with_capacity(1);
        metadata.insert(
            "ARROW:extension:name".to_string(),
            extension_name.to_string(),
        );
        Field::new(name, self.to_data_type(), nullable).with_metadata(metadata)
    }

    /// Converts this geo-data type to a field with the additional [ArrayMetadata].
    ///
    /// # Examples
    ///
    /// ```
    /// use geoarrow::{array::metadata::{ArrayMetadata, Edges}, datatypes::NativeType};
    ///
    /// let geo_data_type = NativeType::Point(Default::default(), 2.try_into().unwrap());
    /// let metadata = ArrayMetadata {
    ///     edges: Some(Edges::Spherical),
    ///     ..Default::default()
    /// };
    /// let field = geo_data_type.to_field_with_metadata("geometry", false, &metadata);
    /// ```
    pub fn to_field_with_metadata<N: Into<String>>(
        &self,
        name: N,
        nullable: bool,
        array_metadata: &ArrayMetadata,
    ) -> Field {
        let extension_name = self.extension_name();
        let mut metadata = HashMap::with_capacity(2);
        metadata.insert(
            "ARROW:extension:name".to_string(),
            extension_name.to_string(),
        );
        if array_metadata.should_serialize() {
            metadata.insert(
                "ARROW:extension:metadata".to_string(),
                serde_json::to_string(array_metadata).unwrap(),
            );
        }
        Field::new(name, self.to_data_type(), nullable).with_metadata(metadata)
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
            Point(_, dim) => Point(coord_type, dim),
            LineString(_, dim) => LineString(coord_type, dim),
            Polygon(_, dim) => Polygon(coord_type, dim),
            MultiPoint(_, dim) => MultiPoint(coord_type, dim),
            MultiLineString(_, dim) => MultiLineString(coord_type, dim),
            MultiPolygon(_, dim) => MultiPolygon(coord_type, dim),
            GeometryCollection(_, dim) => GeometryCollection(coord_type, dim),
            Rect(dim) => Rect(dim),
            Geometry(_) => Geometry(coord_type),
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
            Point(coord_type, _) => Point(coord_type, dim),
            LineString(coord_type, _) => LineString(coord_type, dim),
            Polygon(coord_type, _) => Polygon(coord_type, dim),
            MultiPoint(coord_type, _) => MultiPoint(coord_type, dim),
            MultiLineString(coord_type, _) => MultiLineString(coord_type, dim),
            MultiPolygon(coord_type, _) => MultiPolygon(coord_type, dim),
            GeometryCollection(coord_type, _) => GeometryCollection(coord_type, dim),
            Rect(_) => Rect(dim),
            Geometry(coord_type) => Geometry(coord_type),
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
            WKB => wkb_data_type::<i32>(),
            LargeWKB => wkb_data_type::<i64>(),
            WKT => wkt_data_type::<i32>(),
            LargeWKT => wkt_data_type::<i64>(),
        }
    }

    /// Returns the GeoArrow extension name pertaining to this data type.
    pub fn extension_name(&self) -> &'static str {
        use SerializedType::*;
        match self {
            WKB | LargeWKB => "geoarrow.wkb",
            WKT | LargeWKT => "geoarrow.wkt",
        }
    }

    /// Converts this [`SerializedType`] into an arrow [`Field`], maintaining GeoArrow extension
    /// metadata.
    pub fn to_field<N: Into<String>>(&self, name: N, nullable: bool) -> Field {
        let extension_name = self.extension_name();
        let mut metadata = HashMap::with_capacity(1);
        metadata.insert(
            "ARROW:extension:name".to_string(),
            extension_name.to_string(),
        );
        Field::new(name, self.to_data_type(), nullable).with_metadata(metadata)
    }

    /// Converts this geo-data type to a field with the additional [ArrayMetadata].
    pub fn to_field_with_metadata<N: Into<String>>(
        &self,
        name: N,
        nullable: bool,
        array_metadata: &ArrayMetadata,
    ) -> Field {
        let extension_name = self.extension_name();
        let mut metadata = HashMap::with_capacity(2);
        metadata.insert(
            "ARROW:extension:name".to_string(),
            extension_name.to_string(),
        );
        if array_metadata.should_serialize() {
            metadata.insert(
                "ARROW:extension:metadata".to_string(),
                serde_json::to_string(array_metadata).unwrap(),
            );
        }
        Field::new(name, self.to_data_type(), nullable).with_metadata(metadata)
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

    /// Converts this geo-data type to a field with the additional [ArrayMetadata].
    pub fn to_field_with_metadata<N: Into<String>>(
        &self,
        name: N,
        nullable: bool,
        array_metadata: &ArrayMetadata,
    ) -> Field {
        match self {
            Self::Native(x) => x.to_field_with_metadata(name, nullable, array_metadata),
            Self::Serialized(x) => x.to_field_with_metadata(name, nullable, array_metadata),
        }
    }
}

fn parse_data_type(data_type: &DataType) -> Result<(CoordType, Dimension)> {
    match data_type {
        DataType::FixedSizeList(_, list_size) => {
            Ok((CoordType::Interleaved, (*list_size).try_into()?))
        }
        DataType::Struct(struct_fields) => {
            Ok((CoordType::Separated, struct_fields.len().try_into()?))
        }
        dt => Err(GeoArrowError::General(format!("Unexpected data type {dt}"))),
    }
}

fn parse_point(field: &Field) -> Result<NativeType> {
    let (ct, dim) = parse_data_type(field.data_type())?;
    Ok(NativeType::Point(ct, dim))
}

fn parse_linestring(field: &Field) -> Result<NativeType> {
    match field.data_type() {
        DataType::List(inner_field) | DataType::LargeList(inner_field) => {
            let (ct, dim) = parse_data_type(inner_field.data_type())?;
            Ok(NativeType::LineString(ct, dim))
        }
        dt => Err(GeoArrowError::General(format!("Unexpected data type {dt}"))),
    }
}

fn parse_polygon(field: &Field) -> Result<NativeType> {
    match field.data_type() {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => {
                let (ct, dim) = parse_data_type(inner2.data_type())?;
                Ok(NativeType::Polygon(ct, dim))
            }
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => {
                let (ct, dim) = parse_data_type(inner2.data_type())?;
                Ok(NativeType::Polygon(ct, dim))
            }
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn parse_multi_point(field: &Field) -> Result<NativeType> {
    match field.data_type() {
        DataType::List(inner_field) => {
            let (ct, dim) = parse_data_type(inner_field.data_type())?;
            Ok(NativeType::MultiPoint(ct, dim))
        }
        DataType::LargeList(inner_field) => {
            let (ct, dim) = parse_data_type(inner_field.data_type())?;
            Ok(NativeType::MultiPoint(ct, dim))
        }
        _ => panic!(),
    }
}

fn parse_multi_linestring(field: &Field) -> Result<NativeType> {
    match field.data_type() {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => {
                let (ct, dim) = parse_data_type(inner2.data_type())?;
                Ok(NativeType::MultiLineString(ct, dim))
            }
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => {
                let (ct, dim) = parse_data_type(inner2.data_type())?;
                Ok(NativeType::MultiLineString(ct, dim))
            }
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn parse_multi_polygon(field: &Field) -> Result<NativeType> {
    match field.data_type() {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => match inner2.data_type() {
                DataType::List(inner3) => {
                    let (ct, dim) = parse_data_type(inner3.data_type())?;
                    Ok(NativeType::MultiPolygon(ct, dim))
                }
                _ => panic!(),
            },
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => match inner2.data_type() {
                DataType::LargeList(inner3) => {
                    let (ct, dim) = parse_data_type(inner3.data_type())?;
                    Ok(NativeType::MultiPolygon(ct, dim))
                }
                _ => panic!(),
            },
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn parse_mixed(field: &Field) -> Result<(CoordType, Dimension)> {
    match field.data_type() {
        DataType::Union(fields, _) => {
            let mut coord_types: HashSet<CoordType> = HashSet::new();
            let mut dimensions: HashSet<Dimension> = HashSet::new();
            fields.iter().try_for_each(|(type_id, field)| {
                match type_id {
                    1 => match parse_point(field)? {
                        NativeType::Point(ct, Dimension::XY) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XY);
                        }
                        _ => unreachable!(),
                    },
                    2 => match parse_linestring(field)? {
                        NativeType::LineString(ct, Dimension::XY) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XY);
                        }
                        _ => unreachable!(),
                    },
                    3 => match parse_polygon(field)? {
                        NativeType::Polygon(ct, Dimension::XY) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XY);
                        }
                        _ => unreachable!(),
                    },
                    4 => match parse_multi_point(field)? {
                        NativeType::MultiPoint(ct, Dimension::XY) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XY);
                        }
                        _ => unreachable!(),
                    },
                    5 => match parse_multi_linestring(field)? {
                        NativeType::MultiLineString(ct, Dimension::XY) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XY);
                        }
                        _ => unreachable!(),
                    },
                    6 => match parse_multi_polygon(field)? {
                        NativeType::MultiPolygon(ct, Dimension::XY) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XY);
                        }
                        _ => unreachable!(),
                    },
                    7 => match parse_geometry_collection(field)? {
                        NativeType::GeometryCollection(ct, Dimension::XY) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XY);
                        }
                        _ => unreachable!(),
                    },
                    11 => match parse_point(field)? {
                        NativeType::Point(ct, Dimension::XYZ) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XYZ);
                        }
                        _ => unreachable!(),
                    },
                    12 => match parse_linestring(field)? {
                        NativeType::LineString(ct, Dimension::XYZ) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XYZ);
                        }
                        _ => unreachable!(),
                    },
                    13 => match parse_polygon(field)? {
                        NativeType::Polygon(ct, Dimension::XYZ) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XYZ);
                        }
                        _ => unreachable!(),
                    },
                    14 => match parse_multi_point(field)? {
                        NativeType::MultiPoint(ct, Dimension::XYZ) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XYZ);
                        }
                        _ => unreachable!(),
                    },
                    15 => match parse_multi_linestring(field)? {
                        NativeType::MultiLineString(ct, Dimension::XYZ) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XYZ);
                        }
                        _ => unreachable!(),
                    },
                    16 => match parse_multi_polygon(field)? {
                        NativeType::MultiPolygon(ct, Dimension::XYZ) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XYZ);
                        }
                        _ => unreachable!(),
                    },
                    17 => match parse_geometry_collection(field)? {
                        NativeType::GeometryCollection(ct, Dimension::XYZ) => {
                            coord_types.insert(ct);
                            dimensions.insert(Dimension::XYZ);
                        }
                        _ => unreachable!(),
                    },
                    id => panic!("unexpected type id {}", id),
                };
                Ok::<_, GeoArrowError>(())
            })?;

            if coord_types.len() > 1 {
                return Err(GeoArrowError::General(
                    "Multi coord types in union".to_string(),
                ));
            }
            if dimensions.len() > 1 {
                return Err(GeoArrowError::General(
                    "Multi dimensions types in union".to_string(),
                ));
            }

            let coord_type = coord_types.drain().next().unwrap();
            let dimension = dimensions.drain().next().unwrap();
            Ok((coord_type, dimension))
        }
        _ => panic!("Unexpected data type"),
    }
}

fn parse_geometry_collection(field: &Field) -> Result<NativeType> {
    // We need to parse the _inner_ type of the geometry collection as a union so that we can check
    // what coordinate type it's using.
    match field.data_type() {
        DataType::List(inner_field) | DataType::LargeList(inner_field) => {
            let (coord_type, dim) = parse_mixed(inner_field)?;
            Ok(NativeType::GeometryCollection(coord_type, dim))
        }
        _ => panic!(),
    }
}

fn parse_wkb(field: &Field) -> SerializedType {
    match field.data_type() {
        DataType::Binary => SerializedType::WKB,
        DataType::LargeBinary => SerializedType::LargeWKB,
        _ => panic!(),
    }
}

fn parse_wkt(field: &Field) -> SerializedType {
    match field.data_type() {
        DataType::Utf8 => SerializedType::WKT,
        DataType::LargeUtf8 => SerializedType::LargeWKT,
        _ => panic!(),
    }
}

fn parse_rect(field: &Field) -> NativeType {
    match field.data_type() {
        DataType::Struct(struct_fields) => match struct_fields.len() {
            4 => NativeType::Rect(Dimension::XY),
            6 => NativeType::Rect(Dimension::XYZ),
            _ => panic!("unexpected number of struct fields"),
        },
        _ => panic!("unexpected data type parsing rect"),
    }
}

fn parse_geometry(field: &Field) -> Result<NativeType> {
    if let DataType::Union(fields, _mode) = field.data_type() {
        let mut coord_types: HashSet<CoordType> = HashSet::new();

        fields.iter().try_for_each(|(type_id, field)| {
            match type_id {
                1 => match parse_point(field)? {
                    NativeType::Point(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                2 => match parse_linestring(field)? {
                    NativeType::LineString(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                3 => match parse_polygon(field)? {
                    NativeType::Polygon(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                4 => match parse_multi_point(field)? {
                    NativeType::MultiPoint(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                5 => match parse_multi_linestring(field)? {
                    NativeType::MultiLineString(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                6 => match parse_multi_polygon(field)? {
                    NativeType::MultiPolygon(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                7 => match parse_geometry_collection(field)? {
                    NativeType::GeometryCollection(ct, Dimension::XY) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                11 => match parse_point(field)? {
                    NativeType::Point(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                12 => match parse_linestring(field)? {
                    NativeType::LineString(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                13 => match parse_polygon(field)? {
                    NativeType::Polygon(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                14 => match parse_multi_point(field)? {
                    NativeType::MultiPoint(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                15 => match parse_multi_linestring(field)? {
                    NativeType::MultiLineString(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                16 => match parse_multi_polygon(field)? {
                    NativeType::MultiPolygon(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                17 => match parse_geometry_collection(field)? {
                    NativeType::GeometryCollection(ct, Dimension::XYZ) => {
                        coord_types.insert(ct);
                    }
                    _ => unreachable!(),
                },
                id => panic!("unexpected type id {}", id),
            };
            Ok::<_, GeoArrowError>(())
        })?;

        if coord_types.len() > 1 {
            return Err(GeoArrowError::General(
                "Multi coord types in union".to_string(),
            ));
        }

        let coord_type = coord_types.drain().next().unwrap();
        Ok(NativeType::Geometry(coord_type))
    } else {
        Err(GeoArrowError::General("Expected union type".to_string()))
    }
}

impl TryFrom<&Field> for NativeType {
    type Error = GeoArrowError;

    fn try_from(field: &Field) -> Result<Self> {
        if let Some(extension_name) = field.metadata().get("ARROW:extension:name") {
            let data_type = match extension_name.as_str() {
                "geoarrow.point" => parse_point(field)?,
                "geoarrow.linestring" => parse_linestring(field)?,
                "geoarrow.polygon" => parse_polygon(field)?,
                "geoarrow.multipoint" => parse_multi_point(field)?,
                "geoarrow.multilinestring" => parse_multi_linestring(field)?,
                "geoarrow.multipolygon" => parse_multi_polygon(field)?,
                "geoarrow.geometrycollection" => parse_geometry_collection(field)?,
                "geoarrow.box" => parse_rect(field),
                "geoarrow.geometry" => parse_geometry(field)?,
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
                    2 => NativeType::Point(CoordType::Separated, Dimension::XY),
                    3 => NativeType::Point(CoordType::Separated, Dimension::XYZ),
                    l => return Err(GeoArrowError::General(format!("incorrect number of struct fields {l}"))),
                },
                DataType::FixedSizeList(_, list_size) => NativeType::Point(CoordType::Interleaved, (*list_size as usize).try_into()?),
                _ => return Err(GeoArrowError::General("Only FixedSizeList and Struct arrays are unambigously typed for a GeoArrow native type and can be used without extension metadata.".to_string())),
            };
            Ok(data_type)
        }
    }
}

impl TryFrom<&Field> for SerializedType {
    type Error = GeoArrowError;

    fn try_from(field: &Field) -> Result<Self> {
        if let Some(extension_name) = field.metadata().get("ARROW:extension:name") {
            let data_type = match extension_name.as_str() {
                "geoarrow.wkb" | "ogc.wkb" => parse_wkb(field),
                "geoarrow.wkt" => parse_wkt(field),
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
                DataType::Binary => SerializedType::WKB,
                DataType::LargeBinary => SerializedType::LargeWKB,
                DataType::Utf8 => SerializedType::WKT,
                DataType::LargeUtf8 => SerializedType::LargeWKT,
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
