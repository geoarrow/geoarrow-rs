//! Contains the implementation of [`GeoDataType`], which defines all geometry arrays in this
//! crate.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};

use crate::array::metadata::ArrayMetadata;
use crate::array::CoordType;
use crate::error::{GeoArrowError, Result};

/// The dimension of the geometry array
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dimension {
    XY,
    XYZ,
}

impl Dimension {
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

/// The geometry type is designed to aid in downcasting from dynamically-typed geometry arrays by
/// uniquely identifying the physical buffer layout of each geometry array type.
///
/// It must always be possible to accurately downcast from a `dyn &GeometryArrayTrait` or `dyn
/// &ChunkedGeometryArrayTrait` to a unique concrete array type using this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GeoDataType {
    /// Represents a [PointArray][crate::array::PointArray] or
    /// [ChunkedPointArray][crate::chunked_array::ChunkedPointArray].
    Point(CoordType, Dimension),

    /// Represents a [LineStringArray][crate::array::LineStringArray] or
    /// [ChunkedLineStringArray][crate::chunked_array::ChunkedLineStringArray] with `i32` offsets.
    LineString(CoordType, Dimension),

    /// Represents a [LineStringArray][crate::array::LineStringArray] or
    /// [ChunkedLineStringArray][crate::chunked_array::ChunkedLineStringArray] with `i64` offsets.
    LargeLineString(CoordType, Dimension),

    /// Represents a [PolygonArray][crate::array::PolygonArray] or
    /// [ChunkedPolygonArray][crate::chunked_array::ChunkedPolygonArray] with `i32` offsets.
    Polygon(CoordType, Dimension),

    /// Represents a [PolygonArray][crate::array::PolygonArray] or
    /// [ChunkedPolygonArray][crate::chunked_array::ChunkedPolygonArray] with `i64` offsets.
    LargePolygon(CoordType, Dimension),

    /// Represents a [MultiPointArray][crate::array::MultiPointArray] or
    /// [ChunkedMultiPointArray][crate::chunked_array::ChunkedMultiPointArray] with `i32` offsets.
    MultiPoint(CoordType, Dimension),

    /// Represents a [MultiPointArray][crate::array::MultiPointArray] or
    /// [ChunkedMultiPointArray][crate::chunked_array::ChunkedMultiPointArray] with `i64` offsets.
    LargeMultiPoint(CoordType, Dimension),

    /// Represents a [MultiLineStringArray][crate::array::MultiLineStringArray] or
    /// [ChunkedMultiLineStringArray][crate::chunked_array::ChunkedMultiLineStringArray] with `i32`
    /// offsets.
    MultiLineString(CoordType, Dimension),

    /// Represents a [MultiLineStringArray][crate::array::MultiLineStringArray] or
    /// [ChunkedMultiLineStringArray][crate::chunked_array::ChunkedMultiLineStringArray] with `i64`
    /// offsets.
    LargeMultiLineString(CoordType, Dimension),

    /// Represents a [MultiPolygonArray][crate::array::MultiPolygonArray] or
    /// [ChunkedMultiPolygonArray][crate::chunked_array::ChunkedMultiPolygonArray] with `i32`
    /// offsets.
    MultiPolygon(CoordType, Dimension),

    /// Represents a [MultiPolygonArray][crate::array::MultiPolygonArray] or
    /// [ChunkedMultiPolygonArray][crate::chunked_array::ChunkedMultiPolygonArray] with `i64`
    /// offsets.
    LargeMultiPolygon(CoordType, Dimension),

    /// Represents a [MixedGeometryArray][crate::array::MixedGeometryArray] or
    /// [ChunkedMixedGeometryArray][crate::chunked_array::ChunkedMixedGeometryArray] with `i32`
    /// offsets.
    Mixed(CoordType, Dimension),

    /// Represents a [MixedGeometryArray][crate::array::MixedGeometryArray] or
    /// [ChunkedMixedGeometryArray][crate::chunked_array::ChunkedMixedGeometryArray] with `i64`
    /// offsets.
    LargeMixed(CoordType, Dimension),

    /// Represents a [GeometryCollectionArray][crate::array::GeometryCollectionArray] or
    /// [ChunkedGeometryCollectionArray][crate::chunked_array::ChunkedGeometryCollectionArray] with
    /// `i32` offsets.
    GeometryCollection(CoordType, Dimension),

    /// Represents a [GeometryCollectionArray][crate::array::GeometryCollectionArray] or
    /// [ChunkedGeometryCollectionArray][crate::chunked_array::ChunkedGeometryCollectionArray] with
    /// `i64` offsets.
    LargeGeometryCollection(CoordType, Dimension),

    /// Represents a [WKBArray][crate::array::WKBArray] or
    /// [ChunkedWKBArray][crate::chunked_array::ChunkedWKBArray] with `i32` offsets.
    WKB,

    /// Represents a [WKBArray][crate::array::WKBArray] or
    /// [ChunkedWKBArray][crate::chunked_array::ChunkedWKBArray] with `i64` offsets.
    LargeWKB,

    /// Represents a [RectArray][crate::array::RectArray] or
    /// [ChunkedRectArray][crate::chunked_array::ChunkedRectArray].
    Rect,
}

fn coord_type_to_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
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

// TODO: these are duplicated from the arrays
fn point_data_type(coord_type: CoordType, dim: Dimension) -> DataType {
    coord_type_to_data_type(coord_type, dim)
}

fn line_string_data_type<O: OffsetSizeTrait>(coord_type: CoordType, dim: Dimension) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type, dim);
    let vertices_field = Field::new("vertices", coords_type, false).into();
    match O::IS_LARGE {
        true => DataType::LargeList(vertices_field),
        false => DataType::List(vertices_field),
    }
}

fn polygon_data_type<O: OffsetSizeTrait>(coord_type: CoordType, dim: Dimension) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type, dim);
    let vertices_field = Field::new("vertices", coords_type, false);
    let rings_field = match O::IS_LARGE {
        true => Field::new_large_list("rings", vertices_field, false).into(),
        false => Field::new_list("rings", vertices_field, false).into(),
    };
    match O::IS_LARGE {
        true => DataType::LargeList(rings_field),
        false => DataType::List(rings_field),
    }
}

fn multi_point_data_type<O: OffsetSizeTrait>(coord_type: CoordType, dim: Dimension) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type, dim);
    let vertices_field = Field::new("points", coords_type, false).into();
    match O::IS_LARGE {
        true => DataType::LargeList(vertices_field),
        false => DataType::List(vertices_field),
    }
}

fn multi_line_string_data_type<O: OffsetSizeTrait>(
    coord_type: CoordType,
    dim: Dimension,
) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type, dim);
    let vertices_field = Field::new("vertices", coords_type, false);
    let linestrings_field = match O::IS_LARGE {
        true => Field::new_large_list("linestrings", vertices_field, false).into(),
        false => Field::new_list("linestrings", vertices_field, false).into(),
    };
    match O::IS_LARGE {
        true => DataType::LargeList(linestrings_field),
        false => DataType::List(linestrings_field),
    }
}

fn multi_polygon_data_type<O: OffsetSizeTrait>(coord_type: CoordType, dim: Dimension) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type, dim);
    let vertices_field = Field::new("vertices", coords_type, false);
    let rings_field = match O::IS_LARGE {
        true => Field::new_large_list("rings", vertices_field, false),
        false => Field::new_list("rings", vertices_field, false),
    };
    let polygons_field = match O::IS_LARGE {
        true => Field::new_large_list("polygons", rings_field, false).into(),
        false => Field::new_list("polygons", rings_field, false).into(),
    };
    match O::IS_LARGE {
        true => DataType::LargeList(polygons_field),
        false => DataType::List(polygons_field),
    }
}

fn mixed_data_type<O: OffsetSizeTrait>(coord_type: CoordType, dim: Dimension) -> DataType {
    let mut fields: Vec<Arc<Field>> = vec![];
    let mut type_ids = vec![];

    // TODO: I _think_ it's ok to always push this type id mapping, and only the type ids that
    // actually show up in the data will be used.

    fields.push(
        GeoDataType::Point(coord_type, dim)
            .to_field("", true)
            .into(),
    );
    type_ids.push(1);

    let line_string_field = match O::IS_LARGE {
        true => GeoDataType::LargeLineString(coord_type, dim).to_field("", true),
        false => GeoDataType::LineString(coord_type, dim).to_field("", true),
    };
    fields.push(line_string_field.into());
    type_ids.push(2);

    let polygon_field = match O::IS_LARGE {
        true => GeoDataType::LargePolygon(coord_type, dim).to_field("", true),
        false => GeoDataType::Polygon(coord_type, dim).to_field("", true),
    };
    fields.push(polygon_field.into());
    type_ids.push(3);

    let multi_point_field = match O::IS_LARGE {
        true => GeoDataType::LargeMultiPoint(coord_type, dim).to_field("", true),
        false => GeoDataType::MultiPoint(coord_type, dim).to_field("", true),
    };
    fields.push(multi_point_field.into());
    type_ids.push(4);

    let multi_line_string_field = match O::IS_LARGE {
        true => GeoDataType::LargeMultiLineString(coord_type, dim).to_field("", true),
        false => GeoDataType::MultiLineString(coord_type, dim).to_field("", true),
    };
    fields.push(multi_line_string_field.into());
    type_ids.push(5);

    let multi_polygon_field = match O::IS_LARGE {
        true => GeoDataType::LargeMultiPolygon(coord_type, dim).to_field("", true),
        false => GeoDataType::MultiPolygon(coord_type, dim).to_field("", true),
    };
    fields.push(multi_polygon_field.into());
    type_ids.push(6);

    let union_fields = UnionFields::new(type_ids, fields);
    DataType::Union(union_fields, UnionMode::Dense)
}

fn geometry_collection_data_type<O: OffsetSizeTrait>(
    coord_type: CoordType,
    dim: Dimension,
) -> DataType {
    let geometries_field =
        Field::new("geometries", mixed_data_type::<O>(coord_type, dim), false).into();
    match O::IS_LARGE {
        true => DataType::LargeList(geometries_field),
        false => DataType::List(geometries_field),
    }
}

fn wkb_data_type<O: OffsetSizeTrait>() -> DataType {
    match O::IS_LARGE {
        true => DataType::LargeBinary,
        false => DataType::Binary,
    }
}

fn rect_data_type() -> DataType {
    let inner_field = Field::new("rect", DataType::Float64, false).into();
    DataType::FixedSizeList(inner_field, 4)
}

impl GeoDataType {
    /// Convert a [`GeoDataType`] into the relevant arrow [`DataType`].
    ///
    /// Note that an arrow [`DataType`] will lose the accompanying GeoArrow metadata if it is not
    /// part of a [`Field`] with GeoArrow extension metadata in its field metadata.
    pub fn to_data_type(&self) -> DataType {
        use GeoDataType::*;
        match self {
            Point(coord_type, dim) => point_data_type(*coord_type, *dim),
            LineString(coord_type, dim) => line_string_data_type::<i32>(*coord_type, *dim),
            LargeLineString(coord_type, dim) => line_string_data_type::<i64>(*coord_type, *dim),
            Polygon(coord_type, dim) => polygon_data_type::<i32>(*coord_type, *dim),
            LargePolygon(coord_type, dim) => polygon_data_type::<i64>(*coord_type, *dim),
            MultiPoint(coord_type, dim) => multi_point_data_type::<i32>(*coord_type, *dim),
            LargeMultiPoint(coord_type, dim) => multi_point_data_type::<i64>(*coord_type, *dim),
            MultiLineString(coord_type, dim) => {
                multi_line_string_data_type::<i32>(*coord_type, *dim)
            }
            LargeMultiLineString(coord_type, dim) => {
                multi_line_string_data_type::<i64>(*coord_type, *dim)
            }
            MultiPolygon(coord_type, dim) => multi_polygon_data_type::<i32>(*coord_type, *dim),
            LargeMultiPolygon(coord_type, dim) => multi_polygon_data_type::<i64>(*coord_type, *dim),
            Mixed(coord_type, dim) => mixed_data_type::<i32>(*coord_type, *dim),
            LargeMixed(coord_type, dim) => mixed_data_type::<i64>(*coord_type, *dim),
            GeometryCollection(coord_type, dim) => {
                geometry_collection_data_type::<i32>(*coord_type, *dim)
            }
            LargeGeometryCollection(coord_type, dim) => {
                geometry_collection_data_type::<i64>(*coord_type, *dim)
            }
            WKB => wkb_data_type::<i32>(),
            LargeWKB => wkb_data_type::<i64>(),
            Rect => rect_data_type(),
        }
    }

    /// Get the GeoArrow extension name pertaining to this data type.
    pub fn extension_name(&self) -> &'static str {
        use GeoDataType::*;
        match self {
            Point(_, _) => "geoarrow.point",
            LineString(_, _) | LargeLineString(_, _) => "geoarrow.linestring",
            Polygon(_, _) | LargePolygon(_, _) => "geoarrow.polygon",
            MultiPoint(_, _) | LargeMultiPoint(_, _) => "geoarrow.multipoint",
            MultiLineString(_, _) | LargeMultiLineString(_, _) => "geoarrow.multilinestring",
            MultiPolygon(_, _) | LargeMultiPolygon(_, _) => "geoarrow.multipolygon",
            Mixed(_, _) | LargeMixed(_, _) => "geoarrow.geometry",
            GeometryCollection(_, _) | LargeGeometryCollection(_, _) => {
                "geoarrow.geometrycollection"
            }
            WKB | LargeWKB => "geoarrow.wkb",
            Rect => unimplemented!(),
        }
    }

    /// Convert this [`GeoDataType`] into an arrow [`Field`], maintaining GeoArrow extension
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

    pub fn to_field_with_metadata<N: Into<String>>(
        &self,
        name: N,
        nullable: bool,
        array_metadata: &ArrayMetadata,
    ) -> Field {
        let extension_name = self.extension_name();
        let mut metadata = HashMap::with_capacity(1);
        metadata.insert(
            "ARROW:extension:name".to_string(),
            extension_name.to_string(),
        );
        metadata.insert(
            "ARROW:extension:metadata".to_string(),
            serde_json::to_string(array_metadata).unwrap(),
        );
        Field::new(name, self.to_data_type(), nullable).with_metadata(metadata)
    }

    pub fn with_coord_type(self, coord_type: CoordType) -> GeoDataType {
        use GeoDataType::*;
        match self {
            Point(_, dim) => Point(coord_type, dim),
            LineString(_, dim) => LineString(coord_type, dim),
            LargeLineString(_, dim) => LargeLineString(coord_type, dim),
            Polygon(_, dim) => Polygon(coord_type, dim),
            LargePolygon(_, dim) => LargePolygon(coord_type, dim),
            MultiPoint(_, dim) => MultiPoint(coord_type, dim),
            LargeMultiPoint(_, dim) => LargeMultiPoint(coord_type, dim),
            MultiLineString(_, dim) => MultiLineString(coord_type, dim),
            LargeMultiLineString(_, dim) => LargeMultiLineString(coord_type, dim),
            MultiPolygon(_, dim) => MultiPolygon(coord_type, dim),
            LargeMultiPolygon(_, dim) => LargeMultiPolygon(coord_type, dim),
            Mixed(_, dim) => Mixed(coord_type, dim),
            LargeMixed(_, dim) => LargeMixed(coord_type, dim),
            GeometryCollection(_, dim) => GeometryCollection(coord_type, dim),
            LargeGeometryCollection(_, dim) => LargeGeometryCollection(coord_type, dim),
            WKB => WKB,
            LargeWKB => LargeWKB,
            Rect => Rect,
        }
    }

    pub fn with_dimension(self, dim: Dimension) -> GeoDataType {
        use GeoDataType::*;
        match self {
            Point(coord_type, _) => Point(coord_type, dim),
            LineString(coord_type, _) => LineString(coord_type, dim),
            LargeLineString(coord_type, _) => LargeLineString(coord_type, dim),
            Polygon(coord_type, _) => Polygon(coord_type, dim),
            LargePolygon(coord_type, _) => LargePolygon(coord_type, dim),
            MultiPoint(coord_type, _) => MultiPoint(coord_type, dim),
            LargeMultiPoint(coord_type, _) => LargeMultiPoint(coord_type, dim),
            MultiLineString(coord_type, _) => MultiLineString(coord_type, dim),
            LargeMultiLineString(coord_type, _) => LargeMultiLineString(coord_type, dim),
            MultiPolygon(coord_type, _) => MultiPolygon(coord_type, dim),
            LargeMultiPolygon(coord_type, _) => LargeMultiPolygon(coord_type, dim),
            Mixed(coord_type, _) => Mixed(coord_type, dim),
            LargeMixed(coord_type, _) => LargeMixed(coord_type, dim),
            GeometryCollection(coord_type, _) => GeometryCollection(coord_type, dim),
            LargeGeometryCollection(coord_type, _) => LargeGeometryCollection(coord_type, dim),
            WKB => WKB,
            LargeWKB => LargeWKB,
            Rect => Rect,
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

fn parse_point(field: &Field) -> Result<GeoDataType> {
    let (ct, dim) = parse_data_type(field.data_type())?;
    Ok(GeoDataType::Point(ct, dim))
}

fn parse_linestring(field: &Field) -> Result<GeoDataType> {
    match field.data_type() {
        DataType::List(inner_field) | DataType::LargeList(inner_field) => {
            let (ct, dim) = parse_data_type(inner_field.data_type())?;
            Ok(GeoDataType::LineString(ct, dim))
        }
        dt => Err(GeoArrowError::General(format!("Unexpected data type {dt}"))),
    }
}

fn parse_polygon(field: &Field) -> Result<GeoDataType> {
    match field.data_type() {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => {
                let (ct, dim) = parse_data_type(inner2.data_type())?;
                Ok(GeoDataType::Polygon(ct, dim))
            }
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => {
                let (ct, dim) = parse_data_type(inner2.data_type())?;
                Ok(GeoDataType::LargePolygon(ct, dim))
            }
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn parse_multi_point(field: &Field) -> Result<GeoDataType> {
    match field.data_type() {
        DataType::List(inner_field) => {
            let (ct, dim) = parse_data_type(inner_field.data_type())?;
            Ok(GeoDataType::MultiPoint(ct, dim))
        }
        DataType::LargeList(inner_field) => {
            let (ct, dim) = parse_data_type(inner_field.data_type())?;
            Ok(GeoDataType::LargeMultiPoint(ct, dim))
        }
        _ => panic!(),
    }
}

fn parse_multi_linestring(field: &Field) -> Result<GeoDataType> {
    match field.data_type() {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => {
                let (ct, dim) = parse_data_type(inner2.data_type())?;
                Ok(GeoDataType::MultiLineString(ct, dim))
            }
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => {
                let (ct, dim) = parse_data_type(inner2.data_type())?;
                Ok(GeoDataType::LargeMultiLineString(ct, dim))
            }
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn parse_multi_polygon(field: &Field) -> Result<GeoDataType> {
    match field.data_type() {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => match inner2.data_type() {
                DataType::List(inner3) => {
                    let (ct, dim) = parse_data_type(inner3.data_type())?;
                    Ok(GeoDataType::MultiPolygon(ct, dim))
                }
                _ => panic!(),
            },
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => match inner2.data_type() {
                DataType::LargeList(inner3) => {
                    let (ct, dim) = parse_data_type(inner3.data_type())?;
                    Ok(GeoDataType::LargeMultiPolygon(ct, dim))
                }
                _ => panic!(),
            },
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn parse_geometry(field: &Field) -> Result<GeoDataType> {
    match field.data_type() {
        DataType::Union(fields, _) => {
            let mut coord_types: HashSet<CoordType> = HashSet::new();
            let mut dimensions: HashSet<Dimension> = HashSet::new();
            // let mut data_types = Vec::with_capacity(fields.len());
            fields.iter().try_for_each(|(type_id, field)| {
                match type_id {
                    1 => match parse_point(field)? {
                        GeoDataType::Point(ct, Dimension::XY) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    2 => match parse_linestring(field)? {
                        GeoDataType::LineString(ct, Dimension::XY) => coord_types.insert(ct),
                        GeoDataType::LargeLineString(ct, Dimension::XY) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    3 => match parse_polygon(field)? {
                        GeoDataType::Polygon(ct, Dimension::XY) => coord_types.insert(ct),
                        GeoDataType::LargePolygon(ct, Dimension::XY) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    4 => match parse_multi_point(field)? {
                        GeoDataType::MultiPoint(ct, Dimension::XY) => coord_types.insert(ct),
                        GeoDataType::LargeMultiPoint(ct, Dimension::XY) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    5 => match parse_multi_linestring(field)? {
                        GeoDataType::MultiLineString(ct, Dimension::XY) => coord_types.insert(ct),
                        GeoDataType::LargeMultiLineString(ct, Dimension::XY) => {
                            coord_types.insert(ct)
                        }
                        _ => unreachable!(),
                    },
                    6 => match parse_multi_polygon(field)? {
                        GeoDataType::MultiPolygon(ct, Dimension::XY) => coord_types.insert(ct),
                        GeoDataType::LargeMultiPolygon(ct, Dimension::XY) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    7 => match parse_geometry_collection(field)? {
                        GeoDataType::GeometryCollection(ct, Dimension::XY) => {
                            coord_types.insert(ct)
                        }
                        GeoDataType::LargeGeometryCollection(ct, Dimension::XY) => {
                            coord_types.insert(ct)
                        }
                        _ => unreachable!(),
                    },
                    11 => match parse_point(field)? {
                        GeoDataType::Point(ct, Dimension::XYZ) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    12 => match parse_linestring(field)? {
                        GeoDataType::LineString(ct, Dimension::XYZ) => coord_types.insert(ct),
                        GeoDataType::LargeLineString(ct, Dimension::XYZ) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    13 => match parse_polygon(field)? {
                        GeoDataType::Polygon(ct, Dimension::XYZ) => coord_types.insert(ct),
                        GeoDataType::LargePolygon(ct, Dimension::XYZ) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    14 => match parse_multi_point(field)? {
                        GeoDataType::MultiPoint(ct, Dimension::XYZ) => coord_types.insert(ct),
                        GeoDataType::LargeMultiPoint(ct, Dimension::XYZ) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    15 => match parse_multi_linestring(field)? {
                        GeoDataType::MultiLineString(ct, Dimension::XYZ) => coord_types.insert(ct),
                        GeoDataType::LargeMultiLineString(ct, Dimension::XYZ) => {
                            coord_types.insert(ct)
                        }
                        _ => unreachable!(),
                    },
                    16 => match parse_multi_polygon(field)? {
                        GeoDataType::MultiPolygon(ct, Dimension::XYZ) => coord_types.insert(ct),
                        GeoDataType::LargeMultiPolygon(ct, Dimension::XYZ) => {
                            coord_types.insert(ct)
                        }
                        _ => unreachable!(),
                    },
                    17 => match parse_geometry_collection(field)? {
                        GeoDataType::GeometryCollection(ct, Dimension::XYZ) => {
                            coord_types.insert(ct)
                        }
                        GeoDataType::LargeGeometryCollection(ct, Dimension::XYZ) => {
                            coord_types.insert(ct)
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
            Ok(GeoDataType::Mixed(coord_type, dimension))
        }
        _ => panic!("Unexpected data type"),
    }
}

fn parse_geometry_collection(field: &Field) -> Result<GeoDataType> {
    // We need to parse the _inner_ type of the geometry collection as a union so that we can check
    // what coordinate type it's using.
    match field.data_type() {
        DataType::List(inner_field) => match parse_geometry(inner_field)? {
            GeoDataType::Mixed(coord_type, dim) => {
                Ok(GeoDataType::GeometryCollection(coord_type, dim))
            }
            _ => panic!(),
        },
        DataType::LargeList(inner_field) => match parse_geometry(inner_field)? {
            GeoDataType::LargeMixed(coord_type, dim) => {
                Ok(GeoDataType::LargeGeometryCollection(coord_type, dim))
            }
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn parse_wkb(field: &Field) -> GeoDataType {
    match field.data_type() {
        DataType::Binary => GeoDataType::WKB,
        DataType::LargeBinary => GeoDataType::LargeWKB,
        _ => panic!(),
    }
}

impl TryFrom<&Field> for GeoDataType {
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
                "geoarrow.geometry" => parse_geometry(field)?,
                "geoarrow.geometrycollection" => parse_geometry_collection(field)?,
                "geoarrow.wkb" | "ogc.wkb" => parse_wkb(field),
                name => {
                    return Err(GeoArrowError::General(format!(
                        "Unexpected extension name {}",
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
            DataType::Binary => {
                GeoDataType::WKB
            }
            DataType::LargeBinary => {
                GeoDataType::LargeWKB
            }
            DataType::Struct(struct_fields) => {
                match struct_fields.len() {
                    2 => GeoDataType::Point(CoordType::Separated, Dimension::XY),
                    3 => GeoDataType::Point(CoordType::Separated, Dimension::XYZ),
                    l => return Err(GeoArrowError::General(format!("incorrect number of struct fields {l}") ))
                }
            }
            DataType::FixedSizeList(_, list_size) => {
                GeoDataType::Point(CoordType::Interleaved, (*list_size as usize).try_into()?)
            }
            _ => return Err(GeoArrowError::General("Only Binary, LargeBinary, FixedSizeList, and Struct arrays are unambigously typed and can be used without extension metadata.".to_string()))
        };
            Ok(data_type)
        }
    }
}
