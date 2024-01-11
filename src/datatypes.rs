//! Contains the implementation of [`GeoDataType`], which defines all geometry arrays in this
//! crate.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_schema::{DataType, Field, UnionFields, UnionMode};

use crate::array::CoordType;
use crate::error::{GeoArrowError, Result};

/// The geometry type is designed to aid in downcasting from dynamically-typed geometry arrays by
/// uniquely identifying the physical buffer layout of each geometry array type.
///
/// It must always be possible to accurately downcast from a `dyn &GeometryArrayTrait` or `dyn
/// &ChunkedGeometryArrayTrait` to a unique concrete array type using this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GeoDataType {
    /// Represents a [PointArray][crate::array::PointArray] or
    /// [ChunkedPointArray][crate::chunked_array::ChunkedPointArray].
    Point(CoordType),

    /// Represents a [LineStringArray][crate::array::LineStringArray] or
    /// [ChunkedLineStringArray][crate::chunked_array::ChunkedLineStringArray] with `i32` offsets.
    LineString(CoordType),

    /// Represents a [LineStringArray][crate::array::LineStringArray] or
    /// [ChunkedLineStringArray][crate::chunked_array::ChunkedLineStringArray] with `i64` offsets.
    LargeLineString(CoordType),

    /// Represents a [PolygonArray][crate::array::PolygonArray] or
    /// [ChunkedPolygonArray][crate::chunked_array::ChunkedPolygonArray] with `i32` offsets.
    Polygon(CoordType),

    /// Represents a [PolygonArray][crate::array::PolygonArray] or
    /// [ChunkedPolygonArray][crate::chunked_array::ChunkedPolygonArray] with `i64` offsets.
    LargePolygon(CoordType),

    /// Represents a [MultiPointArray][crate::array::MultiPointArray] or
    /// [ChunkedMultiPointArray][crate::chunked_array::ChunkedMultiPointArray] with `i32` offsets.
    MultiPoint(CoordType),

    /// Represents a [MultiPointArray][crate::array::MultiPointArray] or
    /// [ChunkedMultiPointArray][crate::chunked_array::ChunkedMultiPointArray] with `i64` offsets.
    LargeMultiPoint(CoordType),

    /// Represents a [MultiLineStringArray][crate::array::MultiLineStringArray] or
    /// [ChunkedMultiLineStringArray][crate::chunked_array::ChunkedMultiLineStringArray] with `i32`
    /// offsets.
    MultiLineString(CoordType),

    /// Represents a [MultiLineStringArray][crate::array::MultiLineStringArray] or
    /// [ChunkedMultiLineStringArray][crate::chunked_array::ChunkedMultiLineStringArray] with `i64`
    /// offsets.
    LargeMultiLineString(CoordType),

    /// Represents a [MultiPolygonArray][crate::array::MultiPolygonArray] or
    /// [ChunkedMultiPolygonArray][crate::chunked_array::ChunkedMultiPolygonArray] with `i32`
    /// offsets.
    MultiPolygon(CoordType),

    /// Represents a [MultiPolygonArray][crate::array::MultiPolygonArray] or
    /// [ChunkedMultiPolygonArray][crate::chunked_array::ChunkedMultiPolygonArray] with `i64`
    /// offsets.
    LargeMultiPolygon(CoordType),

    /// Represents a [MixedGeometryArray][crate::array::MixedGeometryArray] or
    /// [ChunkedMixedGeometryArray][crate::chunked_array::ChunkedMixedGeometryArray] with `i32`
    /// offsets.
    Mixed(CoordType),

    /// Represents a [MixedGeometryArray][crate::array::MixedGeometryArray] or
    /// [ChunkedMixedGeometryArray][crate::chunked_array::ChunkedMixedGeometryArray] with `i64`
    /// offsets.
    LargeMixed(CoordType),

    /// Represents a [GeometryCollectionArray][crate::array::GeometryCollectionArray] or
    /// [ChunkedGeometryCollectionArray][crate::chunked_array::ChunkedGeometryCollectionArray] with
    /// `i32` offsets.
    GeometryCollection(CoordType),

    /// Represents a [GeometryCollectionArray][crate::array::GeometryCollectionArray] or
    /// [ChunkedGeometryCollectionArray][crate::chunked_array::ChunkedGeometryCollectionArray] with
    /// `i64` offsets.
    LargeGeometryCollection(CoordType),

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

fn coord_type_to_data_type(coord_type: &CoordType) -> DataType {
    match coord_type {
        CoordType::Interleaved => {
            let values_field = Field::new("xy", DataType::Float64, false);
            DataType::FixedSizeList(Arc::new(values_field), 2)
        }
        CoordType::Separated => {
            let values_fields = vec![
                Field::new("x", DataType::Float64, false),
                Field::new("y", DataType::Float64, false),
            ];
            DataType::Struct(values_fields.into())
        }
    }
}

// TODO: these are duplicated from the arrays
fn point_data_type(coord_type: &CoordType) -> DataType {
    coord_type_to_data_type(coord_type)
}

fn line_string_data_type<O: OffsetSizeTrait>(coord_type: &CoordType) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type);
    let vertices_field = Field::new("vertices", coords_type, false).into();
    match O::IS_LARGE {
        true => DataType::LargeList(vertices_field),
        false => DataType::List(vertices_field),
    }
}

fn polygon_data_type<O: OffsetSizeTrait>(coord_type: &CoordType) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type);
    let vertices_field = Field::new("vertices", coords_type, false);
    let rings_field = match O::IS_LARGE {
        true => Field::new_large_list("rings", vertices_field, true).into(),
        false => Field::new_list("rings", vertices_field, true).into(),
    };
    match O::IS_LARGE {
        true => DataType::LargeList(rings_field),
        false => DataType::List(rings_field),
    }
}

fn multi_point_data_type<O: OffsetSizeTrait>(coord_type: &CoordType) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type);
    let vertices_field = Field::new("points", coords_type, false).into();
    match O::IS_LARGE {
        true => DataType::LargeList(vertices_field),
        false => DataType::List(vertices_field),
    }
}

fn multi_line_string_data_type<O: OffsetSizeTrait>(coord_type: &CoordType) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type);
    let vertices_field = Field::new("vertices", coords_type, false);
    let linestrings_field = match O::IS_LARGE {
        true => Field::new_large_list("linestrings", vertices_field, true).into(),
        false => Field::new_list("linestrings", vertices_field, true).into(),
    };
    match O::IS_LARGE {
        true => DataType::LargeList(linestrings_field),
        false => DataType::List(linestrings_field),
    }
}

fn multi_polygon_data_type<O: OffsetSizeTrait>(coord_type: &CoordType) -> DataType {
    let coords_type = coord_type_to_data_type(coord_type);
    let vertices_field = Field::new("vertices", coords_type, false);
    let rings_field = match O::IS_LARGE {
        true => Field::new_large_list("rings", vertices_field, true),
        false => Field::new_list("rings", vertices_field, true),
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

fn mixed_data_type<O: OffsetSizeTrait>(coord_type: &CoordType) -> DataType {
    let mut fields: Vec<Arc<Field>> = vec![];
    let mut type_ids = vec![];

    // TODO: I _think_ it's ok to always push this type id mapping, and only the type ids that
    // actually show up in the data will be used.

    fields.push(GeoDataType::Point(*coord_type).to_field("", true).into());
    type_ids.push(1);

    let line_string_field = match O::IS_LARGE {
        true => GeoDataType::LargeLineString(*coord_type).to_field("", true),
        false => GeoDataType::LineString(*coord_type).to_field("", true),
    };
    fields.push(line_string_field.into());
    type_ids.push(2);

    let polygon_field = match O::IS_LARGE {
        true => GeoDataType::LargePolygon(*coord_type).to_field("", true),
        false => GeoDataType::Polygon(*coord_type).to_field("", true),
    };
    fields.push(polygon_field.into());
    type_ids.push(3);

    let multi_point_field = match O::IS_LARGE {
        true => GeoDataType::LargeMultiPoint(*coord_type).to_field("", true),
        false => GeoDataType::MultiPoint(*coord_type).to_field("", true),
    };
    fields.push(multi_point_field.into());
    type_ids.push(4);

    let multi_line_string_field = match O::IS_LARGE {
        true => GeoDataType::LargeMultiLineString(*coord_type).to_field("", true),
        false => GeoDataType::MultiLineString(*coord_type).to_field("", true),
    };
    fields.push(multi_line_string_field.into());
    type_ids.push(5);

    let multi_polygon_field = match O::IS_LARGE {
        true => GeoDataType::LargeMultiPolygon(*coord_type).to_field("", true),
        false => GeoDataType::MultiPolygon(*coord_type).to_field("", true),
    };
    fields.push(multi_polygon_field.into());
    type_ids.push(6);

    let union_fields = UnionFields::new(type_ids, fields);
    DataType::Union(union_fields, UnionMode::Dense)
}

fn geometry_collection_data_type<O: OffsetSizeTrait>(coord_type: &CoordType) -> DataType {
    let geometries_field = Field::new("geometries", mixed_data_type::<O>(coord_type), true).into();
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
            Point(coord_type) => point_data_type(coord_type),
            LineString(coord_type) => line_string_data_type::<i32>(coord_type),
            LargeLineString(coord_type) => line_string_data_type::<i64>(coord_type),
            Polygon(coord_type) => polygon_data_type::<i32>(coord_type),
            LargePolygon(coord_type) => polygon_data_type::<i64>(coord_type),
            MultiPoint(coord_type) => multi_point_data_type::<i32>(coord_type),
            LargeMultiPoint(coord_type) => multi_point_data_type::<i64>(coord_type),
            MultiLineString(coord_type) => multi_line_string_data_type::<i32>(coord_type),
            LargeMultiLineString(coord_type) => multi_line_string_data_type::<i64>(coord_type),
            MultiPolygon(coord_type) => multi_polygon_data_type::<i32>(coord_type),
            LargeMultiPolygon(coord_type) => multi_polygon_data_type::<i64>(coord_type),
            Mixed(coord_type) => mixed_data_type::<i32>(coord_type),
            LargeMixed(coord_type) => mixed_data_type::<i64>(coord_type),
            GeometryCollection(coord_type) => geometry_collection_data_type::<i32>(coord_type),
            LargeGeometryCollection(coord_type) => geometry_collection_data_type::<i64>(coord_type),
            WKB => wkb_data_type::<i32>(),
            LargeWKB => wkb_data_type::<i64>(),
            Rect => rect_data_type(),
        }
    }

    /// Get the GeoArrow extension name pertaining to this data type.
    pub fn extension_name(&self) -> &'static str {
        use GeoDataType::*;
        match self {
            Point(_) => "geoarrow.point",
            LineString(_) | LargeLineString(_) => "geoarrow.linestring",
            Polygon(_) | LargePolygon(_) => "geoarrow.polygon",
            MultiPoint(_) | LargeMultiPoint(_) => "geoarrow.multipoint",
            MultiLineString(_) | LargeMultiLineString(_) => "geoarrow.multilinestring",
            MultiPolygon(_) | LargeMultiPolygon(_) => "geoarrow.multipolygon",
            Mixed(_) | LargeMixed(_) => "geoarrow.geometry",
            GeometryCollection(_) | LargeGeometryCollection(_) => "geoarrow.geometrycollection",
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
}

fn data_type_to_coord_type(data_type: &DataType) -> CoordType {
    match data_type {
        DataType::FixedSizeList(_, _) => CoordType::Interleaved,
        DataType::Struct(_) => CoordType::Separated,
        _ => panic!(),
    }
}

fn parse_point(field: &Field) -> GeoDataType {
    GeoDataType::Point(data_type_to_coord_type(field.data_type()))
}

fn parse_linestring(field: &Field) -> GeoDataType {
    match field.data_type() {
        DataType::List(inner_field) => {
            GeoDataType::LineString(data_type_to_coord_type(inner_field.data_type()))
        }
        DataType::LargeList(inner_field) => {
            GeoDataType::LargeLineString(data_type_to_coord_type(inner_field.data_type()))
        }
        _ => panic!(),
    }
}

fn parse_polygon(field: &Field) -> GeoDataType {
    match field.data_type() {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => {
                GeoDataType::Polygon(data_type_to_coord_type(inner2.data_type()))
            }
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => {
                GeoDataType::LargePolygon(data_type_to_coord_type(inner2.data_type()))
            }
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn parse_multi_point(field: &Field) -> GeoDataType {
    match field.data_type() {
        DataType::List(inner_field) => {
            GeoDataType::MultiPoint(data_type_to_coord_type(inner_field.data_type()))
        }
        DataType::LargeList(inner_field) => {
            GeoDataType::LargeMultiPoint(data_type_to_coord_type(inner_field.data_type()))
        }
        _ => panic!(),
    }
}

fn parse_multi_linestring(field: &Field) -> GeoDataType {
    match field.data_type() {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => {
                GeoDataType::MultiLineString(data_type_to_coord_type(inner2.data_type()))
            }
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => {
                GeoDataType::LargeMultiLineString(data_type_to_coord_type(inner2.data_type()))
            }
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn parse_multi_polygon(field: &Field) -> GeoDataType {
    match field.data_type() {
        DataType::List(inner1) => match inner1.data_type() {
            DataType::List(inner2) => match inner2.data_type() {
                DataType::List(inner3) => {
                    GeoDataType::MultiPolygon(data_type_to_coord_type(inner3.data_type()))
                }
                _ => panic!(),
            },
            _ => panic!(),
        },
        DataType::LargeList(inner1) => match inner1.data_type() {
            DataType::LargeList(inner2) => match inner2.data_type() {
                DataType::LargeList(inner3) => {
                    GeoDataType::LargeMultiPolygon(data_type_to_coord_type(inner3.data_type()))
                }
                _ => panic!(),
            },
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn parse_geometry(field: &Field) -> GeoDataType {
    match field.data_type() {
        DataType::Union(fields, _) => {
            let mut coord_types: HashSet<CoordType> = HashSet::new();
            // let mut data_types = Vec::with_capacity(fields.len());
            fields.iter().for_each(|(type_id, field)| {
                match type_id {
                    1 => match parse_point(field) {
                        GeoDataType::Point(ct) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    2 => match parse_linestring(field) {
                        GeoDataType::LineString(ct) => coord_types.insert(ct),
                        GeoDataType::LargeLineString(ct) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    3 => match parse_polygon(field) {
                        GeoDataType::Polygon(ct) => coord_types.insert(ct),
                        GeoDataType::LargePolygon(ct) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    4 => match parse_multi_point(field) {
                        GeoDataType::MultiPoint(ct) => coord_types.insert(ct),
                        GeoDataType::LargeMultiPoint(ct) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    5 => match parse_multi_linestring(field) {
                        GeoDataType::MultiLineString(ct) => coord_types.insert(ct),
                        GeoDataType::LargeMultiLineString(ct) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    6 => match parse_multi_polygon(field) {
                        GeoDataType::MultiPolygon(ct) => coord_types.insert(ct),
                        GeoDataType::LargeMultiPolygon(ct) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    7 => match parse_geometry_collection(field) {
                        GeoDataType::GeometryCollection(ct) => coord_types.insert(ct),
                        GeoDataType::LargeGeometryCollection(ct) => coord_types.insert(ct),
                        _ => unreachable!(),
                    },
                    id => panic!("unexpected type id {}", id),
                };
            });

            if coord_types.len() > 1 {
                panic!("Multi coord types in union");
            }

            let coord_type = coord_types.drain().next().unwrap();
            GeoDataType::Mixed(coord_type)
        }
        _ => panic!("Unexpected data type"),
    }
}

fn parse_geometry_collection(field: &Field) -> GeoDataType {
    // We need to parse the _inner_ type of the geometry collection as a union so that we can check
    // what coordinate type it's using.
    match field.data_type() {
        DataType::List(inner_field) => match parse_geometry(inner_field) {
            GeoDataType::Mixed(coord_type) => GeoDataType::GeometryCollection(coord_type),
            _ => panic!(),
        },
        DataType::LargeList(inner_field) => match parse_geometry(inner_field) {
            GeoDataType::LargeMixed(coord_type) => GeoDataType::LargeGeometryCollection(coord_type),
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
                "geoarrow.point" => parse_point(field),
                "geoarrow.linestring" => parse_linestring(field),
                "geoarrow.polygon" => parse_polygon(field),
                "geoarrow.multipoint" => parse_multi_point(field),
                "geoarrow.multilinestring" => parse_multi_linestring(field),
                "geoarrow.multipolygon" => parse_multi_polygon(field),
                "geoarrow.geometry" => parse_geometry(field),
                "geoarrow.geometrycollection" => parse_geometry_collection(field),
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
            DataType::Struct(_) => {
                GeoDataType::Point(CoordType::Separated)
            }
            DataType::FixedSizeList(_, _) => {
                GeoDataType::Point(CoordType::Interleaved)
            }
            _ => return Err(GeoArrowError::General("Only Binary, LargeBinary, FixedSizeList, and Struct arrays are unambigously typed and can be used without extension metadata.".to_string()))
        };
            Ok(data_type)
        }
    }
}
