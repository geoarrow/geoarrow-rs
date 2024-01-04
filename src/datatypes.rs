use std::collections::HashSet;

use arrow_schema::{DataType, Field};

use crate::array::CoordType;
use crate::error::{GeoArrowError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GeoDataType {
    Point(CoordType),
    LineString(CoordType),
    LargeLineString(CoordType),
    Polygon(CoordType),
    LargePolygon(CoordType),
    MultiPoint(CoordType),
    LargeMultiPoint(CoordType),
    MultiLineString(CoordType),
    LargeMultiLineString(CoordType),
    MultiPolygon(CoordType),
    LargeMultiPolygon(CoordType),
    Mixed(CoordType),
    LargeMixed(CoordType),
    GeometryCollection(CoordType),
    LargeGeometryCollection(CoordType),
    WKB,
    LargeWKB,
    Rect,
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
                "geoarrow.wkb" => parse_wkb(field),
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
