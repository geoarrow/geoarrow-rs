#![allow(non_upper_case_globals)]

use arrow_array::OffsetSizeTrait;
use num_enum::TryFromPrimitive;

use crate::array::CoordType;
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::WKB;

#[derive(Debug, Eq, PartialEq, TryFromPrimitive)]
#[repr(u32)]
pub enum WKBGeometryType {
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
    GeometryCollection = 7,
}

struct AvailableTypes {
    point: bool,
    line_string: bool,
    polygon: bool,
    multi_point: bool,
    multi_line_string: bool,
    multi_polygon: bool,
    mixed: bool,
}

// TODO: in the future, this should also count coord sizes in this same pass, so that it doesn't
// need another pass to figure out how much to allocate.
impl AvailableTypes {
    pub fn new() -> AvailableTypes {
        Self {
            point: true,
            line_string: true,
            polygon: true,
            multi_point: true,
            multi_line_string: true,
            multi_polygon: true,
            mixed: true,
        }
    }

    /// Check if all types are true. Implies that no geometries have been added
    fn all_true(&self) -> bool {
        self.point
            && self.line_string
            && self.polygon
            && self.multi_point
            && self.multi_line_string
            && self.multi_polygon
            && self.mixed
    }

    pub fn add_point(&mut self) {
        self.line_string = false;
        self.polygon = false;
        self.multi_line_string = false;
        self.multi_polygon = false;
    }

    pub fn add_line_string(&mut self) {
        self.point = false;
        self.polygon = false;
        self.multi_point = false;
        self.multi_polygon = false;
    }

    pub fn add_polygon(&mut self) {
        self.point = false;
        self.line_string = false;
        self.multi_point = false;
        self.multi_line_string = false;
    }

    pub fn add_multi_point(&mut self) {
        self.point = false;
        self.line_string = false;
        self.polygon = false;
        self.multi_line_string = false;
        self.multi_polygon = false;
    }

    pub fn add_multi_line_string(&mut self) {
        self.point = false;
        self.line_string = false;
        self.polygon = false;
        self.multi_point = false;
        self.multi_polygon = false;
    }

    pub fn add_multi_polygon(&mut self) {
        self.point = false;
        self.line_string = false;
        self.polygon = false;
        self.multi_point = false;
        self.multi_line_string = false;
    }

    pub fn add_geometry_collection(&mut self) {
        self.point = false;
        self.line_string = false;
        self.polygon = false;
        self.multi_point = false;
        self.multi_line_string = false;
        self.mixed = false;
    }

    pub fn resolve_type(self, large_type: bool, coord_type: CoordType) -> Result<GeoDataType> {
        if self.all_true() {
            return Err(GeoArrowError::General(
                "No geometries have been added.".to_string(),
            ));
        }

        let t = if self.point {
            GeoDataType::Point(coord_type)
        } else if self.line_string {
            if large_type {
                GeoDataType::LargeLineString(coord_type)
            } else {
                GeoDataType::LineString(coord_type)
            }
        } else if self.polygon {
            if large_type {
                GeoDataType::LargePolygon(coord_type)
            } else {
                GeoDataType::Polygon(coord_type)
            }
        } else if self.multi_point {
            if large_type {
                GeoDataType::LargeMultiPoint(coord_type)
            } else {
                GeoDataType::MultiPoint(coord_type)
            }
        } else if self.multi_line_string {
            if large_type {
                GeoDataType::LargeMultiLineString(coord_type)
            } else {
                GeoDataType::MultiLineString(coord_type)
            }
        } else if self.multi_polygon {
            if large_type {
                GeoDataType::LargeMultiPolygon(coord_type)
            } else {
                GeoDataType::MultiPolygon(coord_type)
            }
        } else if self.mixed {
            if large_type {
                GeoDataType::LargeMixed(coord_type)
            } else {
                GeoDataType::Mixed(coord_type)
            }
        } else if large_type {
            GeoDataType::LargeGeometryCollection(coord_type)
        } else {
            GeoDataType::GeometryCollection(coord_type)
        };
        Ok(t)
    }
}

/// Infer the minimal GeoDataType that a sequence of WKB geometries can be casted to.
pub fn infer_geometry_type<'a, O: OffsetSizeTrait>(
    geoms: impl Iterator<Item = WKB<'a, O>>,
    large_type: bool,
    coord_type: CoordType,
) -> Result<GeoDataType> {
    let mut available_type = AvailableTypes::new();
    for geom in geoms {
        match geom.get_wkb_geometry_type() {
            WKBGeometryType::Point => available_type.add_point(),
            WKBGeometryType::LineString => available_type.add_line_string(),
            WKBGeometryType::Polygon => available_type.add_polygon(),
            WKBGeometryType::MultiPoint => available_type.add_multi_point(),
            WKBGeometryType::MultiLineString => available_type.add_multi_line_string(),
            WKBGeometryType::MultiPolygon => available_type.add_multi_polygon(),
            WKBGeometryType::GeometryCollection => available_type.add_geometry_collection(),
        }
    }
    available_type.resolve_type(large_type, coord_type)
}
