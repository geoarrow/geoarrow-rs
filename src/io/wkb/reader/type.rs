// TODO: is this obsolete with downcasting implementation?
#![allow(non_upper_case_globals, dead_code)]

use arrow_array::OffsetSizeTrait;
use num_enum::TryFromPrimitive;

use crate::array::CoordType;
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::io::wkb::common::WKBType;
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

    pub fn resolve_type(self, large_type: bool, coord_type: CoordType) -> Result<NativeType> {
        if self.all_true() {
            return Err(GeoArrowError::General(
                "No geometries have been added.".to_string(),
            ));
        }

        let t = if self.point {
            NativeType::Point(coord_type, Dimension::XY)
        } else if self.line_string {
            if large_type {
                NativeType::LargeLineString(coord_type, Dimension::XY)
            } else {
                NativeType::LineString(coord_type, Dimension::XY)
            }
        } else if self.polygon {
            if large_type {
                NativeType::LargePolygon(coord_type, Dimension::XY)
            } else {
                NativeType::Polygon(coord_type, Dimension::XY)
            }
        } else if self.multi_point {
            if large_type {
                NativeType::LargeMultiPoint(coord_type, Dimension::XY)
            } else {
                NativeType::MultiPoint(coord_type, Dimension::XY)
            }
        } else if self.multi_line_string {
            if large_type {
                NativeType::LargeMultiLineString(coord_type, Dimension::XY)
            } else {
                NativeType::MultiLineString(coord_type, Dimension::XY)
            }
        } else if self.multi_polygon {
            if large_type {
                NativeType::LargeMultiPolygon(coord_type, Dimension::XY)
            } else {
                NativeType::MultiPolygon(coord_type, Dimension::XY)
            }
        } else if self.mixed {
            if large_type {
                NativeType::LargeMixed(coord_type, Dimension::XY)
            } else {
                NativeType::Mixed(coord_type, Dimension::XY)
            }
        } else if large_type {
            NativeType::LargeGeometryCollection(coord_type, Dimension::XY)
        } else {
            NativeType::GeometryCollection(coord_type, Dimension::XY)
        };
        Ok(t)
    }
}

/// Infer the minimal NativeType that a sequence of WKB geometries can be casted to.
pub(crate) fn infer_geometry_type<'a, O: OffsetSizeTrait>(
    geoms: impl Iterator<Item = WKB<'a, O>>,
    large_type: bool,
    coord_type: CoordType,
) -> Result<NativeType> {
    let mut available_type = AvailableTypes::new();
    for geom in geoms {
        match geom.wkb_type()? {
            WKBType::Point => available_type.add_point(),
            WKBType::LineString => available_type.add_line_string(),
            WKBType::Polygon => available_type.add_polygon(),
            WKBType::MultiPoint => available_type.add_multi_point(),
            WKBType::MultiLineString => available_type.add_multi_line_string(),
            WKBType::MultiPolygon => available_type.add_multi_polygon(),
            WKBType::GeometryCollection => available_type.add_geometry_collection(),
            _ => todo!("3d support"),
        }
    }
    available_type.resolve_type(large_type, coord_type)
}
