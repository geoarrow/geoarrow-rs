use crate::array::linestring::LineStringCapacity;
use crate::array::multilinestring::MultiLineStringCapacity;
use crate::array::multipoint::MultiPointCapacity;
use crate::array::multipolygon::MultiPolygonCapacity;
use crate::array::polygon::PolygonCapacity;
use crate::geo_traits::{
    LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PolygonTrait,
};

struct WKBGeometriesStatistics {
    /// Simple: just the total number of points, nulls include
    point: usize,
    line_string: LineStringCapacity,
    polygon: PolygonCapacity,
    multi_point: MultiPointCapacity,
    multi_line_string: MultiLineStringCapacity,
    multi_polygon: MultiPolygonCapacity,
}

impl WKBGeometriesStatistics {
    pub fn new() -> WKBGeometriesStatistics {
        Self {
            point: 0,
            line_string: LineStringCapacity::new_empty(),
            polygon: PolygonCapacity::new_empty(),
            multi_point: MultiPointCapacity::new_empty(),
            multi_line_string: MultiLineStringCapacity::new_empty(),
            multi_polygon: MultiPolygonCapacity::new_empty(),
        }
    }

    // /// Check if all types are true. Implies that no geometries have been added
    // fn all_true(&self) -> bool {
    //     self.point
    //         && self.line_string
    //         && self.polygon
    //         && self.multi_point
    //         && self.multi_line_string
    //         && self.multi_polygon
    //         && self.mixed
    // }

    pub fn add_point(&mut self) {
        self.point += 1;
    }

    pub fn add_line_string<'a>(&mut self, line_string: Option<&'a (impl LineStringTrait + 'a)>) {
        self.line_string.add_line_string(line_string);
    }

    pub fn add_polygon<'a>(&mut self, polygon: Option<&'a (impl PolygonTrait + 'a)>) {
        self.polygon.add_polygon(polygon);
    }

    pub fn add_multi_point<'a>(&mut self, multi_point: Option<&'a (impl MultiPointTrait + 'a)>) {
        self.multi_point.add_multi_point(multi_point);
    }

    pub fn add_multi_line_string<'a>(
        &mut self,
        multi_line_string: Option<&'a (impl MultiLineStringTrait + 'a)>,
    ) {
        self.multi_line_string
            .add_multi_line_string(multi_line_string);
    }

    pub fn add_multi_polygon<'a>(
        &mut self,
        multi_polygon: Option<&'a (impl MultiPolygonTrait + 'a)>,
    ) {
        self.multi_polygon.add_multi_polygon(multi_polygon);
    }
}
