use std::ops::Add;

use crate::geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};
use crate::io::wkb::writer::{
    geometry_collection_wkb_size, line_string_wkb_size, multi_line_string_wkb_size,
    multi_point_wkb_size, multi_polygon_wkb_size, polygon_wkb_size, POINT_WKB_SIZE,
};

#[derive(Debug, Clone, Copy)]
pub struct WKBCapacity {
    pub(crate) buffer_capacity: usize,
    pub(crate) offsets_capacity: usize,
}

impl WKBCapacity {
    pub fn new(buffer_capacity: usize, offsets_capacity: usize) -> Self {
        Self {
            buffer_capacity,
            offsets_capacity,
        }
    }

    pub fn new_empty() -> Self {
        Self::new(0, 0)
    }

    pub fn is_empty(&self) -> bool {
        self.buffer_capacity == 0 && self.offsets_capacity == 0
    }

    pub fn buffer_capacity(&self) -> usize {
        self.buffer_capacity
    }

    pub fn offsets_capacity(&self) -> usize {
        self.offsets_capacity
    }

    #[inline]
    pub fn add_point(&mut self, is_valid: bool) {
        if is_valid {
            self.buffer_capacity += POINT_WKB_SIZE;
        }
        self.offsets_capacity += 1;
    }

    #[inline]
    pub fn add_line_string<'a>(&mut self, line_string: Option<&'a (impl LineStringTrait + 'a)>) {
        if let Some(line_string) = line_string {
            self.buffer_capacity += line_string_wkb_size(line_string);
        }
        self.offsets_capacity += 1;
    }

    #[inline]
    pub fn add_polygon<'a>(&mut self, polygon: Option<&'a (impl PolygonTrait + 'a)>) {
        if let Some(polygon) = polygon {
            self.buffer_capacity += polygon_wkb_size(polygon);
        }
        self.offsets_capacity += 1;
    }

    #[inline]
    pub fn add_multi_point<'a>(&mut self, multi_point: Option<&'a (impl MultiPointTrait + 'a)>) {
        if let Some(multi_point) = multi_point {
            self.buffer_capacity += multi_point_wkb_size(multi_point);
        }
        self.offsets_capacity += 1;
    }

    #[inline]
    pub fn add_multi_line_string<'a>(
        &mut self,
        multi_line_string: Option<&'a (impl MultiLineStringTrait + 'a)>,
    ) {
        if let Some(multi_line_string) = multi_line_string {
            self.buffer_capacity += multi_line_string_wkb_size(multi_line_string);
        }
        self.offsets_capacity += 1;
    }

    #[inline]
    pub fn add_multi_polygon<'a>(
        &mut self,
        multi_polygon: Option<&'a (impl MultiPolygonTrait + 'a)>,
    ) {
        if let Some(multi_polygon) = multi_polygon {
            self.buffer_capacity += multi_polygon_wkb_size(multi_polygon);
        }
        self.offsets_capacity += 1;
    }

    #[inline]
    pub fn add_geometry<'a>(&mut self, geom: Option<&'a (impl GeometryTrait + 'a)>) {
        if let Some(geom) = geom {
            match geom.as_type() {
                crate::geo_traits::GeometryType::Point(_) => self.add_point(true),
                crate::geo_traits::GeometryType::LineString(g) => self.add_line_string(Some(g)),
                crate::geo_traits::GeometryType::Polygon(g) => self.add_polygon(Some(g)),
                crate::geo_traits::GeometryType::MultiPoint(p) => self.add_multi_point(Some(p)),
                crate::geo_traits::GeometryType::MultiLineString(p) => {
                    self.add_multi_line_string(Some(p))
                }
                crate::geo_traits::GeometryType::MultiPolygon(p) => self.add_multi_polygon(Some(p)),
                crate::geo_traits::GeometryType::GeometryCollection(p) => {
                    self.add_geometry_collection(Some(p))
                }
                crate::geo_traits::GeometryType::Rect(_) => todo!(),
            }
        } else {
            self.offsets_capacity += 1;
        }
    }

    #[inline]
    pub fn add_geometry_collection<'a>(
        &mut self,
        geometry_collection: Option<&'a (impl GeometryCollectionTrait + 'a)>,
    ) {
        if let Some(geometry_collection) = geometry_collection {
            self.buffer_capacity += geometry_collection_wkb_size(geometry_collection);
        }
        self.offsets_capacity += 1;
    }

    pub fn from_points<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl PointTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_point(maybe_geom.is_some());
        }
        counter
    }

    pub fn from_line_strings<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_line_string(maybe_geom);
        }
        counter
    }

    pub fn from_polygons<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_polygon(maybe_geom);
        }
        counter
    }

    pub fn from_multi_points<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_multi_point(maybe_geom);
        }
        counter
    }

    pub fn from_multi_line_strings<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiLineStringTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_multi_line_string(maybe_geom);
        }
        counter
    }

    pub fn from_multi_polygons<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_multi_polygon(maybe_geom);
        }
        counter
    }

    pub fn from_geometries<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry(maybe_geom);
        }
        counter
    }

    pub fn from_owned_geometries<'a>(
        geoms: impl Iterator<Item = Option<(impl GeometryTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry(maybe_geom.as_ref());
        }
        counter
    }
}

impl Default for WKBCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl Add for WKBCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let buffer_capacity = self.buffer_capacity + rhs.buffer_capacity;
        let offsets_capacity = self.offsets_capacity + rhs.offsets_capacity;

        Self::new(buffer_capacity, offsets_capacity)
    }
}
