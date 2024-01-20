use std::ops::Add;

use arrow_array::OffsetSizeTrait;

use crate::array::linestring::LineStringCapacity;
use crate::array::multilinestring::MultiLineStringCapacity;
use crate::array::multipoint::MultiPointCapacity;
use crate::array::multipolygon::MultiPolygonCapacity;
use crate::array::polygon::PolygonCapacity;
use crate::error::Result;
use crate::geo_traits::*;

/// A counter for the buffer sizes of a [`MixedGeometryArray`][crate::array::MixedGeometryArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Default, Debug, Clone, Copy)]
pub struct MixedCapacity {
    /// Simple: just the total number of points, nulls included
    pub(crate) point: usize,
    pub(crate) line_string: LineStringCapacity,
    pub(crate) polygon: PolygonCapacity,
    pub(crate) multi_point: MultiPointCapacity,
    pub(crate) multi_line_string: MultiLineStringCapacity,
    pub(crate) multi_polygon: MultiPolygonCapacity,
}

impl MixedCapacity {
    /// Create a new capacity with known sizes.
    pub fn new(
        point: usize,
        line_string: LineStringCapacity,
        polygon: PolygonCapacity,
        multi_point: MultiPointCapacity,
        multi_line_string: MultiLineStringCapacity,
        multi_polygon: MultiPolygonCapacity,
    ) -> Self {
        Self {
            point,
            line_string,
            polygon,
            multi_point,
            multi_line_string,
            multi_polygon,
        }
    }

    /// Create a new empty capacity.
    pub fn new_empty() -> Self {
        Self {
            point: 0,
            line_string: LineStringCapacity::new_empty(),
            polygon: PolygonCapacity::new_empty(),
            multi_point: MultiPointCapacity::new_empty(),
            multi_line_string: MultiLineStringCapacity::new_empty(),
            multi_polygon: MultiPolygonCapacity::new_empty(),
        }
    }

    /// Return `true` if the capacity is empty.
    pub fn is_empty(&self) -> bool {
        self.point == 0
            && self.line_string.is_empty()
            && self.polygon.is_empty()
            && self.multi_point.is_empty()
            && self.multi_line_string.is_empty()
            && self.multi_polygon.is_empty()
    }

    pub fn total_num_geoms(&self) -> usize {
        let mut total = 0;
        total += self.point;
        total += self.line_string.geom_capacity();
        total += self.polygon.geom_capacity();
        total += self.multi_point.geom_capacity();
        total += self.multi_line_string.geom_capacity();
        total += self.multi_polygon.geom_capacity();
        total
    }

    pub fn point_capacity(&self) -> usize {
        self.point
    }

    pub fn line_string_capacity(&self) -> LineStringCapacity {
        self.line_string
    }

    pub fn polygon_capacity(&self) -> PolygonCapacity {
        self.polygon
    }

    pub fn multi_point_capacity(&self) -> MultiPointCapacity {
        self.multi_point
    }

    pub fn multi_line_string_capacity(&self) -> MultiLineStringCapacity {
        self.multi_line_string
    }

    pub fn multi_polygon_capacity(&self) -> MultiPolygonCapacity {
        self.multi_polygon
    }

    pub fn point_compatible(&self) -> bool {
        self.line_string.is_empty()
            && self.polygon.is_empty()
            && self.multi_point.is_empty()
            && self.multi_line_string.is_empty()
            && self.multi_polygon.is_empty()
    }

    pub fn line_string_compatible(&self) -> bool {
        self.point == 0
            && self.polygon.is_empty()
            && self.multi_point.is_empty()
            && self.multi_line_string.is_empty()
            && self.multi_polygon.is_empty()
    }

    pub fn polygon_compatible(&self) -> bool {
        self.point == 0
            && self.line_string.is_empty()
            && self.multi_point.is_empty()
            && self.multi_line_string.is_empty()
            && self.multi_polygon.is_empty()
    }

    pub fn multi_point_compatible(&self) -> bool {
        self.line_string.is_empty()
            && self.polygon.is_empty()
            && self.multi_line_string.is_empty()
            && self.multi_polygon.is_empty()
    }

    pub fn multi_line_string_compatible(&self) -> bool {
        self.point == 0
            && self.polygon.is_empty()
            && self.multi_point.is_empty()
            && self.multi_polygon.is_empty()
    }

    pub fn multi_polygon_compatible(&self) -> bool {
        self.point == 0
            && self.line_string.is_empty()
            && self.multi_point.is_empty()
            && self.multi_line_string.is_empty()
    }

    #[inline]
    pub fn add_point(&mut self) {
        self.point += 1;
    }

    #[inline]
    pub fn add_line_string(&mut self, line_string: Option<&impl LineStringTrait>) {
        self.line_string.add_line_string(line_string);
    }

    #[inline]
    pub fn add_polygon(&mut self, polygon: Option<&impl PolygonTrait>) {
        self.polygon.add_polygon(polygon);
    }

    #[inline]
    pub fn add_multi_point(&mut self, multi_point: Option<&impl MultiPointTrait>) {
        self.multi_point.add_multi_point(multi_point);
    }

    #[inline]
    pub fn add_multi_line_string(&mut self, multi_line_string: Option<&impl MultiLineStringTrait>) {
        self.multi_line_string
            .add_multi_line_string(multi_line_string);
    }

    #[inline]
    pub fn add_multi_polygon(&mut self, multi_polygon: Option<&impl MultiPolygonTrait>) {
        self.multi_polygon.add_multi_polygon(multi_polygon);
    }

    #[inline]
    pub fn add_geometry(&mut self, geom: Option<&impl GeometryTrait>) -> Result<()> {
        // TODO: what to do about null geometries? We don't know which type they have
        assert!(geom.is_some());
        if let Some(geom) = geom {
            match geom.as_type() {
                crate::geo_traits::GeometryType::Point(_) => self.add_point(),
                crate::geo_traits::GeometryType::LineString(g) => self.add_line_string(Some(g)),
                crate::geo_traits::GeometryType::Polygon(g) => self.add_polygon(Some(g)),
                crate::geo_traits::GeometryType::MultiPoint(p) => self.add_multi_point(Some(p)),
                crate::geo_traits::GeometryType::MultiLineString(p) => {
                    self.add_multi_line_string(Some(p))
                }
                crate::geo_traits::GeometryType::MultiPolygon(p) => self.add_multi_polygon(Some(p)),
                crate::geo_traits::GeometryType::GeometryCollection(_) => {
                    panic!("nested geometry collections not supported")
                }
                crate::geo_traits::GeometryType::Rect(_) => todo!(),
            };
        };
        Ok(())
    }

    pub fn from_geometries<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
    ) -> Result<Self> {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry(maybe_geom)?;
        }
        Ok(counter)
    }

    pub fn from_owned_geometries<'a>(
        geoms: impl Iterator<Item = Option<(impl GeometryTrait + 'a)>>,
    ) -> Result<Self> {
        let mut counter = Self::new_empty();
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry(maybe_geom.as_ref())?;
        }
        Ok(counter)
    }

    /// The number of bytes an array with this capacity would occupy.
    pub fn num_bytes<O: OffsetSizeTrait>(&self) -> usize {
        let mut count = self.point * 2 * 8;
        count += self.line_string.num_bytes::<O>();
        count += self.polygon.num_bytes::<O>();
        count += self.multi_point.num_bytes::<O>();
        count += self.multi_line_string.num_bytes::<O>();
        count += self.multi_polygon.num_bytes::<O>();
        count
    }
}

impl Add for MixedCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let point = self.point + rhs.point;
        let line_string = self.line_string + rhs.line_string;
        let polygon = self.polygon + rhs.polygon;
        let multi_point = self.multi_point + rhs.multi_point;
        let multi_line_string = self.multi_line_string + rhs.multi_line_string;
        let multi_polygon = self.multi_polygon + rhs.multi_polygon;

        Self::new(
            point,
            line_string,
            polygon,
            multi_point,
            multi_line_string,
            multi_polygon,
        )
    }
}
