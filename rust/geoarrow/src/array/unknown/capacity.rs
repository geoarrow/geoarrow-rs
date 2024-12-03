use std::ops::AddAssign;

use crate::array::linestring::LineStringCapacity;
use crate::array::mixed::builder::DEFAULT_PREFER_MULTI;
use crate::array::multilinestring::MultiLineStringCapacity;
use crate::array::multipoint::MultiPointCapacity;
use crate::array::multipolygon::MultiPolygonCapacity;
use crate::array::polygon::PolygonCapacity;
use crate::error::Result;
use geo_traits::*;

/// A counter for the buffer sizes of a [`UnknownGeometryArray`][crate::array::UnknownGeometryArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Default, Debug, Clone, Copy)]
pub struct UnknownCapacity {
    /// The number of null geometries. Ideally the builder will assign these to any array that has
    /// already been allocated. Otherwise we don't know where to assign them.
    nulls: usize,

    /// Simple: just the total number of points, nulls included
    point_xy: usize,
    line_string_xy: LineStringCapacity,
    polygon_xy: PolygonCapacity,
    mpoint_xy: MultiPointCapacity,
    mline_string_xy: MultiLineStringCapacity,
    mpolygon_xy: MultiPolygonCapacity,

    point_xyz: usize,
    line_string_xyz: LineStringCapacity,
    polygon_xyz: PolygonCapacity,
    mpoint_xyz: MultiPointCapacity,
    mline_string_xyz: MultiLineStringCapacity,
    mpolygon_xyz: MultiPolygonCapacity,

    /// Whether to prefer multi or single arrays for new geometries.
    prefer_multi: bool,
}

impl UnknownCapacity {
    /// Create a new empty capacity.
    pub fn new_empty() -> Self {
        Self {
            nulls: 0,
            point_xy: 0,
            line_string_xy: LineStringCapacity::new_empty(),
            polygon_xy: PolygonCapacity::new_empty(),
            mpoint_xy: MultiPointCapacity::new_empty(),
            mline_string_xy: MultiLineStringCapacity::new_empty(),
            mpolygon_xy: MultiPolygonCapacity::new_empty(),
            point_xyz: 0,
            line_string_xyz: LineStringCapacity::new_empty(),
            polygon_xyz: PolygonCapacity::new_empty(),
            mpoint_xyz: MultiPointCapacity::new_empty(),
            mline_string_xyz: MultiLineStringCapacity::new_empty(),
            mpolygon_xyz: MultiPolygonCapacity::new_empty(),
            prefer_multi: DEFAULT_PREFER_MULTI,
        }
    }

    /// Return `true` if the capacity is empty.
    pub fn is_empty(&self) -> bool {
        self.point_xy == 0
            && self.line_string_xy.is_empty()
            && self.polygon_xy.is_empty()
            && self.mpoint_xy.is_empty()
            && self.mline_string_xy.is_empty()
            && self.mpolygon_xy.is_empty()
            && self.point_xyz == 0
            && self.line_string_xyz.is_empty()
            && self.polygon_xyz.is_empty()
            && self.mpoint_xyz.is_empty()
            && self.mline_string_xyz.is_empty()
            && self.mpolygon_xyz.is_empty()
    }

    pub fn total_num_geoms(&self) -> usize {
        let mut total = 0;
        total += self.point_xy;
        total += self.line_string_xy.geom_capacity();
        total += self.polygon_xy.geom_capacity();
        total += self.mpoint_xy.geom_capacity();
        total += self.mline_string_xy.geom_capacity();
        total += self.mpolygon_xy.geom_capacity();
        total += self.point_xyz;
        total += self.line_string_xyz.geom_capacity();
        total += self.polygon_xyz.geom_capacity();
        total += self.mpoint_xyz.geom_capacity();
        total += self.mline_string_xyz.geom_capacity();
        total += self.mpolygon_xyz.geom_capacity();
        total
    }

    pub fn point_xy(&self) -> usize {
        self.point_xy
    }

    pub fn line_string_xy(&self) -> LineStringCapacity {
        self.line_string_xy
    }

    pub fn polygon_xy(&self) -> PolygonCapacity {
        self.polygon_xy
    }

    pub fn mpoint_xy(&self) -> MultiPointCapacity {
        self.mpoint_xy
    }

    pub fn mline_string_xy(&self) -> MultiLineStringCapacity {
        self.mline_string_xy
    }

    pub fn mpolygon_xy(&self) -> MultiPolygonCapacity {
        self.mpolygon_xy
    }

    pub fn point_xyz(&self) -> usize {
        self.point_xyz
    }

    pub fn line_string_xyz(&self) -> LineStringCapacity {
        self.line_string_xyz
    }

    pub fn polygon_xyz(&self) -> PolygonCapacity {
        self.polygon_xyz
    }

    pub fn mpoint_xyz(&self) -> MultiPointCapacity {
        self.mpoint_xyz
    }

    pub fn mline_string_xyz(&self) -> MultiLineStringCapacity {
        self.mline_string_xyz
    }

    pub fn mpolygon_xyz(&self) -> MultiPolygonCapacity {
        self.mpolygon_xyz
    }

    // pub fn point_compatible(&self) -> bool {
    //     self.line_string.is_empty()
    //         && self.polygon.is_empty()
    //         && self.multi_point.is_empty()
    //         && self.multi_line_string.is_empty()
    //         && self.multi_polygon.is_empty()
    // }

    // pub fn line_string_compatible(&self) -> bool {
    //     self.point == 0
    //         && self.polygon.is_empty()
    //         && self.multi_point.is_empty()
    //         && self.multi_line_string.is_empty()
    //         && self.multi_polygon.is_empty()
    // }

    // pub fn polygon_compatible(&self) -> bool {
    //     self.point == 0
    //         && self.line_string.is_empty()
    //         && self.multi_point.is_empty()
    //         && self.multi_line_string.is_empty()
    //         && self.multi_polygon.is_empty()
    // }

    // pub fn multi_point_compatible(&self) -> bool {
    //     self.line_string.is_empty()
    //         && self.polygon.is_empty()
    //         && self.multi_line_string.is_empty()
    //         && self.multi_polygon.is_empty()
    // }

    // pub fn multi_line_string_compatible(&self) -> bool {
    //     self.point == 0
    //         && self.polygon.is_empty()
    //         && self.multi_point.is_empty()
    //         && self.multi_polygon.is_empty()
    // }

    // pub fn multi_polygon_compatible(&self) -> bool {
    //     self.point == 0
    //         && self.line_string.is_empty()
    //         && self.multi_point.is_empty()
    //         && self.multi_line_string.is_empty()
    // }

    #[inline]
    pub fn add_point(&mut self, point: Option<&impl PointTrait>) {
        if let Some(point) = point {
            match point.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    if self.prefer_multi {
                        self.mpoint_xy.add_point_capacity(1);
                    } else {
                        self.point_xy += 1;
                    }
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    if self.prefer_multi {
                        self.mpoint_xyz.add_point_capacity(1);
                    } else {
                        self.point_xyz += 1;
                    }
                }
                _ => todo!(),
            }
        } else {
            self.nulls += 1;
        }
    }

    #[inline]
    pub fn add_line_string(&mut self, line_string: Option<&impl LineStringTrait>) {
        if let Some(line_string) = line_string {
            match line_string.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    if self.prefer_multi {
                        self.mline_string_xy.add_line_string(Some(line_string));
                    } else {
                        self.line_string_xy.add_line_string(Some(line_string));
                    }
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    if self.prefer_multi {
                        self.mline_string_xyz.add_line_string(Some(line_string));
                    } else {
                        self.line_string_xyz.add_line_string(Some(line_string));
                    }
                }
                _ => todo!(),
            }
        } else {
            self.nulls += 1;
        }
    }

    #[inline]
    pub fn add_polygon(&mut self, polygon: Option<&impl PolygonTrait>) {
        if let Some(polygon) = polygon {
            match polygon.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    if self.prefer_multi {
                        self.mpolygon_xy.add_polygon(Some(polygon));
                    } else {
                        self.polygon_xy.add_polygon(Some(polygon));
                    }
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    if self.prefer_multi {
                        self.mpolygon_xyz.add_polygon(Some(polygon));
                    } else {
                        self.polygon_xyz.add_polygon(Some(polygon));
                    }
                }
                _ => todo!(),
            }
        } else {
            self.nulls += 1;
        }
    }

    #[inline]
    pub fn add_multi_point(&mut self, multi_point: Option<&impl MultiPointTrait>) {
        if let Some(multi_point) = multi_point {
            match multi_point.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    self.mpoint_xy.add_multi_point(Some(multi_point));
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    self.mpoint_xyz.add_multi_point(Some(multi_point));
                }
                _ => todo!(),
            }
        } else {
            self.nulls += 1;
        }
    }

    #[inline]
    pub fn add_multi_line_string(&mut self, multi_line_string: Option<&impl MultiLineStringTrait>) {
        if let Some(multi_line_string) = multi_line_string {
            match multi_line_string.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    self.mline_string_xy
                        .add_multi_line_string(Some(multi_line_string));
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    self.mline_string_xyz
                        .add_multi_line_string(Some(multi_line_string));
                }
                _ => todo!(),
            }
        } else {
            self.nulls += 1;
        }
    }

    #[inline]
    pub fn add_multi_polygon(&mut self, multi_polygon: Option<&impl MultiPolygonTrait>) {
        if let Some(multi_polygon) = multi_polygon {
            match multi_polygon.dim() {
                Dimensions::Xy | Dimensions::Unknown(2) => {
                    self.mpolygon_xy.add_multi_polygon(Some(multi_polygon));
                }
                Dimensions::Xyz | Dimensions::Unknown(3) => {
                    self.mpolygon_xyz.add_multi_polygon(Some(multi_polygon));
                }
                _ => todo!(),
            }
        } else {
            self.nulls += 1;
        }
    }

    #[inline]
    pub fn add_geometry(&mut self, geom: Option<&impl GeometryTrait>) -> Result<()> {
        if let Some(geom) = geom {
            match geom.as_type() {
                geo_traits::GeometryType::Point(g) => self.add_point(Some(g)),
                geo_traits::GeometryType::LineString(g) => self.add_line_string(Some(g)),
                geo_traits::GeometryType::Polygon(g) => self.add_polygon(Some(g)),
                geo_traits::GeometryType::MultiPoint(p) => self.add_multi_point(Some(p)),
                geo_traits::GeometryType::MultiLineString(p) => self.add_multi_line_string(Some(p)),
                geo_traits::GeometryType::MultiPolygon(p) => self.add_multi_polygon(Some(p)),
                geo_traits::GeometryType::GeometryCollection(_) => {
                    panic!("nested geometry collections not supported")
                }
                _ => todo!(),
            };
        } else {
            self.nulls += 1;
        }
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
    pub fn num_bytes(&self) -> usize {
        let mut count = 0;

        count += self.point_xy * 2 * 8;
        count += self.line_string_xy.num_bytes();
        count += self.polygon_xy.num_bytes();
        count += self.mpoint_xy.num_bytes();
        count += self.mline_string_xy.num_bytes();
        count += self.mpolygon_xy.num_bytes();

        count += self.point_xyz * 3 * 8;
        count += self.line_string_xyz.num_bytes();
        count += self.polygon_xyz.num_bytes();
        count += self.mpoint_xyz.num_bytes();
        count += self.mline_string_xyz.num_bytes();
        count += self.mpolygon_xyz.num_bytes();

        count
    }
}

impl AddAssign for UnknownCapacity {
    fn add_assign(&mut self, rhs: Self) {
        self.nulls += rhs.nulls;

        // TODO: implement AddAssign on all of these and switch to using add assign
        self.point_xy = self.point_xy + rhs.point_xy;
        self.line_string_xy = self.line_string_xy + rhs.line_string_xy;
        self.polygon_xy = self.polygon_xy + rhs.polygon_xy;
        self.mpoint_xy = self.mpoint_xy + rhs.mpoint_xy;
        self.mline_string_xy = self.mline_string_xy + rhs.mline_string_xy;
        self.mpolygon_xy = self.mpolygon_xy + rhs.mpolygon_xy;

        self.point_xyz = self.point_xyz + rhs.point_xyz;
        self.line_string_xyz = self.line_string_xyz + rhs.line_string_xyz;
        self.polygon_xyz = self.polygon_xyz + rhs.polygon_xyz;
        self.mpoint_xyz = self.mpoint_xyz + rhs.mpoint_xyz;
        self.mline_string_xyz = self.mline_string_xyz + rhs.mline_string_xyz;
        self.mpolygon_xyz = self.mpolygon_xyz + rhs.mpolygon_xyz;
    }
}
