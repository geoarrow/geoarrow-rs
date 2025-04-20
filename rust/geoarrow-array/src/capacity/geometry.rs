use std::ops::AddAssign;

use geo_traits::*;

use crate::array::DimensionIndex;
use crate::capacity::{
    GeometryCollectionCapacity, LineStringCapacity, MultiLineStringCapacity, MultiPointCapacity,
    MultiPolygonCapacity, PolygonCapacity,
};
use crate::error::Result;

/// A counter for the buffer sizes of a [`GeometryArray`][crate::array::GeometryArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Default, Debug, Clone, Copy)]
pub struct GeometryCapacity {
    /// The number of null geometries. Ideally the builder will assign these to any array that has
    /// already been allocated. Otherwise we don't know where to assign them.
    nulls: usize,

    /// Simple: just the total number of points, nulls included
    points: [usize; 4],
    /// An array of [LineStringCapacity], ordered XY, XYZ, XYM, XYZM
    line_strings: [LineStringCapacity; 4],
    polygons: [PolygonCapacity; 4],
    mpoints: [MultiPointCapacity; 4],
    mline_strings: [MultiLineStringCapacity; 4],
    mpolygons: [MultiPolygonCapacity; 4],
    gcs: [GeometryCollectionCapacity; 4],

    /// Whether to prefer multi or single arrays for new geometries.
    prefer_multi: bool,
}

impl GeometryCapacity {
    /// Create a new capacity with known sizes.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        nulls: usize,
        points: [usize; 4],
        line_strings: [LineStringCapacity; 4],
        polygons: [PolygonCapacity; 4],
        mpoints: [MultiPointCapacity; 4],
        mline_strings: [MultiLineStringCapacity; 4],
        mpolygons: [MultiPolygonCapacity; 4],
        gcs: [GeometryCollectionCapacity; 4],
        prefer_multi: bool,
    ) -> Self {
        Self {
            nulls,
            points,
            line_strings,
            polygons,
            mpoints,
            mline_strings,
            mpolygons,
            gcs,
            prefer_multi,
        }
    }

    /// Create a new empty capacity.
    pub fn new_empty() -> Self {
        Default::default()
    }

    /// Set whether this capacity counter should prefer allocating "single-type" geometries like
    /// Point/LineString/Polygon in the arrays of their "Multi" counterparts.
    pub fn with_prefer_multi(mut self, prefer_multi: bool) -> Self {
        self.prefer_multi = prefer_multi;
        self
    }

    /// Return `true` if the capacity is empty.
    pub fn is_empty(&self) -> bool {
        if self.points.iter().any(|c| *c > 0) {
            return false;
        }

        if self.line_strings.iter().any(|c| !c.is_empty()) {
            return false;
        }

        if self.polygons.iter().any(|c| !c.is_empty()) {
            return false;
        }

        if self.mpoints.iter().any(|c| !c.is_empty()) {
            return false;
        }

        if self.mline_strings.iter().any(|c| !c.is_empty()) {
            return false;
        }

        if self.mpolygons.iter().any(|c| !c.is_empty()) {
            return false;
        }

        if self.gcs.iter().any(|c| !c.is_empty()) {
            return false;
        }

        true
    }

    /// The total number of geometries across all geometry types.
    pub fn total_num_geoms(&self) -> usize {
        let mut total = 0;

        self.points.iter().for_each(|c| {
            total += c;
        });
        self.line_strings.iter().for_each(|c| {
            total += c.geom_capacity();
        });
        self.polygons.iter().for_each(|c| {
            total += c.geom_capacity();
        });
        self.mpoints.iter().for_each(|c| {
            total += c.geom_capacity();
        });
        self.mline_strings.iter().for_each(|c| {
            total += c.geom_capacity();
        });
        self.mpolygons.iter().for_each(|c| {
            total += c.geom_capacity();
        });
        self.gcs.iter().for_each(|c| {
            total += c.geom_capacity();
        });

        total
    }

    /// Access point capacities
    ///
    /// Values are represent dimensions in the order: XY, XYZ, XYM, XYZM.
    pub fn points(&self) -> [usize; 4] {
        self.points
    }

    /// Access LineString capacities
    ///
    /// Values are represent dimensions in the order: XY, XYZ, XYM, XYZM.
    pub fn line_strings(&self) -> [LineStringCapacity; 4] {
        self.line_strings
    }

    /// Access Polygon capacities
    ///
    /// Values are represent dimensions in the order: XY, XYZ, XYM, XYZM.
    pub fn polygons(&self) -> [PolygonCapacity; 4] {
        self.polygons
    }

    /// Access MultiPoint capacities
    ///
    /// Values are represent dimensions in the order: XY, XYZ, XYM, XYZM.
    pub fn multi_points(&self) -> [MultiPointCapacity; 4] {
        self.mpoints
    }

    /// Access point capacities
    ///
    /// Values are represent dimensions in the order: XY, XYZ, XYM, XYZM.
    pub fn multi_line_strings(&self) -> [MultiLineStringCapacity; 4] {
        self.mline_strings
    }

    /// Access point capacities
    ///
    /// Values are represent dimensions in the order: XY, XYZ, XYM, XYZM.
    pub fn multi_polygons(&self) -> [MultiPolygonCapacity; 4] {
        self.mpolygons
    }

    /// Access GeometryCollection capacities
    ///
    /// Values are represent dimensions in the order: XY, XYZ, XYM, XYZM.
    pub fn geometry_collections(&self) -> [GeometryCollectionCapacity; 4] {
        self.gcs
    }

    /// Add the capacity of the given Point
    #[inline]
    pub fn add_point(&mut self, point: Option<&impl PointTrait>) {
        if let Some(point) = point {
            if self.prefer_multi {
                self.mpoints[point.dim().order()].add_point(Some(point));
            } else {
                self.points[point.dim().order()] += 1;
            }
        } else {
            self.nulls += 1;
        }
    }

    /// Add the capacity of the given LineString
    #[inline]
    pub fn add_line_string(&mut self, line_string: Option<&impl LineStringTrait>) {
        if let Some(line_string) = line_string {
            if self.prefer_multi {
                self.mline_strings[line_string.dim().order()].add_line_string(Some(line_string));
            } else {
                self.line_strings[line_string.dim().order()].add_line_string(Some(line_string));
            }
        } else {
            self.nulls += 1;
        }
    }

    /// Add the capacity of the given Polygon
    #[inline]
    pub fn add_polygon(&mut self, polygon: Option<&impl PolygonTrait>) {
        if let Some(polygon) = polygon {
            if self.prefer_multi {
                self.mpolygons[polygon.dim().order()].add_polygon(Some(polygon));
            } else {
                self.polygons[polygon.dim().order()].add_polygon(Some(polygon));
            }
        } else {
            self.nulls += 1;
        }
    }

    /// Add the capacity of the given MultiPoint
    #[inline]
    pub fn add_multi_point(&mut self, multi_point: Option<&impl MultiPointTrait>) {
        if let Some(multi_point) = multi_point {
            self.multi_points()[multi_point.dim().order()].add_multi_point(Some(multi_point));
        } else {
            self.nulls += 1;
        }
    }

    /// Add the capacity of the given MultiLineString
    #[inline]
    pub fn add_multi_line_string(&mut self, multi_line_string: Option<&impl MultiLineStringTrait>) {
        if let Some(multi_line_string) = multi_line_string {
            self.multi_line_strings()[multi_line_string.dim().order()]
                .add_multi_line_string(Some(multi_line_string));
        } else {
            self.nulls += 1;
        }
    }

    /// Add the capacity of the given MultiPolygon
    #[inline]
    pub fn add_multi_polygon(&mut self, multi_polygon: Option<&impl MultiPolygonTrait>) {
        if let Some(multi_polygon) = multi_polygon {
            self.multi_polygons()[multi_polygon.dim().order()]
                .add_multi_polygon(Some(multi_polygon));
        } else {
            self.nulls += 1;
        }
    }

    /// Add the capacity of the given Geometry
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
                geo_traits::GeometryType::GeometryCollection(p) => {
                    self.add_geometry_collection(Some(p))?
                }
                _ => todo!(),
            };
        } else {
            self.nulls += 1;
        }
        Ok(())
    }

    /// Add the capacity of the given GeometryCollection
    #[inline]
    pub fn add_geometry_collection(
        &mut self,
        gc: Option<&impl GeometryCollectionTrait>,
    ) -> Result<()> {
        if let Some(gc) = gc {
            self.gcs[gc.dim().order()].add_geometry_collection(Some(gc))?;
        } else {
            self.nulls += 1;
        };
        Ok(())
    }

    /// Construct a new counter pre-filled with the given geometries
    pub fn from_geometries<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl GeometryTrait + 'a)>>,
        prefer_multi: bool,
    ) -> Result<Self> {
        let mut counter = Self::new_empty().with_prefer_multi(prefer_multi);
        for maybe_geom in geoms.into_iter() {
            counter.add_geometry(maybe_geom)?;
        }
        Ok(counter)
    }

    /// The number of bytes an array with this capacity would occupy.
    pub fn num_bytes(&self) -> usize {
        let mut count = 0;

        self.points.iter().for_each(|c| count += c * 2 * 8);
        self.line_strings
            .iter()
            .for_each(|c| count += c.num_bytes());
        self.polygons.iter().for_each(|c| count += c.num_bytes());
        self.mpoints.iter().for_each(|c| count += c.num_bytes());
        self.mline_strings
            .iter()
            .for_each(|c| count += c.num_bytes());
        self.mpolygons.iter().for_each(|c| count += c.num_bytes());
        self.gcs.iter().for_each(|c| count += c.num_bytes());

        count
    }
}

impl AddAssign for GeometryCapacity {
    fn add_assign(&mut self, rhs: Self) {
        self.nulls += rhs.nulls;

        self.points = core::array::from_fn(|i| self.points[i] + rhs.points[i]);
        self.line_strings = core::array::from_fn(|i| self.line_strings[i] + rhs.line_strings[i]);
        self.polygons = core::array::from_fn(|i| self.polygons[i] + rhs.polygons[i]);
        self.mpoints = core::array::from_fn(|i| self.mpoints[i] + rhs.mpoints[i]);
        self.mline_strings = core::array::from_fn(|i| self.mline_strings[i] + rhs.mline_strings[i]);
        self.mpolygons = core::array::from_fn(|i| self.mpolygons[i] + rhs.mpolygons[i]);
        self.gcs = core::array::from_fn(|i| self.gcs[i] + rhs.gcs[i]);
    }
}
