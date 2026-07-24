//! `geo` calculates in two dimensions only. Simplify and convex hull select coordinates
//! from their input. They calculate on an XY projection, then read the selected
//! coordinates from these types, and the Z and M values stay correct.

use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    LineTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
    RectTrait, TriangleTrait, UnimplementedGeometryCollection, UnimplementedLine,
    UnimplementedLineString, UnimplementedMultiLineString, UnimplementedMultiPoint,
    UnimplementedMultiPolygon, UnimplementedPoint, UnimplementedPolygon, UnimplementedRect,
    UnimplementedTriangle,
};

/// X, Y, Z, M. Values that the dimension does not include are zero.
pub(crate) type Ordinates = [f64; 4];

pub(crate) fn ordinates<C: CoordTrait<T = f64>>(c: &C) -> Ordinates {
    let mut vals = [0.0; 4];
    for (n, slot) in vals.iter_mut().enumerate().take(c.dim().size()) {
        *slot = c.nth_or_panic(n);
    }
    vals
}

#[derive(Clone, Copy)]
pub(crate) struct DimCoord {
    pub(crate) dim: Dimensions,
    pub(crate) vals: Ordinates,
}

impl CoordTrait for DimCoord {
    type T = f64;
    fn dim(&self) -> Dimensions {
        self.dim
    }
    fn x(&self) -> f64 {
        self.vals[0]
    }
    fn y(&self) -> f64 {
        self.vals[1]
    }
    fn nth_or_panic(&self, n: usize) -> f64 {
        assert!(
            n < self.dim.size(),
            "coordinate dimension {:?} has no ordinate {n}",
            self.dim
        );
        self.vals[n]
    }
}

/// `GeometryTrait` is a supertrait of all the other geometry traits. Thus each type
/// below must declare all ten associated types, but it uses only one.
macro_rules! impl_geometry {
    ($ty:ty, $variant:ident, $ls:ty, $poly:ty, $mls:ty, $mpoly:ty) => {
        impl GeometryTrait for $ty {
            type T = f64;
            type PointType<'b>
                = UnimplementedPoint<f64>
            where
                Self: 'b;
            type LineStringType<'b>
                = $ls
            where
                Self: 'b;
            type PolygonType<'b>
                = $poly
            where
                Self: 'b;
            type MultiPointType<'b>
                = UnimplementedMultiPoint<f64>
            where
                Self: 'b;
            type MultiLineStringType<'b>
                = $mls
            where
                Self: 'b;
            type MultiPolygonType<'b>
                = $mpoly
            where
                Self: 'b;
            type GeometryCollectionType<'b>
                = UnimplementedGeometryCollection<f64>
            where
                Self: 'b;
            type RectType<'b>
                = UnimplementedRect<f64>
            where
                Self: 'b;
            type TriangleType<'b>
                = UnimplementedTriangle<f64>
            where
                Self: 'b;
            type LineType<'b>
                = UnimplementedLine<f64>
            where
                Self: 'b;

            fn dim(&self) -> Dimensions {
                self.dim
            }

            fn as_type(
                &self,
            ) -> GeometryType<
                '_,
                Self::PointType<'_>,
                Self::LineStringType<'_>,
                Self::PolygonType<'_>,
                Self::MultiPointType<'_>,
                Self::MultiLineStringType<'_>,
                Self::MultiPolygonType<'_>,
                Self::GeometryCollectionType<'_>,
                Self::RectType<'_>,
                Self::TriangleType<'_>,
                Self::LineType<'_>,
            > {
                GeometryType::$variant(self)
            }
        }
    };
    ($ty:ty, LineString) => {
        impl_geometry!(
            $ty,
            LineString,
            Self,
            UnimplementedPolygon<f64>,
            UnimplementedMultiLineString<f64>,
            UnimplementedMultiPolygon<f64>
        );
    };
    ($ty:ty, Polygon) => {
        impl_geometry!(
            $ty,
            Polygon,
            UnimplementedLineString<f64>,
            Self,
            UnimplementedMultiLineString<f64>,
            UnimplementedMultiPolygon<f64>
        );
    };
    ($ty:ty, MultiLineString) => {
        impl_geometry!(
            $ty,
            MultiLineString,
            UnimplementedLineString<f64>,
            UnimplementedPolygon<f64>,
            Self,
            UnimplementedMultiPolygon<f64>
        );
    };
    ($ty:ty, MultiPolygon) => {
        impl_geometry!(
            $ty,
            MultiPolygon,
            UnimplementedLineString<f64>,
            UnimplementedPolygon<f64>,
            UnimplementedMultiLineString<f64>,
            Self
        );
    };
}

#[derive(Clone, Copy)]
pub(crate) struct DimRing<'a> {
    pub(crate) dim: Dimensions,
    pub(crate) coords: &'a [Ordinates],
}

impl LineStringTrait for DimRing<'_> {
    type CoordType<'b>
        = DimCoord
    where
        Self: 'b;

    fn num_coords(&self) -> usize {
        self.coords.len()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        DimCoord {
            dim: self.dim,
            vals: self.coords[i],
        }
    }
}

impl_geometry!(DimRing<'_>, LineString);

#[derive(Default)]
pub(crate) struct DimPolygonParts {
    pub(crate) exterior: Vec<Ordinates>,
    pub(crate) interiors: Vec<Vec<Ordinates>>,
}

#[derive(Clone, Copy)]
pub(crate) struct DimPolygon<'a> {
    pub(crate) dim: Dimensions,
    pub(crate) parts: &'a DimPolygonParts,
}

impl PolygonTrait for DimPolygon<'_> {
    type RingType<'b>
        = DimRing<'b>
    where
        Self: 'b;

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        if self.parts.exterior.is_empty() {
            return None;
        }
        Some(DimRing {
            dim: self.dim,
            coords: &self.parts.exterior,
        })
    }

    fn num_interiors(&self) -> usize {
        self.parts.interiors.len()
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        DimRing {
            dim: self.dim,
            coords: &self.parts.interiors[i],
        }
    }
}

impl_geometry!(DimPolygon<'_>, Polygon);

pub(crate) struct DimMultiLineString {
    pub(crate) dim: Dimensions,
    pub(crate) line_strings: Vec<Vec<Ordinates>>,
}

impl MultiLineStringTrait for DimMultiLineString {
    type InnerLineStringType<'b>
        = DimRing<'b>
    where
        Self: 'b;

    fn num_line_strings(&self) -> usize {
        self.line_strings.len()
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::InnerLineStringType<'_> {
        DimRing {
            dim: self.dim,
            coords: &self.line_strings[i],
        }
    }
}

impl_geometry!(DimMultiLineString, MultiLineString);

pub(crate) struct DimMultiPolygon {
    pub(crate) dim: Dimensions,
    pub(crate) polygons: Vec<DimPolygonParts>,
}

impl MultiPolygonTrait for DimMultiPolygon {
    type InnerPolygonType<'b>
        = DimPolygon<'b>
    where
        Self: 'b;

    fn num_polygons(&self) -> usize {
        self.polygons.len()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::InnerPolygonType<'_> {
        DimPolygon {
            dim: self.dim,
            parts: &self.polygons[i],
        }
    }
}

impl_geometry!(DimMultiPolygon, MultiPolygon);

/// Collects the same coordinates as `geo`'s `CoordsIter::exterior_coords_iter`, in the
/// same sequence. The indices from `quick_hull_indices` refer to this sequence.
pub(crate) fn exterior_coords<G: GeometryTrait<T = f64>>(geom: &G) -> Vec<Ordinates> {
    let mut out = Vec::new();
    push_coords(geom, false, &mut out);
    out
}

#[cfg(test)]
pub(crate) fn all_coords<G: GeometryTrait<T = f64>>(geom: &G) -> Vec<Ordinates> {
    let mut out = Vec::new();
    push_coords(geom, true, &mut out);
    out
}

fn push_ring<L: LineStringTrait<T = f64>>(ring: &L, out: &mut Vec<Ordinates>) {
    out.extend(ring.coords().map(|c| ordinates(&c)));
}

fn push_polygon<P: PolygonTrait<T = f64>>(poly: &P, interiors: bool, out: &mut Vec<Ordinates>) {
    if let Some(ext) = poly.exterior() {
        push_ring(&ext, out);
    }
    if interiors {
        for ring in poly.interiors() {
            push_ring(&ring, out);
        }
    }
}

fn push_coords<G: GeometryTrait<T = f64>>(geom: &G, interiors: bool, out: &mut Vec<Ordinates>) {
    match geom.as_type() {
        GeometryType::Point(p) => out.extend(p.coord().map(|c| ordinates(&c))),
        GeometryType::LineString(ls) => push_ring(ls, out),
        GeometryType::Polygon(p) => push_polygon(p, interiors, out),
        GeometryType::MultiPoint(mp) => {
            out.extend(mp.points().filter_map(|p| p.coord().map(|c| ordinates(&c))));
        }
        GeometryType::MultiLineString(mls) => {
            for ls in mls.line_strings() {
                push_ring(&ls, out);
            }
        }
        GeometryType::MultiPolygon(mp) => {
            for poly in mp.polygons() {
                push_polygon(&poly, interiors, out);
            }
        }
        GeometryType::GeometryCollection(gc) => {
            for sub in gc.geometries() {
                push_coords(&sub, interiors, out);
            }
        }
        GeometryType::Rect(rect) => {
            let min = ordinates(&rect.min());
            let max = ordinates(&rect.max());
            // The two other corners have no Z or M values. They use the values from
            // the minimum corner.
            out.extend([
                min,
                [max[0], min[1], min[2], min[3]],
                max,
                [min[0], max[1], min[2], min[3]],
            ]);
        }
        GeometryType::Triangle(tri) => {
            out.extend(
                [tri.first(), tri.second(), tri.third()]
                    .iter()
                    .map(ordinates),
            );
        }
        GeometryType::Line(line) => {
            out.extend([line.start(), line.end()].iter().map(ordinates));
        }
    }
}
