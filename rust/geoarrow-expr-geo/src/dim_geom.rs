//! `geo` calculates in two dimensions only. A kernel calculates on an XY projection,
//! then reads its result back through these types, which carry the Z and M values of
//! the input beside the x and y.

use std::borrow::Borrow;

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

/// The ten associated types of `GeometryTrait`, in declaration order.
macro_rules! geometry_assoc_types {
    ($pt:ty, $ls:ty, $poly:ty, $mpt:ty, $mls:ty, $mpoly:ty, $gc:ty) => {
        type T = f64;
        type PointType<'b>
            = $pt
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
            = $mpt
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
            = $gc
        where
            Self: 'b;
        type RectType<'b>
            = NoRect
        where
            Self: 'b;
        type TriangleType<'b>
            = NoTri
        where
            Self: 'b;
        type LineType<'b>
            = NoLine
        where
            Self: 'b;
    };
}

/// `GeometryTrait` is a supertrait of all the other geometry traits. Thus each
/// leaf type below must declare all ten associated types, but it uses only one.
macro_rules! impl_geometry {
    ($ty:ty, $variant:ident, $pt:ty, $ls:ty, $poly:ty, $mpt:ty, $mls:ty, $mpoly:ty, $gc:ty) => {
        impl GeometryTrait for $ty {
            geometry_assoc_types!($pt, $ls, $poly, $mpt, $mls, $mpoly, $gc);

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
    ($ty:ty, Point) => {
        impl_geometry!($ty, Point, Self, NoL, NoY, NoMP, NoML, NoMY, NoGC);
    };
    ($ty:ty, LineString) => {
        impl_geometry!($ty, LineString, NoP, Self, NoY, NoMP, NoML, NoMY, NoGC);
    };
    ($ty:ty, Polygon) => {
        impl_geometry!($ty, Polygon, NoP, NoL, Self, NoMP, NoML, NoMY, NoGC);
    };
    ($ty:ty, MultiPoint) => {
        impl_geometry!($ty, MultiPoint, NoP, NoL, NoY, Self, NoML, NoMY, NoGC);
    };
    ($ty:ty, MultiLineString) => {
        impl_geometry!($ty, MultiLineString, NoP, NoL, NoY, NoMP, Self, NoMY, NoGC);
    };
    ($ty:ty, MultiPolygon) => {
        impl_geometry!($ty, MultiPolygon, NoP, NoL, NoY, NoMP, NoML, Self, NoGC);
    };
    ($ty:ty, GeometryCollection) => {
        impl_geometry!(
            $ty,
            GeometryCollection,
            NoP,
            NoL,
            NoY,
            NoMP,
            NoML,
            NoMY,
            Self
        );
    };
}

// The slots a leaf type does not use, named as the generic parameters of
// `geo_traits` are. Short names keep each arm above on one line.
type NoP = UnimplementedPoint<f64>;
type NoL = UnimplementedLineString<f64>;
type NoY = UnimplementedPolygon<f64>;
type NoMP = UnimplementedMultiPoint<f64>;
type NoML = UnimplementedMultiLineString<f64>;
type NoMY = UnimplementedMultiPolygon<f64>;
type NoGC = UnimplementedGeometryCollection<f64>;
type NoRect = UnimplementedRect<f64>;
type NoTri = UnimplementedTriangle<f64>;
type NoLine = UnimplementedLine<f64>;

/// Borrowed by a kernel that keeps its coordinates in a buffer of its own, owned
/// by a [`DimGeometry`] variant, which cannot borrow.
#[derive(Clone, Copy)]
pub(crate) struct DimLineString<C> {
    pub(crate) dim: Dimensions,
    pub(crate) coords: C,
}

pub(crate) type DimRing<'a> = DimLineString<&'a [Ordinates]>;
pub(crate) type DimLineStringBuf = DimLineString<Vec<Ordinates>>;

impl<C: Borrow<[Ordinates]>> LineStringTrait for DimLineString<C>
where
    Self: GeometryTrait<T = f64>,
{
    type CoordType<'b>
        = DimCoord
    where
        Self: 'b;

    fn num_coords(&self) -> usize {
        self.coords.borrow().len()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        DimCoord {
            dim: self.dim,
            vals: self.coords.borrow()[i],
        }
    }
}

impl_geometry!(DimRing<'_>, LineString);
impl_geometry!(DimLineStringBuf, LineString);

#[derive(Default)]
pub(crate) struct DimPolygonParts {
    pub(crate) exterior: Vec<Ordinates>,
    pub(crate) interiors: Vec<Vec<Ordinates>>,
}

/// Borrowed and owned as [`DimLineString`] is.
#[derive(Clone, Copy)]
pub(crate) struct DimPolygon<P> {
    pub(crate) dim: Dimensions,
    pub(crate) parts: P,
}

pub(crate) type DimPolygonRef<'a> = DimPolygon<&'a DimPolygonParts>;
pub(crate) type DimPolygonBuf = DimPolygon<DimPolygonParts>;

impl<P: Borrow<DimPolygonParts>> PolygonTrait for DimPolygon<P>
where
    Self: GeometryTrait<T = f64>,
{
    type RingType<'b>
        = DimRing<'b>
    where
        Self: 'b;

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        let exterior = &self.parts.borrow().exterior;
        if exterior.is_empty() {
            return None;
        }
        Some(DimRing {
            dim: self.dim,
            coords: exterior,
        })
    }

    fn num_interiors(&self) -> usize {
        self.parts.borrow().interiors.len()
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        DimRing {
            dim: self.dim,
            coords: &self.parts.borrow().interiors[i],
        }
    }
}

impl_geometry!(DimPolygonRef<'_>, Polygon);
impl_geometry!(DimPolygonBuf, Polygon);

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
        = DimPolygonRef<'b>
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

/// An absent coordinate is an empty point.
#[derive(Clone, Copy)]
pub(crate) struct DimPoint {
    pub(crate) dim: Dimensions,
    pub(crate) coord: Option<Ordinates>,
}

impl PointTrait for DimPoint {
    type CoordType<'b>
        = DimCoord
    where
        Self: 'b;

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        self.coord.map(|vals| DimCoord {
            dim: self.dim,
            vals,
        })
    }
}

impl_geometry!(DimPoint, Point);

pub(crate) struct DimMultiPoint {
    pub(crate) dim: Dimensions,
    pub(crate) coords: Vec<Ordinates>,
}

impl MultiPointTrait for DimMultiPoint {
    type InnerPointType<'b>
        = DimPoint
    where
        Self: 'b;

    fn num_points(&self) -> usize {
        self.coords.len()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::InnerPointType<'_> {
        DimPoint {
            dim: self.dim,
            coord: Some(self.coords[i]),
        }
    }
}

impl_geometry!(DimMultiPoint, MultiPoint);

pub(crate) struct DimGeometryCollection {
    pub(crate) dim: Dimensions,
    pub(crate) geometries: Vec<DimGeometry>,
}

impl GeometryCollectionTrait for DimGeometryCollection {
    type GeometryType<'b>
        = &'b DimGeometry
    where
        Self: 'b;

    fn num_geometries(&self) -> usize {
        self.geometries.len()
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::GeometryType<'_> {
        &self.geometries[i]
    }
}

impl_geometry!(DimGeometryCollection, GeometryCollection);

/// A geometry of any type. `Rect`, `Triangle` and `Line` have no variant: a
/// caller lowers them to a polygon or a line string when it builds this.
pub(crate) enum DimGeometry {
    Point(DimPoint),
    LineString(DimLineStringBuf),
    Polygon(DimPolygonBuf),
    MultiPoint(DimMultiPoint),
    MultiLineString(DimMultiLineString),
    MultiPolygon(DimMultiPolygon),
    GeometryCollection(DimGeometryCollection),
}

/// `as_type` dispatches on the variant, unlike the leaf types above.
macro_rules! impl_dim_geometry {
    ($ty:ty) => {
        impl GeometryTrait for $ty {
            geometry_assoc_types!(
                DimPoint,
                DimLineStringBuf,
                DimPolygonBuf,
                DimMultiPoint,
                DimMultiLineString,
                DimMultiPolygon,
                DimGeometryCollection
            );

            fn dim(&self) -> Dimensions {
                match self {
                    DimGeometry::Point(g) => g.dim,
                    DimGeometry::LineString(g) => g.dim,
                    DimGeometry::Polygon(g) => g.dim,
                    DimGeometry::MultiPoint(g) => g.dim,
                    DimGeometry::MultiLineString(g) => g.dim,
                    DimGeometry::MultiPolygon(g) => g.dim,
                    DimGeometry::GeometryCollection(g) => g.dim,
                }
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
                match self {
                    DimGeometry::Point(g) => GeometryType::Point(g),
                    DimGeometry::LineString(g) => GeometryType::LineString(g),
                    DimGeometry::Polygon(g) => GeometryType::Polygon(g),
                    DimGeometry::MultiPoint(g) => GeometryType::MultiPoint(g),
                    DimGeometry::MultiLineString(g) => GeometryType::MultiLineString(g),
                    DimGeometry::MultiPolygon(g) => GeometryType::MultiPolygon(g),
                    DimGeometry::GeometryCollection(g) => GeometryType::GeometryCollection(g),
                }
            }
        }
    };
}

impl_dim_geometry!(DimGeometry);
// A collection yields its members by reference, thus the reference needs the impl too.
impl_dim_geometry!(&DimGeometry);

/// Rebuilds a geometry with `f` applied to each of its coordinates. `Rect`,
/// `Triangle` and `Line` have no [`DimGeometry`] variant, thus each one lowers to
/// a polygon or a line string.
pub(crate) fn map_ordinates<G: GeometryTrait<T = f64>>(
    geom: &G,
    f: &dyn Fn(Ordinates) -> Ordinates,
) -> DimGeometry {
    let dim = geom.dim();
    match geom.as_type() {
        GeometryType::Point(p) => DimGeometry::Point(DimPoint {
            dim,
            coord: p.coord().map(|c| f(ordinates(&c))),
        }),
        GeometryType::LineString(ls) => DimGeometry::LineString(DimLineString {
            dim,
            coords: map_ring(ls, f),
        }),
        GeometryType::Polygon(poly) => DimGeometry::Polygon(DimPolygon {
            dim,
            parts: map_polygon(poly, f),
        }),
        GeometryType::MultiPoint(mp) => DimGeometry::MultiPoint(DimMultiPoint {
            dim,
            coords: mp
                .points()
                .filter_map(|p| p.coord().map(|c| f(ordinates(&c))))
                .collect(),
        }),
        GeometryType::MultiLineString(mls) => DimGeometry::MultiLineString(DimMultiLineString {
            dim,
            line_strings: mls.line_strings().map(|ls| map_ring(&ls, f)).collect(),
        }),
        GeometryType::MultiPolygon(mp) => DimGeometry::MultiPolygon(DimMultiPolygon {
            dim,
            polygons: mp.polygons().map(|poly| map_polygon(&poly, f)).collect(),
        }),
        GeometryType::GeometryCollection(gc) => {
            DimGeometry::GeometryCollection(DimGeometryCollection {
                dim,
                geometries: gc.geometries().map(|g| map_ordinates(&g, f)).collect(),
            })
        }
        // A mapped rect is no longer axis aligned, thus it becomes a ring in the
        // order of `geo::Rect::to_polygon`. A box holds Z and M at its two corners
        // only, thus the two new corners take those of the minimum corner.
        GeometryType::Rect(rect) => {
            let min = ordinates(&rect.min());
            let max = ordinates(&rect.max());
            let mut corner_max_x = min;
            corner_max_x[0] = max[0];
            let mut corner_max_y = min;
            corner_max_y[1] = max[1];
            DimGeometry::Polygon(ring_polygon(
                dim,
                [corner_max_x, max, corner_max_y, min, corner_max_x]
                    .into_iter()
                    .map(f)
                    .collect(),
            ))
        }
        GeometryType::Triangle(tri) => {
            let a = f(ordinates(&tri.first()));
            let b = f(ordinates(&tri.second()));
            let c = f(ordinates(&tri.third()));
            DimGeometry::Polygon(ring_polygon(dim, vec![a, b, c, a]))
        }
        GeometryType::Line(line) => DimGeometry::LineString(DimLineString {
            dim,
            coords: vec![f(ordinates(&line.start())), f(ordinates(&line.end()))],
        }),
    }
}

fn map_ring<L: LineStringTrait<T = f64>>(
    ring: &L,
    f: &dyn Fn(Ordinates) -> Ordinates,
) -> Vec<Ordinates> {
    ring.coords().map(|c| f(ordinates(&c))).collect()
}

fn map_polygon<Y: PolygonTrait<T = f64>>(
    poly: &Y,
    f: &dyn Fn(Ordinates) -> Ordinates,
) -> DimPolygonParts {
    DimPolygonParts {
        exterior: poly.exterior().map(|r| map_ring(&r, f)).unwrap_or_default(),
        interiors: poly.interiors().map(|r| map_ring(&r, f)).collect(),
    }
}

fn ring_polygon(dim: Dimensions, exterior: Vec<Ordinates>) -> DimPolygonBuf {
    DimPolygon {
        dim,
        parts: DimPolygonParts {
            exterior,
            interiors: Vec::new(),
        },
    }
}

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
