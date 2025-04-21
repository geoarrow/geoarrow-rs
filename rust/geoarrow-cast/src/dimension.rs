use std::sync::Arc;

use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::builder::{LineStringBuilder, PointBuilder};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::error::Result;
use geoarrow_array::{ArrayAccessor, GeoArrowArray, GeoArrowType};
use geoarrow_schema::Dimension;

// TODO: optimize this for separated coord buffers, where you can literally just change the
// dimension in the coord buffer type and leave the buffers the same.
pub fn force_2d(array: &dyn GeoArrowArray) -> Result<Arc<dyn GeoArrowArray>> {
    let out: Arc<dyn GeoArrowArray> = match array.data_type() {
        GeoArrowType::Point(typ) => match typ.dimension() {
            Dimension::XY => Arc::new(array.as_point().clone()),
            _ => {
                let array = array.as_point();
                let mut builder = PointBuilder::with_capacity(
                    typ.with_dimension(Dimension::XY),
                    array.buffer_lengths(),
                );
                for maybe_point in array.iter() {
                    builder.push_point(maybe_point.transpose().unwrap().map(Point2D).as_ref());
                }
                Arc::new(builder.finish())
            }
        },
        GeoArrowType::LineString(typ) => match typ.dimension() {
            Dimension::XY => Arc::new(array.as_line_string().clone()),
            _ => {
                let array = array.as_line_string();
                let mut builder = LineStringBuilder::with_capacity(
                    typ.with_dimension(Dimension::XY),
                    array.buffer_lengths(),
                );
                for maybe_line in array.iter() {
                    builder.push_line_string(
                        maybe_line.transpose().unwrap().map(LineString2D).as_ref(),
                    )?;
                }
                Arc::new(builder.finish())
            }
        },
        _ => panic!("Cannot force 2D on non-geometry type"),
    };
    Ok(out)
}

struct Coord2D<C: CoordTrait<T = f64>>(C);

impl<C: CoordTrait<T = f64>> CoordTrait for Coord2D<C> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn x(&self) -> Self::T {
        self.0.x()
    }

    fn y(&self) -> Self::T {
        self.0.y()
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        match n {
            0 => self.x(),
            1 => self.y(),
            _ => panic!("Invalid dimension index"),
        }
    }
}

struct Point2D<P: PointTrait<T = f64>>(P);

impl<P: PointTrait<T = f64>> PointTrait for Point2D<P> {
    type T = f64;
    type CoordType<'b>
        = Coord2D<P::CoordType<'b>>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        self.0.coord().map(Coord2D)
    }
}

impl<'a, P: PointTrait<T = f64>> PointTrait for &'a Point2D<P> {
    type T = f64;
    type CoordType<'b>
        = Coord2D<P::CoordType<'b>>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        self.0.coord().map(Coord2D)
    }
}

struct Point2DRef<'a, P: PointTrait<T = f64>>(&'a P);

struct LineString2D<L: LineStringTrait<T = f64>>(L);

impl<L: LineStringTrait<T = f64>> LineStringTrait for LineString2D<L> {
    type T = f64;
    type CoordType<'b>
        = Coord2D<L::CoordType<'b>>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn num_coords(&self) -> usize {
        self.0.num_coords()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        Coord2D(unsafe { self.0.coord_unchecked(i) })
    }
}

struct Polygon2D<P: PolygonTrait<T = f64>>(P);

impl<P: PolygonTrait<T = f64>> PolygonTrait for Polygon2D<P> {
    type T = f64;
    type RingType<'a>
        = LineString2D<P::RingType<'a>>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        self.0.exterior().map(LineString2D)
    }

    fn num_interiors(&self) -> usize {
        self.0.num_interiors()
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        LineString2D(unsafe { self.0.interior_unchecked(i) })
    }
}

struct MultiPoint2D<MP: MultiPointTrait<T = f64>>(MP);

impl<MP: MultiPointTrait<T = f64>> MultiPointTrait for MultiPoint2D<MP> {
    type T = f64;
    type PointType<'a>
        = Point2D<MP::PointType<'a>>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn num_points(&self) -> usize {
        self.0.num_points()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        Point2D(unsafe { self.0.point_unchecked(i) })
    }
}

struct MultiLineString2D<ML: MultiLineStringTrait<T = f64>>(ML);

impl<ML: MultiLineStringTrait<T = f64>> MultiLineStringTrait for MultiLineString2D<ML> {
    type T = f64;
    type LineStringType<'a>
        = LineString2D<ML::LineStringType<'a>>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn num_line_strings(&self) -> usize {
        self.0.num_line_strings()
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringType<'_> {
        LineString2D(unsafe { self.0.line_string_unchecked(i) })
    }
}

struct MultiPolygon2D<MP: MultiPolygonTrait<T = f64>>(MP);

impl<MP: MultiPolygonTrait<T = f64>> MultiPolygonTrait for MultiPolygon2D<MP> {
    type T = f64;
    type PolygonType<'a>
        = Polygon2D<MP::PolygonType<'a>>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn num_polygons(&self) -> usize {
        self.0.num_polygons()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::PolygonType<'_> {
        Polygon2D(unsafe { self.0.polygon_unchecked(i) })
    }
}

enum Geometry2D<'a, G: GeometryTrait<T = f64> + 'a> {
    Point(Point2D<G::PointType<'a>>),
    LineString(LineString2D<G::LineStringType<'a>>),
    Polygon(Polygon2D<G::PolygonType<'a>>),
    MultiPoint(MultiPoint2D<G::MultiPointType<'a>>),
    MultiLineString(MultiLineString2D<G::MultiLineStringType<'a>>),
    MultiPolygon(MultiPolygon2D<G::MultiPolygonType<'a>>),
    GeometryCollection(GeometryCollection2D<G::GeometryCollectionType<'a>>),
}

impl<'a, G: GeometryTrait<T = f64> + 'a> Geometry2D<'a, G> {
    fn new(g: G) -> Self {
        match g.as_type() {
            geo_traits::GeometryType::Point(p) => Self::Point(Point2D(p)),
            _ => todo!(),
        }
    }
}

impl<'a, G: GeometryTrait<T = f64>> GeometryTrait for Geometry2D<'a, G> {
    type T = f64;
    type PointType<'b>
        = Point2D<G::PointType<'a>>
    where
        Self: 'b;
    type LineStringType<'b>
        = LineString2D<G::LineStringType<'a>>
    where
        Self: 'b;
    type PolygonType<'b>
        = Polygon2D<G::PolygonType<'a>>
    where
        Self: 'b;
    type MultiPointType<'b>
        = MultiPoint2D<G::MultiPointType<'a>>
    where
        Self: 'b;
    type MultiLineStringType<'b>
        = MultiLineString2D<G::MultiLineStringType<'a>>
    where
        Self: 'b;
    type MultiPolygonType<'b>
        = MultiPolygon2D<G::MultiPolygonType<'a>>
    where
        Self: 'b;
    type GeometryCollectionType<'b>
        = GeometryCollection2D<G::GeometryCollectionType<'a>>
    where
        Self: 'b;
    type RectType<'b>
        = Rect2D<G::RectType<'a>>
    where
        Self: 'b;
    type TriangleType<'b>
        = Triangle2D<G::TriangleType<'a>>
    where
        Self: 'b;
    type LineType<'b>
        = Line2D<G::LineType<'a>>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn as_type(
        &self,
    ) -> geo_traits::GeometryType<
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
            Self::Point(p) => geo_traits::GeometryType::Point(p),
            _ => todo!(),
        }
    }
}

struct GeometryCollection2D<GC: GeometryCollectionTrait<T = f64>>(GC);

impl<GC: GeometryCollectionTrait<T = f64>> GeometryCollectionTrait for GeometryCollection2D<GC> {
    type T = f64;
    type GeometryType<'a>
        = Geometry2D<'a, GC::GeometryType<'a>>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn num_geometries(&self) -> usize {
        self.0.num_geometries()
    }

    unsafe fn geometry_unchecked(&self, i: usize) -> Self::GeometryType<'_> {
        Geometry2D::new(unsafe { self.0.geometry_unchecked(i) })
    }
}

struct Rect2D<R: RectTrait<T = f64>>(R);

impl<R: RectTrait<T = f64>> RectTrait for Rect2D<R> {
    type T = f64;
    type CoordType<'a>
        = Coord2D<R::CoordType<'a>>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn min(&self) -> Self::CoordType<'_> {
        Coord2D(self.0.min())
    }

    fn max(&self) -> Self::CoordType<'_> {
        Coord2D(self.0.max())
    }
}

struct Triangle2D<T: TriangleTrait<T = f64>>(T);

impl<T: TriangleTrait<T = f64>> TriangleTrait for Triangle2D<T> {
    type T = f64;
    type CoordType<'a>
        = Coord2D<T::CoordType<'a>>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn first(&self) -> Self::CoordType<'_> {
        Coord2D(self.0.first())
    }

    fn second(&self) -> Self::CoordType<'_> {
        Coord2D(self.0.second())
    }

    fn third(&self) -> Self::CoordType<'_> {
        Coord2D(self.0.third())
    }
}

struct Line2D<L: LineTrait<T = f64>>(L);

impl<L: LineTrait<T = f64>> LineTrait for Line2D<L> {
    type T = f64;
    type CoordType<'a>
        = Coord2D<L::CoordType<'a>>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn start(&self) -> Self::CoordType<'_> {
        Coord2D(self.0.start())
    }

    fn end(&self) -> Self::CoordType<'_> {
        Coord2D(self.0.end())
    }
}
