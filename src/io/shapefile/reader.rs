use std::io::{Read, Seek};
use std::path::Path;

use dbase::FieldType;
use shapefile::{Reader, ShapeReader};

use crate::geo_traits::{
    CoordTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait,
    PointTrait, PolygonTrait,
};

pub fn read_shapefile<T: Read + Seek>(shp_reader: T, dbf_reader: T) {
    let dbf_reader = dbase::Reader::new(dbf_reader).unwrap();
    let mut shp_reader = ShapeReader::new(shp_reader).unwrap();

    let header = shp_reader.header();
    // header.shape_type

    let fields = dbf_reader.fields();
    let field = &fields[0];
    // match field.field_type() {
    //     FieldType::
    // }

    let mut reader = Reader::new(shp_reader, dbf_reader);
    for x in reader.iter_shapes_and_records_as::<shapefile::Point, dbase::Record>() {
        let (geom, record) = x.unwrap();
        let y = Point(&geom);
    }
}

struct Point<'a>(&'a shapefile::Point);

impl<'a> PointTrait for Point<'a> {
    type T = f64;

    fn dim(&self) -> usize {
        2
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match n {
            0 => self.0.x,
            1 => self.0.y,
            _ => panic!(),
        }
    }

    fn x(&self) -> Self::T {
        self.0.x
    }

    fn y(&self) -> Self::T {
        self.0.y
    }
}

impl<'a> CoordTrait for Point<'a> {
    type T = f64;

    fn dim(&self) -> usize {
        2
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match n {
            0 => self.0.x,
            1 => self.0.y,
            _ => panic!(),
        }
    }

    fn x(&self) -> Self::T {
        self.0.x
    }

    fn y(&self) -> Self::T {
        self.0.y
    }
}

// Note: PointZ can optionally have M values
struct PointZ<'a>(&'a shapefile::PointZ);

impl<'a> PointTrait for PointZ<'a> {
    type T = f64;

    fn dim(&self) -> usize {
        3
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match n {
            0 => self.0.x,
            1 => self.0.y,
            2 => self.0.z,
            _ => panic!(),
        }
    }

    fn x(&self) -> Self::T {
        self.0.x
    }

    fn y(&self) -> Self::T {
        self.0.y
    }
}

impl<'a> CoordTrait for PointZ<'a> {
    type T = f64;

    fn dim(&self) -> usize {
        3
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match n {
            0 => self.0.x,
            1 => self.0.y,
            2 => self.0.z,
            _ => panic!(),
        }
    }

    fn x(&self) -> Self::T {
        self.0.x
    }

    fn y(&self) -> Self::T {
        self.0.y
    }
}

struct LineString<'a>(&'a [shapefile::Point]);

impl<'a> LineStringTrait for LineString<'a> {
    type T = f64;
    type ItemType<'b> = Point<'a> where Self: 'b;

    fn dim(&self) -> usize {
        2
    }

    fn num_coords(&self) -> usize {
        self.0.len()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        Point(&self.0[i])
    }
}

struct LineStringZ<'a>(&'a [shapefile::PointZ]);

impl<'a> LineStringTrait for LineStringZ<'a> {
    type T = f64;
    type ItemType<'b> = PointZ<'a> where Self: 'b;

    fn dim(&self) -> usize {
        3
    }

    fn num_coords(&self) -> usize {
        self.0.len()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        PointZ(&self.0[i])
    }
}

struct Polygon {
    outer: Vec<shapefile::Point>,
    inner: Vec<Vec<shapefile::Point>>,
}

impl<'a> PolygonTrait for &'a Polygon {
    type T = f64;
    type ItemType<'b> = LineString<'a> where Self: 'b;

    fn dim(&self) -> usize {
        2
    }

    fn num_interiors(&self) -> usize {
        self.inner.len()
    }

    fn exterior(&self) -> Option<Self::ItemType<'_>> {
        Some(LineString(&self.outer))
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        LineString(&self.inner[i])
    }
}

struct PolygonZ {
    outer: Vec<shapefile::PointZ>,
    inner: Vec<Vec<shapefile::PointZ>>,
}

impl<'a> PolygonTrait for &'a PolygonZ {
    type T = f64;
    type ItemType<'b> = LineStringZ<'a> where Self: 'b;

    fn dim(&self) -> usize {
        3
    }

    fn num_interiors(&self) -> usize {
        self.inner.len()
    }

    fn exterior(&self) -> Option<Self::ItemType<'_>> {
        Some(LineStringZ(&self.outer))
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        LineStringZ(&self.inner[i])
    }
}

struct MultiPoint<'a>(&'a shapefile::Multipoint);

impl<'a> MultiPointTrait for MultiPoint<'a> {
    type T = f64;
    type ItemType<'b> = Point<'a> where Self: 'b;

    fn dim(&self) -> usize {
        2
    }

    fn num_points(&self) -> usize {
        self.0.points().len()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        Point(self.0.point(i).unwrap())
    }
}

struct MultiPointZ<'a>(&'a shapefile::MultipointZ);

impl<'a> MultiPointTrait for MultiPointZ<'a> {
    type T = f64;
    type ItemType<'b> = PointZ<'a> where Self: 'b;

    fn dim(&self) -> usize {
        3
    }

    fn num_points(&self) -> usize {
        self.0.points().len()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        PointZ(self.0.point(i).unwrap())
    }
}

struct Polyline<'a>(&'a shapefile::Polyline);

impl<'a> MultiLineStringTrait for Polyline<'a> {
    type T = f64;
    type ItemType<'b> = LineString<'a> where Self: 'b;

    fn dim(&self) -> usize {
        2
    }

    fn num_lines(&self) -> usize {
        self.0.parts().len()
    }

    unsafe fn line_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        LineString(self.0.part(i).unwrap())
    }
}

struct PolylineZ<'a>(&'a shapefile::PolylineZ);

impl<'a> MultiLineStringTrait for PolylineZ<'a> {
    type T = f64;
    type ItemType<'b> = LineStringZ<'a> where Self: 'b;

    fn dim(&self) -> usize {
        3
    }

    fn num_lines(&self) -> usize {
        self.0.parts().len()
    }

    unsafe fn line_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        LineStringZ(self.0.part(i).unwrap())
    }
}

struct MultiPolygon(Vec<Polygon>);

impl MultiPolygon {
    /// This is ported from the geo-types From impl
    /// https://github.com/tmontaigu/shapefile-rs/blob/a27a93ec721d954661620d7f451db53e4bf4e5e9/src/record/polygon.rs#L564
    fn new(geom: shapefile::Polygon) -> Self {
        let mut last_poly = None;
        let mut polygons = Vec::new();
        for ring in geom.into_inner() {
            match ring {
                shapefile::PolygonRing::Outer(points) => {
                    if let Some(poly) = last_poly.take() {
                        polygons.push(poly);
                    }
                    last_poly = Some(Polygon {
                        outer: points,
                        inner: vec![],
                    });
                }
                shapefile::PolygonRing::Inner(points) => {
                    if let Some(poly) = last_poly.as_mut() {
                        poly.inner.push(points);
                    } else {
                        panic!("inner ring without a previous outer ring");
                        // This is the strange (?) case: inner ring without a previous outer ring
                        // polygons.push(geo_types::Polygon::<f64>::new(
                        //     LineString::<f64>::from(Vec::<Coordinate<f64>>::new()),
                        //     vec![LineString::from(interior)],
                        // ));
                    }
                }
            }
        }

        if let Some(poly) = last_poly.take() {
            polygons.push(poly);
        }

        Self(polygons)
    }
}

impl MultiPolygonTrait for MultiPolygon {
    type T = f64;
    type ItemType<'a> = &'a Polygon;

    fn dim(&self) -> usize {
        2
    }

    fn num_polygons(&self) -> usize {
        self.0.len()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        &self.0[i]
    }
}

struct MultiPolygonZ(Vec<PolygonZ>);

impl MultiPolygonZ {
    /// This is ported from the geo-types From impl
    /// https://github.com/tmontaigu/shapefile-rs/blob/a27a93ec721d954661620d7f451db53e4bf4e5e9/src/record/polygon.rs#L564
    fn new(geom: shapefile::PolygonZ) -> Self {
        let mut last_poly = None;
        let mut polygons = Vec::new();
        for ring in geom.into_inner() {
            match ring {
                shapefile::PolygonRing::Outer(points) => {
                    if let Some(poly) = last_poly.take() {
                        polygons.push(poly);
                    }
                    last_poly = Some(PolygonZ {
                        outer: points,
                        inner: vec![],
                    });
                }
                shapefile::PolygonRing::Inner(points) => {
                    if let Some(poly) = last_poly.as_mut() {
                        poly.inner.push(points);
                    } else {
                        panic!("inner ring without a previous outer ring");
                        // This is the strange (?) case: inner ring without a previous outer ring
                        // polygons.push(geo_types::Polygon::<f64>::new(
                        //     LineString::<f64>::from(Vec::<Coordinate<f64>>::new()),
                        //     vec![LineString::from(interior)],
                        // ));
                    }
                }
            }
        }

        if let Some(poly) = last_poly.take() {
            polygons.push(poly);
        }

        Self(polygons)
    }
}

impl MultiPolygonTrait for MultiPolygonZ {
    type T = f64;
    type ItemType<'a> = &'a PolygonZ;

    fn dim(&self) -> usize {
        3
    }

    fn num_polygons(&self) -> usize {
        self.0.len()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        &self.0[i]
    }
}
