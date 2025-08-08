//! Note: This is copied and updated from the geoparquet crate.

use std::ops::Add;

use arrow_array::Float64Array;
use arrow_array::builder::Float64Builder;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    UnimplementedGeometryCollection, UnimplementedLine, UnimplementedLineString,
    UnimplementedMultiLineString, UnimplementedMultiPoint, UnimplementedMultiPolygon,
    UnimplementedPoint, UnimplementedPolygon, UnimplementedTriangle,
};
use geoarrow_array::array::RectArray;
use geoarrow_array::builder::RectBuilder;
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::scalar::Rect;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{BoxType, Dimension, GeoArrowType};
use wkt::types::Coord;

#[derive(Debug, Clone, Copy)]
pub(crate) struct BoundingRect {
    pub(crate) minx: f64,
    pub(crate) miny: f64,
    pub(crate) minz: f64,
    pub(crate) maxx: f64,
    pub(crate) maxy: f64,
    pub(crate) maxz: f64,
    /// If `True`, expose itself as a 3D bounding box through geo-traits APIs, otherwise 2D. This
    /// is needed because the coord builders currently require that the declare dimension of the
    /// added coordinate matches the stated dimension when the builder is created.
    /// See <https://github.com/geoarrow/geoarrow-rs/issues/1300>
    include_z: bool,
}

impl BoundingRect {
    /// New
    pub fn new(include_z: bool) -> Self {
        BoundingRect {
            minx: f64::INFINITY,
            miny: f64::INFINITY,
            minz: f64::INFINITY,
            maxx: -f64::INFINITY,
            maxy: -f64::INFINITY,
            maxz: -f64::INFINITY,
            include_z,
        }
    }

    pub fn minx(&self) -> f64 {
        self.minx
    }

    pub fn miny(&self) -> f64 {
        self.miny
    }

    fn minz(&self) -> Option<f64> {
        if self.minz == f64::INFINITY {
            None
        } else {
            Some(self.minz)
        }
    }

    pub fn maxx(&self) -> f64 {
        self.maxx
    }

    pub fn maxy(&self) -> f64 {
        self.maxy
    }

    fn maxz(&self) -> Option<f64> {
        if self.maxz == -f64::INFINITY {
            None
        } else {
            Some(self.maxz)
        }
    }

    fn add_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        let x = coord.x();
        let y = coord.y();
        let z = coord.nth(2);

        if x < self.minx {
            self.minx = x;
        }
        if y < self.miny {
            self.miny = y;
        }
        if let Some(z) = z {
            if z < self.minz {
                self.minz = z;
            }
        }

        if x > self.maxx {
            self.maxx = x;
        }
        if y > self.maxy {
            self.maxy = y;
        }
        if let Some(z) = z {
            if z > self.maxz {
                self.maxz = z;
            }
        }
    }

    fn add_point(&mut self, point: &impl PointTrait<T = f64>) {
        if let Some(coord) = point.coord() {
            self.add_coord(&coord);
        }
    }

    fn add_line_string(&mut self, line_string: &impl LineStringTrait<T = f64>) {
        for coord in line_string.coords() {
            self.add_coord(&coord);
        }
    }

    fn add_polygon(&mut self, polygon: &impl PolygonTrait<T = f64>) {
        if let Some(exterior_ring) = polygon.exterior() {
            self.add_line_string(&exterior_ring);
        }

        for exterior in polygon.interiors() {
            self.add_line_string(&exterior)
        }
    }

    fn add_multi_point(&mut self, multi_point: &impl MultiPointTrait<T = f64>) {
        for point in multi_point.points() {
            self.add_point(&point);
        }
    }

    fn add_multi_line_string(&mut self, multi_line_string: &impl MultiLineStringTrait<T = f64>) {
        for linestring in multi_line_string.line_strings() {
            self.add_line_string(&linestring);
        }
    }

    fn add_multi_polygon(&mut self, multi_polygon: &impl MultiPolygonTrait<T = f64>) {
        for polygon in multi_polygon.polygons() {
            self.add_polygon(&polygon);
        }
    }

    fn add_geometry(&mut self, geometry: &impl GeometryTrait<T = f64>) {
        use GeometryType::*;

        match geometry.as_type() {
            Point(g) => self.add_point(g),
            LineString(g) => self.add_line_string(g),
            Polygon(g) => self.add_polygon(g),
            MultiPoint(g) => self.add_multi_point(g),
            MultiLineString(g) => self.add_multi_line_string(g),
            MultiPolygon(g) => self.add_multi_polygon(g),
            GeometryCollection(g) => self.add_geometry_collection(g),
            Rect(g) => self.add_rect(g),
            Triangle(_) | Line(_) => unreachable!(),
        }
    }

    fn add_geometry_collection(
        &mut self,
        geometry_collection: &impl GeometryCollectionTrait<T = f64>,
    ) {
        for geometry in geometry_collection.geometries() {
            self.add_geometry(&geometry);
        }
    }

    fn add_rect(&mut self, rect: &impl RectTrait<T = f64>) {
        self.add_coord(&rect.min());
        self.add_coord(&rect.max());
    }

    pub fn update(&mut self, other: &BoundingRect) {
        self.add_rect(other)
    }
}

impl Add for BoundingRect {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(self.include_z, rhs.include_z);
        BoundingRect {
            minx: self.minx.min(rhs.minx),
            miny: self.miny.min(rhs.miny),
            minz: self.minz.min(rhs.minz),
            maxx: self.maxx.max(rhs.maxx),
            maxy: self.maxy.max(rhs.maxy),
            maxz: self.maxz.max(rhs.maxz),
            include_z: self.include_z,
        }
    }
}

impl RectTrait for BoundingRect {
    type CoordType<'a> = wkt::types::Coord;

    fn min(&self) -> Self::CoordType<'_> {
        let mut c = Coord {
            x: self.minx,
            y: self.miny,
            z: None,
            m: None,
        };
        if self.include_z && self.minz != f64::INFINITY {
            c.z = Some(self.minz);
        }
        c
    }

    fn max(&self) -> Self::CoordType<'_> {
        let mut c = Coord {
            x: self.maxx,
            y: self.maxy,
            z: None,
            m: None,
        };
        if self.include_z && self.maxz != -f64::INFINITY {
            c.z = Some(self.maxz);
        }
        c
    }
}

impl GeometryTrait for BoundingRect {
    type T = f64;
    type PointType<'a>
        = UnimplementedPoint<f64>
    where
        Self: 'a;
    type LineStringType<'a>
        = UnimplementedLineString<f64>
    where
        Self: 'a;
    type PolygonType<'a>
        = UnimplementedPolygon<f64>
    where
        Self: 'a;
    type MultiPointType<'a>
        = UnimplementedMultiPoint<f64>
    where
        Self: 'a;
    type MultiLineStringType<'a>
        = UnimplementedMultiLineString<f64>
    where
        Self: 'a;
    type MultiPolygonType<'a>
        = UnimplementedMultiPolygon<f64>
    where
        Self: 'a;
    type GeometryCollectionType<'a>
        = UnimplementedGeometryCollection<f64>
    where
        Self: 'a;
    type RectType<'a>
        = Self
    where
        Self: 'a;
    type TriangleType<'a>
        = UnimplementedTriangle<f64>
    where
        Self: 'a;
    type LineType<'a>
        = UnimplementedLine<f64>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        if self.include_z && self.minz().is_some() && self.maxz().is_some() {
            geo_traits::Dimensions::Xyz
        } else {
            geo_traits::Dimensions::Xy
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
        GeometryType::Rect(self)
    }
}

/// Create a new RectArray using the bounding box of each geometry.
///
/// Note that this is fully planar and **does not** handle the antimeridian for geographic
/// coordinates.
pub(crate) fn bounding_rect(arr: &dyn GeoArrowArray, include_z: bool) -> GeoArrowResult<RectArray> {
    if let Some(rect_arr) = arr.as_rect_opt() {
        Ok(rect_arr.clone())
    } else {
        downcast_geoarrow_array!(arr, impl_array_accessor, include_z)
    }
}

/// The actual implementation of computing the bounding rect
fn impl_array_accessor<'a>(
    arr: &'a impl GeoArrowArrayAccessor<'a>,
    include_z: bool,
) -> GeoArrowResult<RectArray> {
    match arr.data_type() {
        GeoArrowType::Rect(_) => unreachable!(),
        _ => {
            let dim = if include_z {
                Dimension::XYZ
            } else {
                Dimension::XY
            };
            let mut builder = RectBuilder::with_capacity(
                BoxType::new(dim, arr.data_type().metadata().clone()),
                arr.len(),
            );
            for item in arr.iter() {
                if let Some(item) = item {
                    let mut rect = BoundingRect::new(include_z);
                    rect.add_geometry(&item?);
                    builder.push_rect(Some(&rect));
                } else {
                    builder.push_null();
                }
            }
            Ok(builder.finish())
        }
    }
}

/// Get the total bounds (i.e. minx, miny, maxx, maxy) of the entire geoarrow array.
pub(crate) fn total_bounds(arr: &dyn GeoArrowArray) -> GeoArrowResult<BoundingRect> {
    downcast_geoarrow_array!(arr, impl_total_bounds)
}

/// The actual implementation of computing the total bounds
fn impl_total_bounds<'a>(arr: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<BoundingRect> {
    let mut rect = BoundingRect::new(false);

    for item in arr.iter().flatten() {
        rect.add_geometry(&item?);
    }

    Ok(rect)
}

/// The actual implementation of computing the bounding rect
///
/// include_z: If true, the Z dimension is included in the initial bounding box calculations.
pub(crate) fn impl_extrema(
    arr: &dyn GeoArrowArray,
    include_z: bool,
    cb: impl Fn(Rect) -> f64,
) -> GeoArrowResult<Float64Array> {
    let rect_array = bounding_rect(arr, include_z)?;

    let mut output_array = Float64Builder::with_capacity(arr.len());
    for rect in rect_array.iter() {
        if let Some(rect) = rect {
            output_array.append_value(cb(rect?));
        } else {
            output_array.append_null();
        }
    }

    Ok(output_array.finish())
}
