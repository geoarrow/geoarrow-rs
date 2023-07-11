use crate::array::{
    GeometryArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray,
    PointArray, PolygonArray, WKBArray,
};
use crate::GeometryArrayTrait;
use arrow2::array::{MutablePrimitiveArray, PrimitiveArray};
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::DataType;
use geo::prelude::Area as GeoArea;

fn zeroes(len: usize, validity: Option<&Bitmap>) -> PrimitiveArray<f64> {
    let values = vec![0.0f64; len];
    PrimitiveArray::new(DataType::Float64, values.into(), validity.cloned())
}

/// Signed and unsigned planar area of a geometry.
///
/// # Examples
///
/// ```
/// use geo::polygon;
/// use geo::Area;
///
/// let mut polygon = polygon![
///     (x: 0., y: 0.),
///     (x: 5., y: 0.),
///     (x: 5., y: 6.),
///     (x: 0., y: 6.),
///     (x: 0., y: 0.),
/// ];
///
/// assert_eq!(polygon.signed_area(), 30.);
/// assert_eq!(polygon.unsigned_area(), 30.);
///
/// polygon.exterior_mut(|line_string| {
///     line_string.0.reverse();
/// });
///
/// assert_eq!(polygon.signed_area(), -30.);
/// assert_eq!(polygon.unsigned_area(), 30.);
/// ```
pub trait Area {
    fn signed_area(&self) -> PrimitiveArray<f64>;

    fn unsigned_area(&self) -> PrimitiveArray<f64>;
}

impl Area for PointArray {
    fn signed_area(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }

    fn unsigned_area(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }
}

impl Area for MultiPointArray {
    fn signed_area(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }

    fn unsigned_area(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }
}

impl Area for LineStringArray {
    fn signed_area(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }

    fn unsigned_area(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }
}

impl Area for MultiLineStringArray {
    fn signed_area(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }

    fn unsigned_area(&self) -> PrimitiveArray<f64> {
        zeroes(self.len(), self.validity())
    }
}

impl Area for PolygonArray {
    fn signed_area(&self) -> PrimitiveArray<f64> {
        let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.signed_area())));
        output_array.into()
    }

    fn unsigned_area(&self) -> PrimitiveArray<f64> {
        let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        output_array.into()
    }
}

impl Area for MultiPolygonArray {
    fn signed_area(&self) -> PrimitiveArray<f64> {
        let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.signed_area())));
        output_array.into()
    }

    fn unsigned_area(&self) -> PrimitiveArray<f64> {
        let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        output_array.into()
    }
}

impl Area for WKBArray {
    fn signed_area(&self) -> PrimitiveArray<f64> {
        let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.signed_area())));
        output_array.into()
    }

    fn unsigned_area(&self) -> PrimitiveArray<f64> {
        let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(self.len());
        self.iter_geo()
            .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        output_array.into()
    }
}

impl Area for GeometryArray {
    fn signed_area(&self) -> PrimitiveArray<f64> {
        match self {
            GeometryArray::WKB(arr) => arr.signed_area(),
            GeometryArray::Point(arr) => arr.signed_area(),
            GeometryArray::LineString(arr) => arr.signed_area(),
            GeometryArray::Polygon(arr) => arr.signed_area(),
            GeometryArray::MultiPoint(arr) => arr.signed_area(),
            GeometryArray::MultiLineString(arr) => arr.signed_area(),
            GeometryArray::MultiPolygon(arr) => arr.signed_area(),
        }
    }

    fn unsigned_area(&self) -> PrimitiveArray<f64> {
        match self {
            GeometryArray::WKB(arr) => arr.unsigned_area(),
            GeometryArray::Point(arr) => arr.unsigned_area(),
            GeometryArray::LineString(arr) => arr.unsigned_area(),
            GeometryArray::Polygon(arr) => arr.unsigned_area(),
            GeometryArray::MultiPoint(arr) => arr.unsigned_area(),
            GeometryArray::MultiLineString(arr) => arr.unsigned_area(),
            GeometryArray::MultiPolygon(arr) => arr.unsigned_area(),
        }
    }
}
