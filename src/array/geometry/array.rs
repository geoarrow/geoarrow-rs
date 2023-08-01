use arrow2::array::Array;
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::DataType;
use arrow2::types::Offset;
use rstar::RTree;

use crate::array::{
    GeometryCollectionArray, LineStringArray, MultiLineStringArray, MultiPointArray,
    MultiPolygonArray, PointArray, PolygonArray, RectArray, WKBArray,
};
use crate::error::GeoArrowError;
use crate::scalar::Geometry;
use crate::GeometryArrayTrait;

/// A GeometryArray that can be any of various underlying geometry types
#[derive(Debug, Clone, PartialEq)]
pub enum GeometryArray<O: Offset> {
    Point(PointArray),
    LineString(LineStringArray<O>),
    Polygon(PolygonArray<O>),
    MultiPoint(MultiPointArray<O>),
    MultiLineString(MultiLineStringArray<O>),
    MultiPolygon(MultiPolygonArray<O>),
    GeometryCollection(GeometryCollectionArray<O>),
    WKB(WKBArray<O>),
    Rect(RectArray),
}

impl<'a, O: Offset> GeometryArrayTrait<'a> for GeometryArray<O> {
    type Scalar = crate::scalar::Geometry<'a, O>;
    type ScalarGeo = geo::Geometry;
    type ArrowArray = Box<dyn Array>;
    type RTreeObject = Self::Scalar;

    fn value(&'a self, i: usize) -> Self::Scalar {
        use GeometryArray::*;
        match self {
            Point(arr) => Geometry::Point(arr.value(i)),
            LineString(arr) => Geometry::LineString(arr.value(i)),
            Polygon(arr) => Geometry::Polygon(arr.value(i)),
            MultiPoint(arr) => Geometry::MultiPoint(arr.value(i)),
            MultiLineString(arr) => Geometry::MultiLineString(arr.value(i)),
            MultiPolygon(arr) => Geometry::MultiPolygon(arr.value(i)),
            GeometryCollection(arr) => Geometry::GeometryCollection(arr.value(i)),
            WKB(arr) => Geometry::WKB(arr.value(i)),
            Rect(arr) => Geometry::Rect(arr.value(i)),
        }
    }

    fn logical_type(&self) -> DataType {
        use GeometryArray::*;
        match self {
            Point(arr) => arr.logical_type(),
            LineString(arr) => arr.logical_type(),
            Polygon(arr) => arr.logical_type(),
            MultiPoint(arr) => arr.logical_type(),
            MultiLineString(arr) => arr.logical_type(),
            MultiPolygon(arr) => arr.logical_type(),
            GeometryCollection(arr) => arr.logical_type(),
            WKB(arr) => arr.logical_type(),
            Rect(arr) => arr.logical_type(),
        }
    }

    fn extension_type(&self) -> DataType {
        use GeometryArray::*;
        match self {
            Point(arr) => arr.extension_type(),
            LineString(arr) => arr.extension_type(),
            Polygon(arr) => arr.extension_type(),
            MultiPoint(arr) => arr.extension_type(),
            MultiLineString(arr) => arr.extension_type(),
            MultiPolygon(arr) => arr.extension_type(),
            GeometryCollection(arr) => arr.extension_type(),
            WKB(arr) => arr.extension_type(),
            Rect(arr) => arr.extension_type(),
        }
    }

    fn into_arrow(self) -> Self::ArrowArray {
        use GeometryArray::*;
        match self {
            Point(arr) => arr.into_arrow(),
            LineString(arr) => arr.into_arrow().boxed(),
            Polygon(arr) => arr.into_arrow().boxed(),
            MultiPoint(arr) => arr.into_arrow().boxed(),
            MultiLineString(arr) => arr.into_arrow().boxed(),
            MultiPolygon(arr) => arr.into_arrow().boxed(),
            GeometryCollection(arr) => arr.into_arrow().boxed(),
            WKB(arr) => arr.into_arrow().boxed(),
            Rect(arr) => arr.into_arrow().boxed(),
        }
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow()
    }

    fn with_coords(self, coords: crate::array::CoordBuffer) -> Self {
        use GeometryArray::*;
        match self {
            Point(arr) => Point(arr.with_coords(coords)),
            LineString(arr) => LineString(arr.with_coords(coords)),
            Polygon(arr) => Polygon(arr.with_coords(coords)),
            MultiPoint(arr) => MultiPoint(arr.with_coords(coords)),
            MultiLineString(arr) => MultiLineString(arr.with_coords(coords)),
            MultiPolygon(arr) => MultiPolygon(arr.with_coords(coords)),
            GeometryCollection(arr) => GeometryCollection(arr.with_coords(coords)),
            WKB(arr) => WKB(arr.with_coords(coords)),
            Rect(arr) => Rect(arr.with_coords(coords)),
        }
    }

    fn coord_type(&self) -> crate::array::CoordType {
        use GeometryArray::*;
        match self {
            Point(arr) => arr.coord_type(),
            LineString(arr) => arr.coord_type(),
            Polygon(arr) => arr.coord_type(),
            MultiPoint(arr) => arr.coord_type(),
            MultiLineString(arr) => arr.coord_type(),
            MultiPolygon(arr) => arr.coord_type(),
            GeometryCollection(arr) => arr.coord_type(),
            WKB(arr) => arr.coord_type(),
            Rect(arr) => arr.coord_type(),
        }
    }

    fn into_coord_type(self, coord_type: crate::array::CoordType) -> Self {
        use GeometryArray::*;
        match self {
            Point(arr) => Point(arr.into_coord_type(coord_type)),
            LineString(arr) => LineString(arr.into_coord_type(coord_type)),
            Polygon(arr) => Polygon(arr.into_coord_type(coord_type)),
            MultiPoint(arr) => MultiPoint(arr.into_coord_type(coord_type)),
            MultiLineString(arr) => MultiLineString(arr.into_coord_type(coord_type)),
            MultiPolygon(arr) => MultiPolygon(arr.into_coord_type(coord_type)),
            GeometryCollection(arr) => GeometryCollection(arr.into_coord_type(coord_type)),
            WKB(arr) => WKB(arr.into_coord_type(coord_type)),
            Rect(arr) => Rect(arr.into_coord_type(coord_type)),
        }
    }

    fn rstar_tree(&'a self) -> RTree<Self::Scalar> {
        let elements: Vec<_> = (0..self.len())
            .filter_map(|geom_idx| self.get(geom_idx))
            .collect();
        RTree::bulk_load(elements)
    }

    /// The length of the [`GeometryArray`]. Every array has a length corresponding to the number
    /// of geometries it contains.
    fn len(&self) -> usize {
        use GeometryArray::*;
        match self {
            Point(arr) => arr.len(),
            LineString(arr) => arr.len(),
            Polygon(arr) => arr.len(),
            MultiPoint(arr) => arr.len(),
            MultiLineString(arr) => arr.len(),
            MultiPolygon(arr) => arr.len(),
            GeometryCollection(arr) => arr.len(),
            WKB(arr) => arr.len(),
            Rect(arr) => arr.len(),
        }
    }

    /// The validity of the [`GeometryArray`]: every array has an optional [`Bitmap`] that, when
    /// available specifies whether the geometry at a given slot is valid or not (null). When the
    /// validity is [`None`], all slots are valid.
    fn validity(&self) -> Option<&Bitmap> {
        use GeometryArray::*;
        match self {
            Point(arr) => arr.validity(),
            LineString(arr) => arr.validity(),
            Polygon(arr) => arr.validity(),
            MultiPoint(arr) => arr.validity(),
            MultiLineString(arr) => arr.validity(),
            MultiPolygon(arr) => arr.validity(),
            GeometryCollection(arr) => arr.validity(),
            WKB(arr) => arr.validity(),
            Rect(arr) => arr.validity(),
        }
    }

    /// Slices the [`GeometryArray`] in place
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct to the heap.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    fn slice(&mut self, offset: usize, length: usize) {
        use GeometryArray::*;
        match self {
            Point(arr) => arr.slice(offset, length),
            LineString(arr) => arr.slice(offset, length),
            Polygon(arr) => arr.slice(offset, length),
            MultiPoint(arr) => arr.slice(offset, length),
            MultiLineString(arr) => arr.slice(offset, length),
            MultiPolygon(arr) => arr.slice(offset, length),
            GeometryCollection(arr) => arr.slice(offset, length),
            WKB(arr) => arr.slice(offset, length),
            Rect(arr) => arr.slice(offset, length),
        }
    }

    /// Slices the [`GeometryArray`] in place
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct to the heap.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        use GeometryArray::*;
        match self {
            Point(arr) => arr.slice_unchecked(offset, length),
            LineString(arr) => arr.slice_unchecked(offset, length),
            Polygon(arr) => arr.slice_unchecked(offset, length),
            MultiPoint(arr) => arr.slice_unchecked(offset, length),
            MultiLineString(arr) => arr.slice_unchecked(offset, length),
            MultiPolygon(arr) => arr.slice_unchecked(offset, length),
            GeometryCollection(arr) => arr.slice_unchecked(offset, length),
            WKB(arr) => arr.slice_unchecked(offset, length),
            Rect(arr) => arr.slice_unchecked(offset, length),
        }
    }

    // /// Clones this [`GeometryArray`] with a new assigned bitmap.
    // /// # Panic
    // /// This function panics iff `validity.len() != self.len()`.
    // pub fn with_validity(&self, validity: Option<Bitmap>) -> Box<GeometryArrayTrait>;

    /// Clone a [`GeometryArray`] to an owned `Box<GeometryArray>`.
    fn to_boxed(&self) -> Box<GeometryArray<O>> {
        use GeometryArray::*;
        Box::new(match self {
            Point(arr) => Point(arr.clone()),
            LineString(arr) => LineString(arr.clone()),
            Polygon(arr) => Polygon(arr.clone()),
            MultiPoint(arr) => MultiPoint(arr.clone()),
            MultiLineString(arr) => MultiLineString(arr.clone()),
            MultiPolygon(arr) => MultiPolygon(arr.clone()),
            GeometryCollection(arr) => GeometryCollection(arr.clone()),
            WKB(arr) => WKB(arr.clone()),
            Rect(arr) => Rect(arr.clone()),
        })
    }
}

impl TryFrom<&dyn Array> for GeometryArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::Extension(extension_name, _field, _extension_meta) => {
                match extension_name.as_str() {
                    "geoarrow.point" => Ok(GeometryArray::Point(value.try_into()?)),
                    "geoarrow.linestring" => Ok(GeometryArray::LineString(value.try_into()?)),
                    "geoarrow.polygon" => Ok(GeometryArray::Polygon(value.try_into()?)),
                    "geoarrow.multipoint" => Ok(GeometryArray::MultiPoint(value.try_into()?)),
                    "geoarrow.multilinestring" => {
                        Ok(GeometryArray::MultiLineString(value.try_into()?))
                    }
                    "geoarrow.multipolygon" => Ok(GeometryArray::MultiPolygon(value.try_into()?)),
                    "geoarrow.wkb" => Ok(GeometryArray::WKB(value.try_into()?)),
                    _ => Err(GeoArrowError::General(format!(
                        "Unknown geoarrow type {}",
                        extension_name
                    ))),
                }
            }
            // TODO: better error here, and document that arrays without geoarrow extension
            // metadata should use TryFrom for a specific geometry type directly, instead of using
            // GeometryArray
            _ => todo!(),
        }
    }
}

impl TryFrom<&dyn Array> for GeometryArray<i64> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::Extension(extension_name, _field, _extension_meta) => {
                match extension_name.as_str() {
                    "geoarrow.point" => Ok(GeometryArray::Point(value.try_into()?)),
                    "geoarrow.linestring" => Ok(GeometryArray::LineString(value.try_into()?)),
                    "geoarrow.polygon" => Ok(GeometryArray::Polygon(value.try_into()?)),
                    "geoarrow.multipoint" => Ok(GeometryArray::MultiPoint(value.try_into()?)),
                    "geoarrow.multilinestring" => {
                        Ok(GeometryArray::MultiLineString(value.try_into()?))
                    }
                    "geoarrow.multipolygon" => Ok(GeometryArray::MultiPolygon(value.try_into()?)),
                    "geoarrow.wkb" => Ok(GeometryArray::WKB(value.try_into()?)),
                    _ => Err(GeoArrowError::General(format!(
                        "Unknown geoarrow type {}",
                        extension_name
                    ))),
                }
            }
            // TODO: better error here, and document that arrays without geoarrow extension
            // metadata should use TryFrom for a specific geometry type directly, instead of using
            // GeometryArray
            _ => todo!(),
        }
    }
}

// TODO: write a macro to dedupe these `From`s
impl<O: Offset> From<PointArray> for GeometryArray<O> {
    fn from(value: PointArray) -> Self {
        GeometryArray::Point(value)
    }
}

impl<O: Offset> From<LineStringArray<O>> for GeometryArray<O> {
    fn from(value: LineStringArray<O>) -> Self {
        GeometryArray::LineString(value)
    }
}

impl<O: Offset> From<PolygonArray<O>> for GeometryArray<O> {
    fn from(value: PolygonArray<O>) -> Self {
        GeometryArray::Polygon(value)
    }
}

impl<O: Offset> From<MultiPointArray<O>> for GeometryArray<O> {
    fn from(value: MultiPointArray<O>) -> Self {
        GeometryArray::MultiPoint(value)
    }
}

impl<O: Offset> From<MultiLineStringArray<O>> for GeometryArray<O> {
    fn from(value: MultiLineStringArray<O>) -> Self {
        GeometryArray::MultiLineString(value)
    }
}

impl<O: Offset> From<MultiPolygonArray<O>> for GeometryArray<O> {
    fn from(value: MultiPolygonArray<O>) -> Self {
        GeometryArray::MultiPolygon(value)
    }
}

impl<O: Offset> From<WKBArray<O>> for GeometryArray<O> {
    fn from(value: WKBArray<O>) -> Self {
        GeometryArray::WKB(value)
    }
}

impl From<GeometryArray<i32>> for GeometryArray<i64> {
    fn from(value: GeometryArray<i32>) -> Self {
        use GeometryArray::*;
        match value {
            Point(arr) => Point(arr),
            LineString(arr) => LineString(arr.into()),
            Polygon(arr) => Polygon(arr.into()),
            MultiPoint(arr) => MultiPoint(arr.into()),
            MultiLineString(arr) => MultiLineString(arr.into()),
            MultiPolygon(arr) => MultiPolygon(arr.into()),
            GeometryCollection(_arr) => todo!(), // GeometryCollection(arr.into()),
            WKB(arr) => WKB(arr.into()),
            Rect(arr) => Rect(arr),
        }
    }
}

impl TryFrom<GeometryArray<i64>> for GeometryArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: GeometryArray<i64>) -> Result<Self, Self::Error> {
        use GeometryArray::*;
        Ok(match value {
            Point(arr) => Point(arr),
            LineString(arr) => LineString(arr.try_into()?),
            Polygon(arr) => Polygon(arr.try_into()?),
            MultiPoint(arr) => MultiPoint(arr.try_into()?),
            MultiLineString(arr) => MultiLineString(arr.try_into()?),
            MultiPolygon(arr) => MultiPolygon(arr.try_into()?),
            GeometryCollection(_arr) => todo!(), // GeometryCollection(arr.into()),
            WKB(arr) => WKB(arr.try_into()?),
            Rect(arr) => Rect(arr),
        })
    }
}
