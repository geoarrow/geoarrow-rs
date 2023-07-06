use arrow2::array::Array;
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::DataType;

use crate::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray, WKBArray,
};
use crate::scalar::Geometry;
use crate::GeometryArrayTrait;

/// A GeometryArray that can be any of various underlying geometry types
#[derive(Debug, Clone)]
pub enum GeometryArray {
    Point(PointArray),
    LineString(LineStringArray),
    Polygon(PolygonArray),
    MultiPoint(MultiPointArray),
    MultiLineString(MultiLineStringArray),
    MultiPolygon(MultiPolygonArray),
    WKB(WKBArray),
}

impl<'a> GeometryArrayTrait<'a> for GeometryArray {
    type Scalar = crate::scalar::Geometry<'a>;
    type ScalarGeo = geo::Geometry;
    type ArrowArray = Box<dyn Array>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        match self {
            GeometryArray::Point(arr) => Geometry::Point(arr.value(i)),
            GeometryArray::LineString(arr) => Geometry::LineString(arr.value(i)),
            GeometryArray::Polygon(arr) => Geometry::Polygon(arr.value(i)),
            GeometryArray::MultiPoint(arr) => Geometry::MultiPoint(arr.value(i)),
            GeometryArray::MultiLineString(arr) => Geometry::MultiLineString(arr.value(i)),
            GeometryArray::MultiPolygon(arr) => Geometry::MultiPolygon(arr.value(i)),
            GeometryArray::WKB(arr) => Geometry::WKB(arr.value(i)),
        }
    }

    fn logical_type(&self) -> DataType {
        match self {
            GeometryArray::Point(arr) => arr.logical_type(),
            GeometryArray::LineString(arr) => arr.logical_type(),
            GeometryArray::Polygon(arr) => arr.logical_type(),
            GeometryArray::MultiPoint(arr) => arr.logical_type(),
            GeometryArray::MultiLineString(arr) => arr.logical_type(),
            GeometryArray::MultiPolygon(arr) => arr.logical_type(),
            GeometryArray::WKB(arr) => arr.logical_type(),
        }
    }

    fn extension_type(&self) -> DataType {
        match self {
            GeometryArray::Point(arr) => arr.extension_type(),
            GeometryArray::LineString(arr) => arr.extension_type(),
            GeometryArray::Polygon(arr) => arr.extension_type(),
            GeometryArray::MultiPoint(arr) => arr.extension_type(),
            GeometryArray::MultiLineString(arr) => arr.extension_type(),
            GeometryArray::MultiPolygon(arr) => arr.extension_type(),
            GeometryArray::WKB(arr) => arr.extension_type(),
        }
    }

    fn into_arrow(self) -> Self::ArrowArray {
        match self {
            GeometryArray::Point(arr) => arr.into_arrow(),
            GeometryArray::LineString(arr) => arr.into_arrow().boxed(),
            GeometryArray::Polygon(arr) => arr.into_arrow().boxed(),
            GeometryArray::MultiPoint(arr) => arr.into_arrow().boxed(),
            GeometryArray::MultiLineString(arr) => arr.into_arrow().boxed(),
            GeometryArray::MultiPolygon(arr) => arr.into_arrow().boxed(),
            GeometryArray::WKB(arr) => arr.into_arrow().boxed(),
        }
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow()
    }

    // fn rstar_tree(&'a self) -> rstar::RTree<Self::Scalar> {
    //     let mut tree = RTree::new();
    //     (0..self.len())
    //         .filter_map(|geom_idx| self.get(geom_idx))
    //         .for_each(|geom| tree.insert(geom));
    //     tree
    // }

    /// The length of the [`GeometryArray`]. Every array has a length corresponding to the number
    /// of geometries it contains.
    fn len(&self) -> usize {
        match self {
            GeometryArray::Point(arr) => arr.len(),
            GeometryArray::LineString(arr) => arr.len(),
            GeometryArray::Polygon(arr) => arr.len(),
            GeometryArray::MultiPoint(arr) => arr.len(),
            GeometryArray::MultiLineString(arr) => arr.len(),
            GeometryArray::MultiPolygon(arr) => arr.len(),
            GeometryArray::WKB(arr) => arr.len(),
        }
    }

    /// The validity of the [`GeometryArray`]: every array has an optional [`Bitmap`] that, when
    /// available specifies whether the geometry at a given slot is valid or not (null). When the
    /// validity is [`None`], all slots are valid.
    fn validity(&self) -> Option<&Bitmap> {
        match self {
            GeometryArray::Point(arr) => arr.validity(),
            GeometryArray::LineString(arr) => arr.validity(),
            GeometryArray::Polygon(arr) => arr.validity(),
            GeometryArray::MultiPoint(arr) => arr.validity(),
            GeometryArray::MultiLineString(arr) => arr.validity(),
            GeometryArray::MultiPolygon(arr) => arr.validity(),
            GeometryArray::WKB(arr) => arr.validity(),
        }
    }

    /// Slices the [`GeometryArray`] in place
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct to the heap.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    fn slice(&mut self, offset: usize, length: usize) {
        match self {
            GeometryArray::Point(arr) => arr.slice(offset, length),
            GeometryArray::LineString(arr) => arr.slice(offset, length),
            GeometryArray::Polygon(arr) => arr.slice(offset, length),
            GeometryArray::MultiPoint(arr) => arr.slice(offset, length),
            GeometryArray::MultiLineString(arr) => arr.slice(offset, length),
            GeometryArray::MultiPolygon(arr) => arr.slice(offset, length),
            GeometryArray::WKB(arr) => arr.slice(offset, length),
        }
    }

    /// Slices the [`GeometryArray`] in place
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct to the heap.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        match self {
            GeometryArray::Point(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::LineString(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::Polygon(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::MultiPoint(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::MultiLineString(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::MultiPolygon(arr) => arr.slice_unchecked(offset, length),
            GeometryArray::WKB(arr) => arr.slice_unchecked(offset, length),
        }
    }

    // /// Clones this [`GeometryArray`] with a new assigned bitmap.
    // /// # Panic
    // /// This function panics iff `validity.len() != self.len()`.
    // pub fn with_validity(&self, validity: Option<Bitmap>) -> Box<GeometryArrayTrait>;

    /// Clone a [`GeometryArray`] to an owned `Box<GeometryArray>`.
    fn to_boxed(&self) -> Box<GeometryArray> {
        Box::new(match self {
            GeometryArray::Point(arr) => GeometryArray::Point(arr.clone()),
            GeometryArray::LineString(arr) => GeometryArray::LineString(arr.clone()),
            GeometryArray::Polygon(arr) => GeometryArray::Polygon(arr.clone()),
            GeometryArray::MultiPoint(arr) => GeometryArray::MultiPoint(arr.clone()),
            GeometryArray::MultiLineString(arr) => GeometryArray::MultiLineString(arr.clone()),
            GeometryArray::MultiPolygon(arr) => GeometryArray::MultiPolygon(arr.clone()),
            GeometryArray::WKB(arr) => GeometryArray::WKB(arr.clone()),
        })
    }
}

impl GeometryArray {
    /// Convert an [`arrow2`] [`Array`] to a [`GeometryArray`].
    pub fn from_arrow(_arr: &dyn Array, _is_multi: bool) -> Self {
        todo!()
        //     match arr.data_type() {
        //         DataType::LargeBinary => {
        //             let lit_arr = arr.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
        //             GeometryArray::WKB(lit_arr.clone().into())
        //         }
        //         DataType::Struct(_) => {
        //             let lit_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
        //             GeometryArray::Point(lit_arr.clone().try_into().unwrap())
        //         }
        //         DataType::List(dt) | DataType::LargeList(dt) => match dt.data_type() {
        //             DataType::Struct(_) => {
        //                 let lit_arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();

        //                 if is_multi {
        //                     GeometryArray::MultiPoint(lit_arr.clone().try_into().unwrap())
        //                 } else {
        //                     GeometryArray::LineString(lit_arr.clone().try_into().unwrap())
        //                 }
        //             }
        //             DataType::List(dt2) | DataType::LargeList(dt2) => match dt2.data_type() {
        //                 DataType::Struct(_) => {
        //                     let lit_arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();
        //                     if is_multi {
        //                         GeometryArray::MultiLineString(lit_arr.clone().try_into().unwrap())
        //                     } else {
        //                         GeometryArray::Polygon(lit_arr.clone().try_into().unwrap())
        //                     }
        //                 }
        //                 DataType::List(_) | DataType::LargeList(_) => {
        //                     let lit_arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();
        //                     GeometryArray::MultiPolygon(lit_arr.clone().try_into().unwrap())
        //                 }
        //                 _ => panic!("Unexpected inner list type: {:?}", dt2),
        //             },
        //             _ => panic!("Unexpected inner list type: {:?}", dt),
        //         },
        //         dt => panic!("Unexpected geoarrow type: {:?}", dt),
        //     }
    }
}
