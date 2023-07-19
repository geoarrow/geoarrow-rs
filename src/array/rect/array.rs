use arrow2::array::{Array, FixedSizeListArray, PrimitiveArray};
use arrow2::bitmap::Bitmap;
use arrow2::buffer::Buffer;
use arrow2::datatypes::{DataType, Field};
use rstar::primitives::CachedEnvelope;
use rstar::RTree;

use crate::array::{CoordBuffer, CoordType};
use crate::util::slice_validity_unchecked;
use crate::GeometryArrayTrait;

/// Internally this is implemented as a FixedSizeList[4], laid out as minx, miny, maxx, maxy.
#[derive(Debug, Clone, PartialEq)]
pub struct RectArray {
    /// A Buffer of float values for the bounding rectangles
    /// Invariant: the length of values must always be a multiple of 4
    values: Buffer<f64>,
    validity: Option<Bitmap>,
}

impl RectArray {
    pub fn new(values: Buffer<f64>, validity: Option<Bitmap>) -> Self {
        Self { values, validity }
    }

    fn inner_type(&self) -> DataType {
        DataType::Float64
    }

    fn outer_type(&self) -> DataType {
        let inner_field = Field::new("rect", self.inner_type(), false);
        DataType::FixedSizeList(Box::new(inner_field), 4)
    }
}

impl<'a> GeometryArrayTrait<'a> for RectArray {
    type Scalar = crate::scalar::Rect<'a>;
    type ScalarGeo = geo::Rect;
    type ArrowArray = FixedSizeListArray;
    type RTreeObject = CachedEnvelope<Self::Scalar>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        crate::scalar::Rect {
            values: &self.values,
            geom_index: i,
        }
    }

    fn logical_type(&self) -> DataType {
        self.outer_type()
    }

    fn extension_type(&self) -> DataType {
        DataType::Extension(
            "geoarrow._rect".to_string(),
            Box::new(self.logical_type()),
            None,
        )
    }

    fn into_arrow(self) -> Self::ArrowArray {
        let extension_type = self.extension_type();
        let validity = self.validity;

        let values = PrimitiveArray::new(DataType::Float64, self.values, None);
        FixedSizeListArray::new(extension_type, values.boxed(), validity)
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow().boxed()
    }

    fn with_coords(self, _coords: CoordBuffer) -> Self {
        unimplemented!()
    }

    fn coord_type(&self) -> CoordType {
        unimplemented!()
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        unimplemented!()
    }

    /// Build a spatial index containing this array's geometries
    fn rstar_tree(&'a self) -> RTree<Self::RTreeObject> {
        RTree::bulk_load(self.iter().flatten().map(CachedEnvelope::new).collect())
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.values.len() / 4
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Slices this [`PolygonArray`] in place.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Examples
    /// ```
    /// use arrow2::array::PrimitiveArray;
    ///
    /// let array = PrimitiveArray::from_vec(vec![1, 2, 3]);
    /// assert_eq!(format!("{:?}", array), "Int32[1, 2, 3]");
    /// let sliced = array.slice(1, 1);
    /// assert_eq!(format!("{:?}", sliced), "Int32[2]");
    /// // note: `sliced` and `array` share the same memory region.
    /// ```
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Slices this [`PolygonArray`] in place.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        slice_validity_unchecked(&mut self.validity, offset, length);
        self.values.slice_unchecked(offset * 4, length * 4);
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}
