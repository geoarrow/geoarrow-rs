use arrow_array::{Array, FixedSizeListArray, Float64Array};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};

use crate::array::{CoordBuffer, CoordType};
use crate::scalar::Rect;
use crate::util::{owned_slice_validity, slice_validity_unchecked};
use crate::GeometryArrayTrait;

/// Internally this is implemented as a FixedSizeList[4], laid out as minx, miny, maxx, maxy.
#[derive(Debug, Clone, PartialEq)]
pub struct RectArray {
    /// A Buffer of float values for the bounding rectangles
    /// Invariant: the length of values must always be a multiple of 4
    values: ScalarBuffer<f64>,
    validity: Option<NullBuffer>,
}

impl RectArray {
    pub fn new(values: ScalarBuffer<f64>, validity: Option<NullBuffer>) -> Self {
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
    type Scalar = Rect<'a>;
    type ScalarGeo = geo::Rect;
    type ArrowArray = FixedSizeListArray;

    fn value(&'a self, i: usize) -> Self::Scalar {
        Rect::new_borrowed(&self.values, i)
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

        let values = Float64Array::new(self.values, None);
        FixedSizeListArray::new(extension_type, 2, values.boxed(), validity)
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

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.values.len() / 4
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&NullBuffer> {
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

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        let mut values = self.values.clone();
        values.slice(offset * 4, length * 4);

        let validity = owned_slice_validity(self.nulls(), offset, length);

        Self::new(values.as_slice().to_vec().into(), validity)
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}
