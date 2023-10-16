use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};

use crate::array::geometrycollection::GeometryCollectionArrayIter;
use crate::array::{CoordBuffer, CoordType, MixedGeometryArray};
use crate::scalar::GeometryCollection;
use crate::util::slice_validity_unchecked;
use crate::GeometryArrayTrait;

/// An immutable array of GeometryCollection geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<GeometryCollection>>` due to the internal
/// validity bitmap.
#[derive(Debug, Clone)]
pub struct GeometryCollectionArray<O: OffsetSizeTrait> {
    pub array: MixedGeometryArray<O>,

    /// Offsets into the mixed geometry array where each geometry starts
    pub geom_offsets: OffsetBuffer<O>,

    /// Validity bitmap
    pub validity: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> GeometryCollectionArray<O> {
    /// Create a new GeometryCollectionArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    pub fn new(
        array: MixedGeometryArray<O>,
        geom_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
    ) -> Self {
        Self {
            array,
            geom_offsets,
            validity,
        }
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayTrait<'a> for GeometryCollectionArray<O> {
    type Scalar = GeometryCollection<'a, O>;
    type ScalarGeo = geo::GeometryCollection;
    type ArrowArray = GenericListArray<O>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        GeometryCollection {
            array: &self.array,
            geom_offsets: &self.geom_offsets,
            geom_index: i,
        }
    }

    fn storage_type(&self) -> DataType {
        todo!()
    }

    fn extension_field(&self) -> Arc<Field> {
        let mut metadata = HashMap::new();
        metadata.insert(
            "ARROW:extension:name".to_string(),
            "geoarrow.geometrycollection".to_string(),
        );
        Arc::new(Field::new("geometry", self.storage_type(), true).with_metadata(metadata))
    }

    fn into_arrow(self) -> Self::ArrowArray {
        let extension_type = self.extension_field();
        let validity = self.validity;
        let values = self.array.into_boxed_arrow();
        GenericListArray::new(extension_type, self.geom_offsets, values, validity)
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow().boxed()
    }

    fn with_coords(self, _coords: CoordBuffer) -> Self {
        todo!()
    }

    fn coord_type(&self) -> CoordType {
        todo!()
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        todo!()
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }

    /// Slices this [`GeometryCollectionArray`] in place.
    ///
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

    /// Slices this [`GeometryCollectionArray`] in place.
    ///
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        slice_validity_unchecked(&mut self.validity, offset, length);
        self.geom_offsets.slice_unchecked(offset, length + 1);
    }

    fn owned_slice(&self, _offset: usize, _length: usize) -> Self {
        todo!()
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

// Implement geometry accessors
impl<O: OffsetSizeTrait> GeometryCollectionArray<O> {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::GeometryCollection> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(&self) -> GeometryCollectionArrayIter<'_, O> {
        GeometryCollectionArrayIter::new(self)
    }

    /// Returns the value at slot `i` as a GEOS geometry.
    #[cfg(feature = "geos")]
    pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
        self.value(i).try_into().unwrap()
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects
    #[cfg(feature = "geos")]
    pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geos(i))
    }

    // /// Iterator over GEOS geometry objects, taking validity into account
    // #[cfg(feature = "geos")]
    // pub fn iter_geos(
    //     &self,
    // ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitmapIter> {
    //     ZipValidity::new_with_validity(self.iter_geos_values(), self.nulls())
    // }
}
