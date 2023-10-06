use crate::array::{CoordType, MutableWKBArray};
use crate::error::GeoArrowError;
use crate::scalar::WKB;
// use crate::util::{owned_slice_offsets, owned_slice_validity};
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;
use arrow_array::{Array, BinaryArray, GenericBinaryArray, LargeBinaryArray};
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;

/// An immutable array of WKB geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<WKB>>` due to the internal validity bitmap.
///
/// This array _can_ be used directly for operations, but that will incur costly encoding to and
/// from WKB on every operation. Instead, you usually want to use the WKBArray only for
/// serialization purposes (e.g. to and from [GeoParquet](https://geoparquet.org/)) but convert to
/// strongly-typed arrays (such as the [`PointArray`][crate::array::PointArray]) for computations.
#[derive(Debug, Clone, PartialEq)]
pub struct WKBArray<O: OffsetSizeTrait>(GenericBinaryArray<O>);

// Implement geometry accessors
impl<O: OffsetSizeTrait> WKBArray<O> {
    /// Create a new WKBArray from a BinaryArray
    pub fn new(arr: GenericBinaryArray<O>) -> Self {
        Self(arr)
    }

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn with_validity(&self, validity: Option<NullBuffer>) -> Self {
        WKBArray::new(self.0.clone().with_validity(validity))
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayTrait<'a> for WKBArray<O> {
    type Scalar = WKB<'a, O>;
    type ScalarGeo = geo::Geometry;
    type ArrowArray = GenericBinaryArray<O>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        WKB::new_borrowed(&self.0, i)
    }

    fn logical_type(&self) -> DataType {
        self.0.data_type().clone()
    }

    fn extension_type(&self) -> DataType {
        DataType::Extension(
            "geoarrow.wkb".to_string(),
            Box::new(self.logical_type()),
            None,
        )
    }

    fn into_arrow(self) -> GenericBinaryArray<O> {
        // Recreate a BinaryArray so that we can force it to have geoarrow.wkb extension type
        GenericBinaryArray::new(
            self.0.offsets().clone(),
            self.0.values().clone(),
            self.0.nulls().cloned(),
        )
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow().boxed()
    }

    fn with_coords(self, _coords: crate::array::CoordBuffer) -> Self {
        unimplemented!()
    }

    fn coord_type(&self) -> CoordType {
        CoordType::Interleaved
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        self
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the optional validity.
    fn validity(&self) -> Option<&NullBuffer> {
        self.0.nulls()
    }

    /// Slices this [`WKBArray`] in place.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Examples
    /// ```
    /// use arrow2::array::PrimitiveArray;
    ///
    /// let array = PrimitiveArray::from_vec(vec![1, 2, 3]);
    /// assert_eq!(format!("{:?}", array), "Int32[1, 2, 3]");
    /// array.slice(1, 1);
    /// assert_eq!(format!("{:?}", array), "Int32[2]");
    /// ```
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        self.0.slice(offset, length);
    }
    /// Slices this [`WKBArray`] in place.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        self.0.slice_unchecked(offset, length)
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        todo!()
        // assert!(
        //     offset + length <= self.len(),
        //     "offset + length may not exceed length of array"
        // );
        // assert!(length >= 1, "length must be at least 1");

        // // Find the start and end of the ring offsets
        // let (start_idx, _) = self.0.offsets().start_end(offset);
        // let (_, end_idx) = self.0.offsets().start_end(offset + length - 1);

        // let new_offsets = owned_slice_offsets(self.0.offsets(), offset, length);

        // let mut values = self.0.values().clone();
        // values.slice(start_idx, end_idx - start_idx);

        // let validity = owned_slice_validity(self.0.nulls(), offset, length);

        // Self::new(GenericBinaryArray::new(
        //     new_offsets,
        //     values.as_slice().to_vec().into(),
        //     validity,
        // ))
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> WKBArray<O> {
    /// Returns the value at slot `i` as a GEOS geometry.
    #[cfg(feature = "geos")]
    pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
        let buf = self.0.value(i);
        geos::Geometry::new_from_wkb(buf).expect("Unable to parse WKB")
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        if self.is_null(i) {
            return None;
        }

        let buf = self.0.value(i);
        Some(geos::Geometry::new_from_wkb(buf).expect("Unable to parse WKB"))
    }

    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::Geometry, impl Iterator<Item = geo::Geometry> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.nulls())
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

impl<O: OffsetSizeTrait> From<GenericBinaryArray<O>> for WKBArray<O> {
    fn from(value: GenericBinaryArray<O>) -> Self {
        Self::new(value)
    }
}

impl TryFrom<&dyn Array> for WKBArray<i32> {
    type Error = GeoArrowError;
    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::Binary => {
                let downcasted = value.as_any().downcast_ref::<BinaryArray>().unwrap();
                Ok(downcasted.clone().into())
            }
            DataType::LargeBinary => {
                let downcasted = value.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                let geom_array: WKBArray<i64> = downcasted.clone().into();
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<&dyn Array> for WKBArray<i64> {
    type Error = GeoArrowError;
    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::Binary => {
                let downcasted = value.as_any().downcast_ref::<BinaryArray>().unwrap();
                let geom_array: WKBArray<i32> = downcasted.clone().into();
                Ok(geom_array.into())
            }
            DataType::LargeBinary => {
                let downcasted = value.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                Ok(downcasted.clone().into())
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl From<WKBArray<i32>> for WKBArray<i64> {
    fn from(value: WKBArray<i32>) -> Self {
        let binary_array = value.0;
        let (_data_type, offsets, values, validity) = binary_array.into_inner();
        Self::new(BinaryArray::new((&offsets).into(), values, validity))
    }
}

impl TryFrom<WKBArray<i64>> for WKBArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<i64>) -> Result<Self, Self::Error> {
        let binary_array = value.0;
        let (_data_type, offsets, values, validity) = binary_array.into_inner();
        Ok(Self::new(LargeBinaryArray::new(
            (&offsets).try_into()?,
            values,
            validity,
        )))
    }
}

// impl TryFrom<&BinaryArray<i64>> for WKBArray {
//     type Error = GeoArrowError;

//     fn try_from(value: &BinaryArray<i64>) -> Result<Self, Self::Error> {
//         Ok(Self::new(value.clone()))
//     }
// }

// impl TryFrom<&dyn Array> for WKBArray {
//     type Error = GeoArrowError;

//     fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
//         match value.data_type() {
//             DataType::Binary => {
//                 let downcasted = value.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
//                 downcasted.try_into()
//             }
//             DataType::LargeBinary => {
//                 let downcasted = value.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
//                 downcasted.try_into()
//             }
//             _ => Err(GeoArrowError::General(format!(
//                 "Unexpected type: {:?}",
//                 value.data_type()
//             ))),
//         }
//     }
// }

impl<O: OffsetSizeTrait> From<Vec<Option<geo::Geometry>>> for WKBArray<O> {
    fn from(other: Vec<Option<geo::Geometry>>) -> Self {
        let mut_arr: MutableWKBArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, Option<geo::Geometry>>>
    for WKBArray<O>
{
    fn from(other: bumpalo::collections::Vec<'_, Option<geo::Geometry>>) -> Self {
        let mut_arr: MutableWKBArray<O> = other.into();
        mut_arr.into()
    }
}
