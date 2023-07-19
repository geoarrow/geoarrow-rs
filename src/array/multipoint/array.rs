use super::MutableMultiPointArray;
use crate::array::{CoordBuffer, CoordType, LineStringArray, PointArray, WKBArray};
use crate::error::GeoArrowError;
use crate::util::slice_validity_unchecked;
use crate::GeometryArrayTrait;
use arrow2::array::{Array, ListArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::{DataType, Field};
use arrow2::offset::{Offsets, OffsetsBuffer};
use arrow2::types::Offset;
use rstar::primitives::CachedEnvelope;
use rstar::RTree;

/// An immutable array of MultiPoint geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<MultiPoint>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiPointArray<O: Offset> {
    pub coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub geom_offsets: OffsetsBuffer<O>,

    /// Validity bitmap
    pub validity: Option<Bitmap>,
}

pub(super) fn check<O: Offset>(
    _coords: &CoordBuffer,
    validity_len: Option<usize>,
    geom_offsets: &OffsetsBuffer<O>,
) -> Result<(), GeoArrowError> {
    // TODO: check geom offsets?
    if validity_len.map_or(false, |len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }
    Ok(())
}

impl<O: Offset> MultiPointArray<O> {
    /// Create a new MultiPointArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetsBuffer<O>,
        validity: Option<Bitmap>,
    ) -> Self {
        check(&coords, validity.as_ref().map(|v| v.len()), &geom_offsets).unwrap();
        Self {
            coords,
            geom_offsets,
            validity,
        }
    }

    /// Create a new MultiPointArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetsBuffer<O>,
        validity: Option<Bitmap>,
    ) -> Result<Self, GeoArrowError> {
        check(&coords, validity.as_ref().map(|v| v.len()), &geom_offsets)?;
        Ok(Self {
            coords,
            geom_offsets,
            validity,
        })
    }

    fn vertices_type(&self) -> DataType {
        self.coords.logical_type()
    }

    fn outer_type(&self) -> DataType {
        let inner_field = Field::new("points", self.vertices_type(), true);
        DataType::LargeList(Box::new(inner_field))
    }
}

impl<'a, O: Offset> GeometryArrayTrait<'a> for MultiPointArray<O> {
    type Scalar = crate::scalar::MultiPoint<'a, O>;
    type ScalarGeo = geo::MultiPoint;
    type ArrowArray = ListArray<O>;
    type RTreeObject = CachedEnvelope<Self::Scalar>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        crate::scalar::MultiPoint {
            coords: &self.coords,
            geom_offsets: &self.geom_offsets,
            geom_index: i,
        }
    }

    fn logical_type(&self) -> DataType {
        self.outer_type()
    }

    fn extension_type(&self) -> DataType {
        DataType::Extension(
            "geoarrow.multipoint".to_string(),
            Box::new(self.logical_type()),
            None,
        )
    }

    fn into_arrow(self) -> Self::ArrowArray {
        let extension_type = self.extension_type();
        let validity = self.validity;
        let coord_array = self.coords.into_arrow();
        ListArray::new(extension_type, self.geom_offsets, coord_array, validity)
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow().boxed()
    }

    fn with_coords(self, coords: CoordBuffer) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(coords, self.geom_offsets, self.validity)
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.geom_offsets,
            self.validity,
        )
    }

    fn rstar_tree(&'a self) -> RTree<Self::RTreeObject> {
        RTree::bulk_load(self.iter().flatten().map(CachedEnvelope::new).collect())
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    /// Returns the optional validity.
    #[inline]
    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Slices this [`MultiPointArray`] in place.
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

    /// Slices this [`MultiPointArray`] in place.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        slice_validity_unchecked(&mut self.validity, offset, length);
        self.geom_offsets.slice_unchecked(offset, length + 1);
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

// Implement geometry accessors
impl<O: Offset> MultiPointArray<O> {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::MultiPoint> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::MultiPoint, impl Iterator<Item = geo::MultiPoint> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.validity())
    }

    // GEOS from not implemented for MultiPoint?!?
    //
    // /// Returns the value at slot `i` as a GEOS geometry.
    // #[cfg(feature = "geos")]
    // pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
    //     (&self.value_as_geo(i)).try_into().unwrap()
    // }

    // /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    // #[cfg(feature = "geos")]
    // pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
    //     if self.is_null(i) {
    //         return None;
    //     }

    //     self.get_as_geo(i).as_ref().map(|g| g.try_into().unwrap())
    // }

    // /// Iterator over GEOS geometry objects
    // #[cfg(feature = "geos")]
    // pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
    //     (0..self.len()).map(|i| self.value_as_geos(i))
    // }

    // /// Iterator over GEOS geometry objects, taking validity into account
    // #[cfg(feature = "geos")]
    // pub fn iter_geos(
    //     &self,
    // ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitmapIter> {
    //     ZipValidity::new_with_validity(self.iter_geos_values(), self.validity())
    // }
}

impl<O: Offset> TryFrom<&ListArray<O>> for MultiPointArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: &ListArray<O>) -> Result<Self, Self::Error> {
        let coords: CoordBuffer = value.values().as_ref().try_into()?;
        let geom_offsets = value.offsets();
        let validity = value.validity();

        Ok(Self::new(coords, geom_offsets.clone(), validity.cloned()))
    }
}

impl TryFrom<&dyn Array> for MultiPointArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type().to_logical_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray<i32>>().unwrap();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray<i64>>().unwrap();
                let geom_array: MultiPointArray<i64> = downcasted.try_into()?;
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<&dyn Array> for MultiPointArray<i64> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type().to_logical_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray<i32>>().unwrap();
                let geom_array: MultiPointArray<i32> = downcasted.try_into()?;
                Ok(geom_array.into())
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray<i64>>().unwrap();
                downcasted.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl<O: Offset> From<Vec<Option<geo::MultiPoint>>> for MultiPointArray<O> {
    fn from(other: Vec<Option<geo::MultiPoint>>) -> Self {
        let mut_arr: MutableMultiPointArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: Offset> From<Vec<geo::MultiPoint>> for MultiPointArray<O> {
    fn from(other: Vec<geo::MultiPoint>) -> Self {
        let mut_arr: MutableMultiPointArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, Option<geo::MultiPoint>>>
    for MultiPointArray<O>
{
    fn from(other: bumpalo::collections::Vec<'_, Option<geo::MultiPoint>>) -> Self {
        let mut_arr: MutableMultiPointArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, geo::MultiPoint>> for MultiPointArray<O> {
    fn from(other: bumpalo::collections::Vec<'_, geo::MultiPoint>) -> Self {
        let mut_arr: MutableMultiPointArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: Offset> TryFrom<WKBArray<O>> for MultiPointArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let mut_arr: MutableMultiPointArray<O> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl<O: Offset> From<MultiPointArray<O>> for LineStringArray<O> {
    fn from(value: MultiPointArray<O>) -> Self {
        Self::new(value.coords, value.geom_offsets, value.validity)
    }
}

impl<O: Offset> TryFrom<PointArray> for MultiPointArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: PointArray) -> Result<Self, Self::Error> {
        let geom_length = value.len();

        let coords = value.coords;
        let validity = value.validity;

        // Create offsets that are all of length 1
        let mut geom_offsets = Offsets::with_capacity(geom_length);
        for _ in 0..coords.len() {
            geom_offsets.try_push_usize(1)?;
        }

        Ok(Self::new(coords, geom_offsets.into(), validity))
    }
}

impl From<MultiPointArray<i32>> for MultiPointArray<i64> {
    fn from(value: MultiPointArray<i32>) -> Self {
        Self::new(value.coords, (&value.geom_offsets).into(), value.validity)
    }
}

impl TryFrom<MultiPointArray<i64>> for MultiPointArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: MultiPointArray<i64>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value.coords,
            (&value.geom_offsets).try_into()?,
            value.validity,
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::geoarrow_data::{
        example_multipoint_interleaved, example_multipoint_separated, example_multipoint_wkb,
    };
    use crate::test::multipoint::{mp0, mp1};

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: MultiPointArray<i64> = vec![mp0(), mp1()].into();
        assert_eq!(arr.value_as_geo(0), mp0());
        assert_eq!(arr.value_as_geo(1), mp1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: MultiPointArray<i64> = vec![Some(mp0()), Some(mp1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(mp0()));
        assert_eq!(arr.get_as_geo(1), Some(mp1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    #[test]
    fn slice() {
        let mut arr: MultiPointArray<i64> = vec![mp0(), mp1()].into();
        arr.slice(1, 1);
        assert_eq!(arr.len(), 1);
        assert_eq!(arr.get_as_geo(0), Some(mp1()));
    }

    #[test]
    fn parse_wkb_geoarrow_interleaved_example() {
        let geom_arr = example_multipoint_interleaved();

        let wkb_arr = example_multipoint_wkb();
        let parsed_geom_arr: MultiPointArray<i64> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        // TODO: support checking equality of interleaved vs separated coords
        let geom_arr = example_multipoint_separated().into_coord_type(CoordType::Interleaved);

        let wkb_arr = example_multipoint_wkb();
        let parsed_geom_arr: MultiPointArray<i64> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }
}
