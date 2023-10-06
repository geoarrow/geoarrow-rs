use std::sync::Arc;

use crate::array::multilinestring::MultiLineStringArrayIter;
use crate::array::{CoordBuffer, CoordType, LineStringArray, PolygonArray, WKBArray};
use crate::error::GeoArrowError;
use crate::scalar::MultiLineString;
use crate::util::{owned_slice_offsets, owned_slice_validity, slice_validity_unchecked};
use crate::GeometryArrayTrait;
use arrow_array::{Array, GenericListArray, LargeListArray, ListArray, OffsetSizeTrait};
use arrow_buffer::{BufferBuilder, NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};

use super::MutableMultiLineStringArray;

/// An immutable array of MultiLineString geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<MultiLineString>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
// #[derive(Debug, Clone, PartialEq)]
pub struct MultiLineStringArray<O: OffsetSizeTrait> {
    pub coords: CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub geom_offsets: OffsetBuffer<O>,

    /// Offsets into the coordinate array where each ring starts
    pub ring_offsets: OffsetBuffer<O>,

    /// Validity bitmap
    pub validity: Option<NullBuffer>,
}

pub(super) fn check<O: OffsetSizeTrait>(
    coords: &CoordBuffer,
    geom_offsets: &OffsetBuffer<O>,
    ring_offsets: &OffsetBuffer<O>,
    validity_len: Option<usize>,
) -> Result<(), GeoArrowError> {
    if validity_len.map_or(false, |len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if ring_offsets.last().to_usize() != coords.len() {
        return Err(GeoArrowError::General(
            "largest ring offset must match coords length".to_string(),
        ));
    }

    if geom_offsets.last().to_usize() != ring_offsets.len_proxy() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match ring offsets length".to_string(),
        ));
    }

    Ok(())
}

impl<O: OffsetSizeTrait> MultiLineStringArray<O> {
    /// Create a new MultiLineStringArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest geometry offset does not match the size of ring offsets
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
    ) -> Self {
        check(
            &coords,
            &geom_offsets,
            &ring_offsets,
            validity.as_ref().map(|v| v.len()),
        )
        .unwrap();
        Self {
            coords,
            geom_offsets,
            ring_offsets,
            validity,
        }
    }

    /// Create a new MultiLineStringArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest geometry offset does not match the size of ring offsets
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
    ) -> Result<Self, GeoArrowError> {
        check(
            &coords,
            &geom_offsets,
            &ring_offsets,
            validity.as_ref().map(|v| v.len()),
        )?;
        Ok(Self {
            coords,
            geom_offsets,
            ring_offsets,
            validity,
        })
    }

    fn vertices_type(&self) -> DataType {
        self.coords.logical_type()
    }

    fn linestrings_type(&self) -> DataType {
        let vertices_field = Field::new("vertices", self.vertices_type(), false);
        match O::IS_LARGE {
            true => DataType::LargeList(Arc::new(vertices_field)),
            false => DataType::List(Arc::new(vertices_field)),
        }
    }

    fn outer_type(&self) -> DataType {
        let linestrings_field = Field::new("linestrings", self.linestrings_type(), true);
        match O::IS_LARGE {
            true => DataType::LargeList(Arc::new(linestrings_field)),
            false => DataType::List(Arc::new(linestrings_field)),
        }
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayTrait<'a> for MultiLineStringArray<O> {
    type Scalar = MultiLineString<'a, O>;
    type ScalarGeo = geo::MultiLineString;
    type ArrowArray = GenericListArray<O>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        MultiLineString::new_borrowed(&self.coords, &self.geom_offsets, &self.ring_offsets, i)
    }

    fn logical_type(&self) -> DataType {
        self.outer_type()
    }

    fn extension_type(&self) -> DataType {
        DataType::Extension(
            "geoarrow.multilinestring".to_string(),
            Box::new(self.logical_type()),
            None,
        )
    }

    fn into_arrow(self) -> Self::ArrowArray {
        let linestrings_type = self.linestrings_type();
        let extension_type = self.extension_type();
        let validity = self.validity;
        let coord_array = self.coords.into_arrow();
        let ring_array =
            GenericListArray::new(linestrings_type, self.ring_offsets, coord_array, None).boxed();
        GenericListArray::new(extension_type, self.geom_offsets, ring_array, validity)
    }

    fn into_boxed_arrow(self) -> Box<dyn Array> {
        self.into_arrow().boxed()
    }

    fn with_coords(self, coords: CoordBuffer) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(coords, self.geom_offsets, self.ring_offsets, self.validity)
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.geom_offsets,
            self.ring_offsets,
            self.validity,
        )
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

    /// Slices this [`MultiLineStringArray`] in place.
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

    /// Slices this [`MultiLineStringArray`] in place.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        slice_validity_unchecked(&mut self.validity, offset, length);
        self.geom_offsets.slice_unchecked(offset, length + 1);
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        assert!(length >= 1, "length must be at least 1");

        // Find the start and end of the ring offsets
        let (start_ring_idx, _) = self.geom_offsets.start_end(offset);
        let (_, end_ring_idx) = self.geom_offsets.start_end(offset + length - 1);

        // Find the start and end of the coord buffer
        let (start_coord_idx, _) = self.ring_offsets.start_end(start_ring_idx);
        let (_, end_coord_idx) = self.ring_offsets.start_end(end_ring_idx - 1);

        // Slice the geom_offsets
        let geom_offsets = owned_slice_offsets(&self.geom_offsets, offset, length);
        let ring_offsets = owned_slice_offsets(
            &self.ring_offsets,
            start_ring_idx,
            end_ring_idx - start_ring_idx,
        );
        let coords = self
            .coords
            .owned_slice(start_coord_idx, end_coord_idx - start_coord_idx);

        let validity = owned_slice_validity(self.nulls(), offset, length);

        Self::new(coords, geom_offsets, ring_offsets, validity)
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

// Implement geometry accessors
impl<O: OffsetSizeTrait> MultiLineStringArray<O> {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::MultiLineString> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(&self) -> MultiLineStringArrayIter<'_, O> {
        MultiLineStringArrayIter::new(self)
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

impl<O: OffsetSizeTrait> TryFrom<&GenericListArray<O>> for MultiLineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(geom_array: &GenericListArray<O>) -> Result<Self, Self::Error> {
        let geom_offsets = geom_array.offsets();
        let validity = geom_array.nulls();

        let rings_dyn_array = geom_array.values();
        let rings_array = rings_dyn_array
            .as_any()
            .downcast_ref::<GenericListArray<O>>()
            .unwrap();

        let ring_offsets = rings_array.offsets();
        let coords: CoordBuffer = rings_array.values().as_ref().try_into()?;

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
        ))
    }
}

impl TryFrom<&dyn Array> for MultiLineStringArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<LargeListArray>().unwrap();
                let geom_array: MultiLineStringArray<i64> = downcasted.try_into()?;
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<&dyn Array> for MultiLineStringArray<i64> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                let geom_array: MultiLineStringArray<i32> = downcasted.try_into()?;
                Ok(geom_array.into())
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<LargeListArray>().unwrap();
                downcasted.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl<O: OffsetSizeTrait> From<Vec<Option<geo::MultiLineString>>> for MultiLineStringArray<O> {
    fn from(other: Vec<Option<geo::MultiLineString>>) -> Self {
        let mut_arr: MutableMultiLineStringArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> From<Vec<geo::MultiLineString>> for MultiLineStringArray<O> {
    fn from(other: Vec<geo::MultiLineString>) -> Self {
        let mut_arr: MutableMultiLineStringArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, Option<geo::MultiLineString>>>
    for MultiLineStringArray<O>
{
    fn from(other: bumpalo::collections::Vec<'_, Option<geo::MultiLineString>>) -> Self {
        let mut_arr: MutableMultiLineStringArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> From<bumpalo::collections::Vec<'_, geo::MultiLineString>>
    for MultiLineStringArray<O>
{
    fn from(other: bumpalo::collections::Vec<'_, geo::MultiLineString>) -> Self {
        let mut_arr: MutableMultiLineStringArray<O> = other.into();
        mut_arr.into()
    }
}
/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl<O: OffsetSizeTrait> From<MultiLineStringArray<O>> for PolygonArray<O> {
    fn from(value: MultiLineStringArray<O>) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
        )
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for MultiLineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self, Self::Error> {
        let mut_arr: MutableMultiLineStringArray<O> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

impl<O: OffsetSizeTrait> TryFrom<LineStringArray<O>> for MultiLineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: LineStringArray<O>) -> Result<Self, Self::Error> {
        let geom_length = value.len();

        let coords = value.coords;
        let ring_offsets = value.geom_offsets;
        let validity = value.validity;

        // Create offsets that are all of length 1
        let mut geom_offsets = BufferBuilder::new(geom_length);
        for _ in 0..coords.len() {
            geom_offsets.try_push_usize(1)?;
        }

        Ok(Self::new(
            coords,
            geom_offsets.into(),
            ring_offsets,
            validity,
        ))
    }
}

impl From<MultiLineStringArray<i32>> for MultiLineStringArray<i64> {
    fn from(value: MultiLineStringArray<i32>) -> Self {
        Self::new(
            value.coords,
            (&value.geom_offsets).into(),
            (&value.ring_offsets).into(),
            value.validity,
        )
    }
}

impl TryFrom<MultiLineStringArray<i64>> for MultiLineStringArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: MultiLineStringArray<i64>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value.coords,
            (&value.geom_offsets).try_into()?,
            (&value.ring_offsets).try_into()?,
            value.validity,
        ))
    }
}

/// Default to an empty array
impl<O: OffsetSizeTrait> Default for MultiLineStringArray<O> {
    fn default() -> Self {
        MutableMultiLineStringArray::default().into()
    }
}

#[cfg(test)]
mod test {
    use crate::test::geoarrow_data::{
        example_multilinestring_interleaved, example_multilinestring_separated,
        example_multilinestring_wkb,
    };
    use crate::test::multilinestring::{ml0, ml1};

    use super::*;

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: MultiLineStringArray<i64> = vec![ml0(), ml1()].into();
        assert_eq!(arr.value_as_geo(0), ml0());
        assert_eq!(arr.value_as_geo(1), ml1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: MultiLineStringArray<i64> = vec![Some(ml0()), Some(ml1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(ml0()));
        assert_eq!(arr.get_as_geo(1), Some(ml1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    #[test]
    fn slice() {
        let mut arr: MultiLineStringArray<i64> = vec![ml0(), ml1()].into();
        arr.slice(1, 1);
        assert_eq!(arr.len(), 1);
        assert_eq!(arr.get_as_geo(0), Some(ml1()));
    }

    #[test]
    fn owned_slice() {
        let arr: MultiLineStringArray<i64> = vec![ml0(), ml1()].into();
        let sliced = arr.owned_slice(1, 1);

        assert!(
            !sliced.geom_offsets.buffer().is_sliced(),
            "underlying offsets should not be sliced"
        );
        assert_eq!(arr.len(), 2);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(ml1()));

        // Offset is 0 because it's copied to an owned buffer
        assert_eq!(*sliced.geom_offsets.first(), 0);
        assert_eq!(*sliced.ring_offsets.first(), 0);
    }

    #[test]
    fn parse_wkb_geoarrow_interleaved_example() {
        let geom_arr = example_multilinestring_interleaved();

        let wkb_arr = example_multilinestring_wkb();
        let parsed_geom_arr: MultiLineStringArray<i64> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        // TODO: support checking equality of interleaved vs separated coords
        let geom_arr = example_multilinestring_separated().into_coord_type(CoordType::Interleaved);

        let wkb_arr = example_multilinestring_wkb();
        let parsed_geom_arr: MultiLineStringArray<i64> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }
}
