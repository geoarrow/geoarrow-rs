use crate::array::{CoordBuffer, CoordType, MultiPointArray, WKBArray};
use crate::error::{GeoArrowError, Result};
use crate::util::slice_validity_unchecked;
use crate::GeometryArrayTrait;
use arrow2::array::{Array, ListArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use arrow2::datatypes::{DataType, Field};
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;
use rstar::primitives::CachedEnvelope;
use rstar::RTree;

use super::MutableLineStringArray;

/// An immutable array of LineString geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<LineString>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone, PartialEq)]
pub struct LineStringArray<O: Offset> {
    pub coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub geom_offsets: OffsetsBuffer<O>,

    /// Validity bitmap
    pub validity: Option<Bitmap>,
}

pub(super) fn check<O: Offset>(
    coords: &CoordBuffer,
    validity_len: Option<usize>,
    geom_offsets: &OffsetsBuffer<O>,
) -> Result<()> {
    if validity_len.map_or(false, |len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if geom_offsets.last().to_usize() != coords.len() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match coords length".to_string(),
        ));
    }

    Ok(())
}

impl<O: Offset> LineStringArray<O> {
    /// Create a new LineStringArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest geometry offset does not match the number of coordinates
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

    /// Create a new LineStringArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity buffer does not have the same length as the number of geometries
    /// - if the geometry offsets do not match the number of coordinates
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetsBuffer<O>,
        validity: Option<Bitmap>,
    ) -> Result<Self> {
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
        let inner_field = Field::new("vertices", self.vertices_type(), true);
        match O::IS_LARGE {
            true => DataType::LargeList(Box::new(inner_field)),
            false => DataType::List(Box::new(inner_field)),
        }
    }
}

impl<'a, O: Offset> GeometryArrayTrait<'a> for LineStringArray<O> {
    type Scalar = crate::scalar::LineString<'a, O>;
    type ScalarGeo = geo::LineString;
    type ArrowArray = ListArray<O>;
    type RTreeObject = CachedEnvelope<Self::Scalar>;

    /// Gets the value at slot `i`
    fn value(&'a self, i: usize) -> Self::Scalar {
        crate::scalar::LineString {
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
            "geoarrow.linestring".to_string(),
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

    /// Build a spatial index containing this array's geometries
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

    /// Slices this [`LineStringArray`] in place.
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

    /// Slices this [`LineStringArray`] in place.
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

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

// Implement geometry accessors
impl<O: Offset> LineStringArray<O> {
    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::LineString> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::LineString, impl Iterator<Item = geo::LineString> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.validity())
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

    /// Iterator over GEOS geometry objects, taking validity into account
    #[cfg(feature = "geos")]
    pub fn iter_geos(
        &self,
    ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.iter_geos_values(), self.validity())
    }
}

impl<O: Offset> TryFrom<&ListArray<O>> for LineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: &ListArray<O>) -> Result<Self> {
        let coords: CoordBuffer = value.values().as_ref().try_into()?;
        let geom_offsets = value.offsets();
        let validity = value.validity();

        Ok(Self::new(coords, geom_offsets.clone(), validity.cloned()))
    }
}

impl TryFrom<&dyn Array> for LineStringArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type().to_logical_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray<i32>>().unwrap();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray<i64>>().unwrap();
                let geom_array: LineStringArray<i64> = downcasted.try_into()?;
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<&dyn Array> for LineStringArray<i64> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type().to_logical_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray<i32>>().unwrap();
                let geom_array: LineStringArray<i32> = downcasted.try_into()?;
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

impl<O: Offset> From<Vec<Option<geo::LineString>>> for LineStringArray<O> {
    fn from(other: Vec<Option<geo::LineString>>) -> Self {
        let mut_arr: MutableLineStringArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: Offset> From<Vec<geo::LineString>> for LineStringArray<O> {
    fn from(other: Vec<geo::LineString>) -> Self {
        let mut_arr: MutableLineStringArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, Option<geo::LineString>>>
    for LineStringArray<O>
{
    fn from(other: bumpalo::collections::Vec<'_, Option<geo::LineString>>) -> Self {
        let mut_arr: MutableLineStringArray<O> = other.into();
        mut_arr.into()
    }
}

impl<O: Offset> From<bumpalo::collections::Vec<'_, geo::LineString>> for LineStringArray<O> {
    fn from(other: bumpalo::collections::Vec<'_, geo::LineString>) -> Self {
        let mut_arr: MutableLineStringArray<O> = other.into();
        mut_arr.into()
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl<O: Offset> From<LineStringArray<O>> for MultiPointArray<O> {
    fn from(value: LineStringArray<O>) -> Self {
        Self::new(value.coords, value.geom_offsets, value.validity)
    }
}

impl<O: Offset> TryFrom<WKBArray<O>> for LineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let mut_arr: MutableLineStringArray<O> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

impl From<LineStringArray<i32>> for LineStringArray<i64> {
    fn from(value: LineStringArray<i32>) -> Self {
        Self::new(value.coords, (&value.geom_offsets).into(), value.validity)
    }
}

impl TryFrom<LineStringArray<i64>> for LineStringArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: LineStringArray<i64>) -> Result<Self> {
        Ok(Self::new(
            value.coords,
            (&value.geom_offsets).try_into()?,
            value.validity,
        ))
    }
}

/// Default to an empty array
impl<O: Offset> Default for LineStringArray<O> {
    fn default() -> Self {
        MutableLineStringArray::default().into()
    }
}

#[cfg(test)]
mod test {
    use crate::test::geoarrow_data::{
        example_linestring_interleaved, example_linestring_separated, example_linestring_wkb,
    };
    use crate::test::linestring::{ls0, ls1};

    use super::*;

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: LineStringArray<i64> = vec![ls0(), ls1()].into();
        assert_eq!(arr.value_as_geo(0), ls0());
        assert_eq!(arr.value_as_geo(1), ls1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: LineStringArray<i64> = vec![Some(ls0()), Some(ls1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(ls0()));
        assert_eq!(arr.get_as_geo(1), Some(ls1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    // #[test]
    // fn rstar_integration() {
    //     let arr: LineStringArray = vec![ls0(), ls1()].into();
    //     let tree = arr.rstar_tree();

    //     let search_box = AABB::from_corners([3.5, 5.5], [4.5, 6.5]);
    //     let results: Vec<&crate::scalar::LineString> =
    //         tree.locate_in_envelope_intersecting(&search_box).collect();

    //     assert_eq!(results.len(), 1);
    //     assert_eq!(
    //         results[0].geom_index, 1,
    //         "The second element in the LineStringArray should be found"
    //     );
    // }

    #[test]
    fn slice() {
        let mut arr: LineStringArray<i64> = vec![ls0(), ls1()].into();
        arr.slice(1, 1);
        assert_eq!(arr.len(), 1);
        assert_eq!(arr.get_as_geo(0), Some(ls1()));
    }

    #[test]
    fn parse_wkb_geoarrow_interleaved_example() {
        let linestring_arr = example_linestring_interleaved();

        let wkb_arr = example_linestring_wkb();
        let parsed_linestring_arr: LineStringArray<i64> = wkb_arr.try_into().unwrap();

        assert_eq!(linestring_arr, parsed_linestring_arr);
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        let linestring_arr = example_linestring_separated().into_coord_type(CoordType::Interleaved);

        let wkb_arr = example_linestring_wkb();
        let parsed_linestring_arr: LineStringArray<i64> = wkb_arr.try_into().unwrap();

        assert_eq!(linestring_arr, parsed_linestring_arr);
    }
}
