use std::collections::HashMap;
use std::sync::Arc;

use crate::algorithm::native::eq::offset_buffer_eq;
use crate::array::linestring::LineStringCapacity;
use crate::array::metadata::ArrayMetadata;
use crate::array::util::{offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32, OffsetBufferUtils};
use crate::array::{CoordBuffer, CoordType, MultiPointArray, WKBArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::scalar::LineString;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::util::{owned_slice_offsets, owned_slice_validity};
use crate::GeometryArrayTrait;
use arrow_array::{Array, ArrayRef, GenericListArray, LargeListArray, ListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef};

use super::LineStringBuilder;

/// An immutable array of LineString geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<LineString>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
pub struct LineStringArray<O: OffsetSizeTrait> {
    // Always GeoDataType::LineString or GeoDataType::LargeLineString
    data_type: GeoDataType,

    pub(crate) metadata: Arc<ArrayMetadata>,

    pub(crate) coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<O>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

pub(super) fn check<O: OffsetSizeTrait>(
    coords: &CoordBuffer,
    validity_len: Option<usize>,
    geom_offsets: &OffsetBuffer<O>,
) -> Result<()> {
    if validity_len.map_or(false, |len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if geom_offsets.last().to_usize().unwrap() != coords.len() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match coords length".to_string(),
        ));
    }

    Ok(())
}

impl<O: OffsetSizeTrait> LineStringArray<O> {
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
        geom_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        Self::try_new(coords, geom_offsets, validity, metadata).unwrap()
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
        geom_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self> {
        check(&coords, validity.as_ref().map(|v| v.len()), &geom_offsets)?;

        let coord_type = coords.coord_type();
        let data_type = match O::IS_LARGE {
            true => GeoDataType::LargeLineString(coord_type),
            false => GeoDataType::LineString(coord_type),
        };

        Ok(Self {
            data_type,
            coords,
            geom_offsets,
            validity,
            metadata,
        })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("vertices", self.coords.storage_type(), false).into()
    }

    fn outer_type(&self) -> DataType {
        match O::IS_LARGE {
            true => DataType::LargeList(self.vertices_field()),
            false => DataType::List(self.vertices_field()),
        }
    }

    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    pub fn geom_offsets(&self) -> &OffsetBuffer<O> {
        &self.geom_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> LineStringCapacity {
        LineStringCapacity::new(self.geom_offsets.last().to_usize().unwrap(), self.len())
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.validity().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes::<O>()
    }
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for LineStringArray<O> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        &self.data_type
    }

    fn storage_type(&self) -> DataType {
        self.outer_type()
    }

    fn extension_field(&self) -> FieldRef {
        let mut metadata = HashMap::with_capacity(2);
        metadata.insert(
            "ARROW:extension:name".to_string(),
            self.extension_name().to_string(),
        );
        metadata.insert(
            "ARROW:extension:metadata".to_string(),
            serde_json::to_string(self.metadata.as_ref()).unwrap(),
        );
        Arc::new(Field::new("", self.storage_type(), true).with_metadata(metadata))
    }

    fn extension_name(&self) -> &str {
        "geoarrow.linestring"
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
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

    fn as_ref(&self) -> &dyn GeometryArrayTrait {
        self
    }
}

impl<O: OffsetSizeTrait> GeometryArraySelfMethods for LineStringArray<O> {
    fn with_coords(self, coords: CoordBuffer) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(coords, self.geom_offsets, self.validity, self.metadata)
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.geom_offsets,
            self.validity,
            self.metadata,
        )
    }

    /// Slices this [`LineStringArray`] in place.
    ///
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Examples
    /// ```
    /// use arrow::array::PrimitiveArray;
    /// use arrow_array::types::Int32Type;
    ///
    /// let array: PrimitiveArray<Int32Type> = PrimitiveArray::from(vec![1, 2, 3]);
    /// assert_eq!(format!("{:?}", array), "PrimitiveArray<Int32>\n[\n  1,\n  2,\n  3,\n]");
    /// let sliced = array.slice(1, 1);
    /// assert_eq!(format!("{:?}", sliced), "PrimitiveArray<Int32>\n[\n  2,\n]");
    /// // note: `sliced` and `array` share the same memory region.
    /// ```
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        // Note: we **only** slice the geom_offsets and not any actual data. Otherwise the offsets
        // would be in the wrong location.
        Self {
            data_type: self.data_type,
            coords: self.coords.clone(),
            geom_offsets: self.geom_offsets.slice(offset, length),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
            metadata: self.metadata(),
        }
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        assert!(length >= 1, "length must be at least 1");

        // Find the start and end of the coord buffer
        let (start_coord_idx, _) = self.geom_offsets.start_end(offset);
        let (_, end_coord_idx) = self.geom_offsets.start_end(offset + length - 1);

        let geom_offsets = owned_slice_offsets(&self.geom_offsets, offset, length);

        let coords = self
            .coords
            .owned_slice(start_coord_idx, end_coord_idx - start_coord_idx);

        let validity = owned_slice_validity(self.nulls(), offset, length);

        Self::new(coords, geom_offsets, validity, self.metadata())
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for LineStringArray<O> {
    type Item = LineString<'a, O>;
    type ItemGeo = geo::LineString;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        LineString::new_borrowed(&self.coords, &self.geom_offsets, index)
    }
}

impl<O: OffsetSizeTrait> IntoArrow for LineStringArray<O> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = self.vertices_field();
        let validity = self.validity;
        let coord_array = self.coords.into_array_ref();
        GenericListArray::new(vertices_field, self.geom_offsets, coord_array, validity)
    }
}

impl<O: OffsetSizeTrait> TryFrom<&GenericListArray<O>> for LineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: &GenericListArray<O>) -> Result<Self> {
        let coords: CoordBuffer = value.values().as_ref().try_into()?;
        let geom_offsets = value.offsets();
        let validity = value.nulls();

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            validity.cloned(),
            Default::default(),
        ))
    }
}

impl TryFrom<&dyn Array> for LineStringArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<LargeListArray>().unwrap();
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
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                let geom_array: LineStringArray<i32> = downcasted.try_into()?;
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

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> From<Vec<Option<G>>> for LineStringArray<O> {
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: LineStringBuilder<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> From<&[G]> for LineStringArray<O> {
    fn from(other: &[G]) -> Self {
        let mut_arr: LineStringBuilder<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> From<bumpalo::collections::Vec<'_, Option<G>>>
    for LineStringArray<O>
{
    fn from(other: bumpalo::collections::Vec<'_, Option<G>>) -> Self {
        let mut_arr: LineStringBuilder<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait, G: LineStringTrait<T = f64>> From<bumpalo::collections::Vec<'_, G>>
    for LineStringArray<O>
{
    fn from(other: bumpalo::collections::Vec<'_, G>) -> Self {
        let mut_arr: LineStringBuilder<O> = other.into();
        mut_arr.into()
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl<O: OffsetSizeTrait> From<LineStringArray<O>> for MultiPointArray<O> {
    fn from(value: LineStringArray<O>) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.validity,
            value.metadata,
        )
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for LineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let mut_arr: LineStringBuilder<O> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

impl From<LineStringArray<i32>> for LineStringArray<i64> {
    fn from(value: LineStringArray<i32>) -> Self {
        Self::new(
            value.coords,
            offsets_buffer_i32_to_i64(&value.geom_offsets),
            value.validity,
            value.metadata,
        )
    }
}

impl TryFrom<LineStringArray<i64>> for LineStringArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: LineStringArray<i64>) -> Result<Self> {
        Ok(Self::new(
            value.coords,
            offsets_buffer_i64_to_i32(&value.geom_offsets)?,
            value.validity,
            value.metadata,
        ))
    }
}

/// Default to an empty array
impl<O: OffsetSizeTrait> Default for LineStringArray<O> {
    fn default() -> Self {
        LineStringBuilder::default().into()
    }
}

impl<O: OffsetSizeTrait> PartialEq for LineStringArray<O> {
    fn eq(&self, other: &Self) -> bool {
        if self.validity != other.validity {
            return false;
        }

        if !offset_buffer_eq(&self.geom_offsets, &other.geom_offsets) {
            return false;
        }

        if self.coords != other.coords {
            return false;
        }

        true
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
        let arr: LineStringArray<i64> = vec![ls0(), ls1()].as_slice().into();
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
    //     let arr: LineStringArray = vec![ls0(), ls1()].as_slice().into();
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
        let arr: LineStringArray<i64> = vec![ls0(), ls1()].as_slice().into();
        let sliced = arr.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(ls1()));
    }

    #[test]
    fn owned_slice() {
        let arr: LineStringArray<i64> = vec![ls0(), ls1()].as_slice().into();
        let sliced = arr.owned_slice(1, 1);

        // assert!(
        //     !sliced.geom_offsets.buffer().is_sliced(),
        //     "underlying offsets should not be sliced"
        // );
        assert_eq!(arr.len(), 2);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(ls1()));
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
