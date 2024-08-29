use std::sync::Arc;

use crate::algorithm::native::eq::offset_buffer_eq;
use crate::array::metadata::ArrayMetadata;
use crate::array::multilinestring::MultiLineStringCapacity;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::util::{offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32, OffsetBufferUtils};
use crate::array::{CoordBuffer, CoordType, LineStringArray, PolygonArray, WKBArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::MultiLineStringTrait;
use crate::scalar::MultiLineString;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::util::{owned_slice_offsets, owned_slice_validity};
use crate::GeometryArrayTrait;
use arrow_array::{Array, GenericListArray, LargeListArray, ListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};

use super::MultiLineStringBuilder;

/// An immutable array of MultiLineString geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<MultiLineString>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
// #[derive(Debug, Clone, PartialEq)]
pub struct MultiLineStringArray<O: OffsetSizeTrait, const D: usize> {
    // Always GeoDataType::MultiLineString or GeoDataType::LargeMultiLineString
    data_type: GeoDataType,

    pub(crate) metadata: Arc<ArrayMetadata>,

    pub(crate) coords: CoordBuffer<D>,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<O>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetBuffer<O>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

pub(super) fn check<O: OffsetSizeTrait, const D: usize>(
    coords: &CoordBuffer<D>,
    geom_offsets: &OffsetBuffer<O>,
    ring_offsets: &OffsetBuffer<O>,
    validity_len: Option<usize>,
) -> Result<()> {
    if validity_len.map_or(false, |len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if ring_offsets.last().to_usize().unwrap() != coords.len() {
        return Err(GeoArrowError::General(
            "largest ring offset must match coords length".to_string(),
        ));
    }

    if geom_offsets.last().to_usize().unwrap() != ring_offsets.len_proxy() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match ring offsets length".to_string(),
        ));
    }

    Ok(())
}

impl<O: OffsetSizeTrait, const D: usize> MultiLineStringArray<O, D> {
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
        coords: CoordBuffer<D>,
        geom_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        Self::try_new(coords, geom_offsets, ring_offsets, validity, metadata).unwrap()
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
        coords: CoordBuffer<D>,
        geom_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self> {
        check(
            &coords,
            &geom_offsets,
            &ring_offsets,
            validity.as_ref().map(|v| v.len()),
        )?;

        let coord_type = coords.coord_type();
        let data_type = match O::IS_LARGE {
            true => GeoDataType::LargeMultiLineString(coord_type, D.try_into()?),
            false => GeoDataType::MultiLineString(coord_type, D.try_into()?),
        };

        Ok(Self {
            data_type,
            coords,
            geom_offsets,
            ring_offsets,
            validity,
            metadata,
        })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("vertices", self.coords.storage_type(), false).into()
    }

    fn linestrings_field(&self) -> Arc<Field> {
        match O::IS_LARGE {
            true => Field::new_large_list("linestrings", self.vertices_field(), false).into(),
            false => Field::new_list("linestrings", self.vertices_field(), false).into(),
        }
    }

    pub fn coords(&self) -> &CoordBuffer<D> {
        &self.coords
    }

    pub fn geom_offsets(&self) -> &OffsetBuffer<O> {
        &self.geom_offsets
    }

    pub fn ring_offsets(&self) -> &OffsetBuffer<O> {
        &self.ring_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> MultiLineStringCapacity {
        MultiLineStringCapacity::new(
            self.ring_offsets.last().to_usize().unwrap(),
            self.geom_offsets.last().to_usize().unwrap(),
            self.len(),
        )
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes::<O>()
    }

    /// Slices this [`MultiLineStringArray`] in place.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
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
            ring_offsets: self.ring_offsets.clone(),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
            metadata: self.metadata(),
        }
    }

    pub fn owned_slice(&self, offset: usize, length: usize) -> Self {
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

        Self::new(
            coords,
            geom_offsets,
            ring_offsets,
            validity,
            self.metadata(),
        )
    }
}

impl<O: OffsetSizeTrait, const D: usize> GeometryArrayTrait for MultiLineStringArray<O, D> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> GeoDataType {
        self.data_type
    }

    fn storage_type(&self) -> DataType {
        self.data_type.to_data_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        self.data_type
            .to_field_with_metadata("geometry", true, &self.metadata)
            .into()
    }

    fn extension_name(&self) -> &str {
        self.data_type.extension_name()
    }

    fn into_array_ref(self) -> Arc<dyn Array> {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn to_coord_type(&self, coord_type: CoordType) -> Arc<dyn GeometryArrayTrait> {
        Arc::new(self.clone().into_coord_type(coord_type))
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
    }

    fn with_metadata(&self, metadata: Arc<ArrayMetadata>) -> crate::trait_::GeometryArrayRef {
        let mut arr = self.clone();
        arr.metadata = metadata;
        Arc::new(arr)
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.geom_offsets.len_proxy()
    }

    /// Returns the optional validity.
    #[inline]
    fn nulls(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }

    fn as_ref(&self) -> &dyn GeometryArrayTrait {
        self
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeometryArrayTrait> {
        Arc::new(self.slice(offset, length))
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Arc<dyn GeometryArrayTrait> {
        Arc::new(self.owned_slice(offset, length))
    }
}

impl<O: OffsetSizeTrait, const D: usize> GeometryArraySelfMethods<D>
    for MultiLineStringArray<O, D>
{
    fn with_coords(self, coords: CoordBuffer<D>) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(
            coords,
            self.geom_offsets,
            self.ring_offsets,
            self.validity,
            self.metadata,
        )
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.geom_offsets,
            self.ring_offsets,
            self.validity,
            self.metadata,
        )
    }
}

// Implement geometry accessors
impl<'a, O: OffsetSizeTrait, const D: usize> GeometryArrayAccessor<'a>
    for MultiLineStringArray<O, D>
{
    type Item = MultiLineString<'a, O, D>;
    type ItemGeo = geo::MultiLineString;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        MultiLineString::new(&self.coords, &self.geom_offsets, &self.ring_offsets, index)
    }
}

impl<O: OffsetSizeTrait, const D: usize> IntoArrow for MultiLineStringArray<O, D> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = self.vertices_field();
        let linestrings_field = self.linestrings_field();
        let validity = self.validity;
        let coord_array = self.coords.into_array_ref();
        let ring_array = Arc::new(GenericListArray::new(
            vertices_field,
            self.ring_offsets,
            coord_array,
            None,
        ));
        GenericListArray::new(linestrings_field, self.geom_offsets, ring_array, validity)
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<&GenericListArray<O>>
    for MultiLineStringArray<O, D>
{
    type Error = GeoArrowError;

    fn try_from(geom_array: &GenericListArray<O>) -> Result<Self> {
        let geom_offsets = geom_array.offsets();
        let validity = geom_array.nulls();

        let rings_dyn_array = geom_array.values();
        let rings_array = rings_dyn_array
            .as_any()
            .downcast_ref::<GenericListArray<O>>()
            .unwrap();

        let ring_offsets = rings_array.offsets();
        let coords: CoordBuffer<D> = rings_array.values().as_ref().try_into()?;

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
            Default::default(),
        ))
    }
}

impl<const D: usize> TryFrom<&dyn Array> for MultiLineStringArray<i32, D> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<LargeListArray>().unwrap();
                let geom_array: MultiLineStringArray<i64, D> = downcasted.try_into()?;
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl<const D: usize> TryFrom<&dyn Array> for MultiLineStringArray<i64, D> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                let geom_array: MultiLineStringArray<i32, D> = downcasted.try_into()?;
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

impl<const D: usize> TryFrom<(&dyn Array, &Field)> for MultiLineStringArray<i32, D> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<const D: usize> TryFrom<(&dyn Array, &Field)> for MultiLineStringArray<i64, D> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<O: OffsetSizeTrait, G: MultiLineStringTrait<T = f64>, const D: usize> From<Vec<Option<G>>>
    for MultiLineStringArray<O, D>
{
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: MultiLineStringBuilder<O, D> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait, G: MultiLineStringTrait<T = f64>, const D: usize> From<&[G]>
    for MultiLineStringArray<O, D>
{
    fn from(other: &[G]) -> Self {
        let mut_arr: MultiLineStringBuilder<O, D> = other.into();
        mut_arr.into()
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl<O: OffsetSizeTrait, const D: usize> From<MultiLineStringArray<O, D>> for PolygonArray<O, D> {
    fn from(value: MultiLineStringArray<O, D>) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
            value.metadata,
        )
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<WKBArray<O>> for MultiLineStringArray<O, D> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let mut_arr: MultiLineStringBuilder<O, D> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<LineStringArray<O, D>>
    for MultiLineStringArray<O, D>
{
    type Error = GeoArrowError;

    fn try_from(value: LineStringArray<O, D>) -> Result<Self> {
        let geom_length = value.len();

        let coords = value.coords;
        let ring_offsets = value.geom_offsets;
        let validity = value.validity;

        // Create offsets that are all of length 1
        let mut geom_offsets = OffsetsBuilder::with_capacity(geom_length);
        for _ in 0..coords.len() {
            geom_offsets.try_push_usize(1)?;
        }

        Ok(Self::new(
            coords,
            geom_offsets.into(),
            ring_offsets,
            validity,
            value.metadata,
        ))
    }
}

impl<const D: usize> From<MultiLineStringArray<i32, D>> for MultiLineStringArray<i64, D> {
    fn from(value: MultiLineStringArray<i32, D>) -> Self {
        Self::new(
            value.coords,
            offsets_buffer_i32_to_i64(&value.geom_offsets),
            offsets_buffer_i32_to_i64(&value.ring_offsets),
            value.validity,
            value.metadata,
        )
    }
}

impl<const D: usize> TryFrom<MultiLineStringArray<i64, D>> for MultiLineStringArray<i32, D> {
    type Error = GeoArrowError;

    fn try_from(value: MultiLineStringArray<i64, D>) -> Result<Self> {
        Ok(Self::new(
            value.coords,
            offsets_buffer_i64_to_i32(&value.geom_offsets)?,
            offsets_buffer_i64_to_i32(&value.ring_offsets)?,
            value.validity,
            value.metadata,
        ))
    }
}

/// Default to an empty array
impl<O: OffsetSizeTrait, const D: usize> Default for MultiLineStringArray<O, D> {
    fn default() -> Self {
        MultiLineStringBuilder::default().into()
    }
}

impl<O: OffsetSizeTrait, const D: usize> PartialEq for MultiLineStringArray<O, D> {
    fn eq(&self, other: &Self) -> bool {
        if self.validity != other.validity {
            return false;
        }

        if !offset_buffer_eq(&self.geom_offsets, &other.geom_offsets) {
            return false;
        }

        if !offset_buffer_eq(&self.ring_offsets, &other.ring_offsets) {
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
        example_multilinestring_interleaved, example_multilinestring_separated,
        example_multilinestring_wkb,
    };
    use crate::test::multilinestring::{ml0, ml1};

    use super::*;

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: MultiLineStringArray<i64, 2> = vec![ml0(), ml1()].as_slice().into();
        assert_eq!(arr.value_as_geo(0), ml0());
        assert_eq!(arr.value_as_geo(1), ml1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: MultiLineStringArray<i64, 2> = vec![Some(ml0()), Some(ml1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(ml0()));
        assert_eq!(arr.get_as_geo(1), Some(ml1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    #[test]
    fn slice() {
        let arr: MultiLineStringArray<i64, 2> = vec![ml0(), ml1()].as_slice().into();
        let sliced = arr.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(ml1()));
    }

    #[test]
    fn owned_slice() {
        let arr: MultiLineStringArray<i64, 2> = vec![ml0(), ml1()].as_slice().into();
        let sliced = arr.owned_slice(1, 1);

        // assert!(
        //     !sliced.geom_offsets.buffer().is_sliced(),
        //     "underlying offsets should not be sliced"
        // );
        assert_eq!(arr.len(), 2);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(ml1()));

        // // Offset is 0 because it's copied to an owned buffer
        // assert_eq!(*sliced.geom_offsets.first(), 0);
        // assert_eq!(*sliced.ring_offsets.first(), 0);
    }

    #[test]
    fn parse_wkb_geoarrow_interleaved_example() {
        let geom_arr = example_multilinestring_interleaved();

        let wkb_arr = example_multilinestring_wkb();
        let parsed_geom_arr: MultiLineStringArray<i64, 2> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        let geom_arr = example_multilinestring_separated().into_coord_type(CoordType::Interleaved);

        let wkb_arr = example_multilinestring_wkb();
        let parsed_geom_arr: MultiLineStringArray<i64, 2> = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }
}
