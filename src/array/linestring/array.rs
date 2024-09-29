use std::collections::HashMap;
use std::sync::Arc;

use crate::algorithm::native::downcast::can_downcast_multi;
use crate::algorithm::native::eq::offset_buffer_eq;
use crate::array::linestring::LineStringCapacity;
use crate::array::metadata::ArrayMetadata;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, CoordType, GeometryCollectionArray, MixedGeometryArray, MultiLineStringArray, MultiPointArray, WKBArray};
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::scalar::LineString;
use crate::trait_::{ArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::util::{owned_slice_offsets, owned_slice_validity};
use crate::{ArrayBase, NativeArray};
use arrow_array::{Array, ArrayRef, GenericListArray, LargeListArray, ListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef};

use super::LineStringBuilder;

/// An immutable array of LineString geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<LineString>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
pub struct LineStringArray<const D: usize> {
    // Always NativeType::LineString or NativeType::LargeLineString
    data_type: NativeType,

    pub(crate) metadata: Arc<ArrayMetadata>,

    pub(crate) coords: CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i32>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

pub(super) fn check<const D: usize>(coords: &CoordBuffer<D>, validity_len: Option<usize>, geom_offsets: &OffsetBuffer<i32>) -> Result<()> {
    if validity_len.map_or(false, |len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General("validity mask length must match the number of values".to_string()));
    }

    if geom_offsets.last().to_usize().unwrap() != coords.len() {
        return Err(GeoArrowError::General("largest geometry offset must match coords length".to_string()));
    }

    Ok(())
}

impl<const D: usize> LineStringArray<D> {
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
    pub fn new(coords: CoordBuffer<D>, geom_offsets: OffsetBuffer<i32>, validity: Option<NullBuffer>, metadata: Arc<ArrayMetadata>) -> Self {
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
    pub fn try_new(coords: CoordBuffer<D>, geom_offsets: OffsetBuffer<i32>, validity: Option<NullBuffer>, metadata: Arc<ArrayMetadata>) -> Result<Self> {
        check(&coords, validity.as_ref().map(|v| v.len()), &geom_offsets)?;

        let coord_type = coords.coord_type();
        let data_type = NativeType::LineString(coord_type, D.try_into()?);

        Ok(Self { data_type, coords, geom_offsets, validity, metadata })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("vertices", self.coords.storage_type(), false).into()
    }

    pub fn coords(&self) -> &CoordBuffer<D> {
        &self.coords
    }

    pub fn into_inner(self) -> (CoordBuffer<D>, OffsetBuffer<i32>, Option<NullBuffer>) {
        (self.coords, self.geom_offsets, self.validity)
    }

    pub fn geom_offsets(&self) -> &OffsetBuffer<i32> {
        &self.geom_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> LineStringCapacity {
        LineStringCapacity::new(self.geom_offsets.last().to_usize().unwrap(), self.len())
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes()
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
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(offset + length <= self.len(), "offset + length may not exceed length of array");
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

    pub fn owned_slice(&self, offset: usize, length: usize) -> Self {
        assert!(offset + length <= self.len(), "offset + length may not exceed length of array");
        assert!(length >= 1, "length must be at least 1");

        // Find the start and end of the coord buffer
        let (start_coord_idx, _) = self.geom_offsets.start_end(offset);
        let (_, end_coord_idx) = self.geom_offsets.start_end(offset + length - 1);

        let geom_offsets = owned_slice_offsets(&self.geom_offsets, offset, length);

        let coords = self.coords.owned_slice(start_coord_idx, end_coord_idx - start_coord_idx);

        let validity = owned_slice_validity(self.nulls(), offset, length);

        Self::new(coords, geom_offsets, validity, self.metadata())
    }

    pub fn to_coord_type(&self, coord_type: CoordType) -> Self {
        self.clone().into_coord_type(coord_type)
    }

    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(self.coords.into_coord_type(coord_type), self.geom_offsets, self.validity, self.metadata)
    }
}

impl<const D: usize> ArrayBase for LineStringArray<D> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        self.data_type.to_data_type()
    }

    fn extension_field(&self) -> FieldRef {
        let mut metadata = HashMap::with_capacity(2);
        metadata.insert("ARROW:extension:name".to_string(), self.extension_name().to_string());
        if self.metadata.should_serialize() {
            metadata.insert("ARROW:extension:metadata".to_string(), serde_json::to_string(self.metadata.as_ref()).unwrap());
        }
        Arc::new(Field::new("", self.storage_type(), true).with_metadata(metadata))
    }

    fn extension_name(&self) -> &str {
        self.data_type.extension_name()
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> arrow_array::ArrayRef {
        self.clone().into_array_ref()
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
    fn nulls(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }
}

impl<const D: usize> NativeArray for LineStringArray<D> {
    fn data_type(&self) -> NativeType {
        self.data_type
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn to_coord_type(&self, coord_type: CoordType) -> Arc<dyn NativeArray> {
        Arc::new(self.clone().into_coord_type(coord_type))
    }

    fn with_metadata(&self, metadata: Arc<ArrayMetadata>) -> crate::trait_::NativeArrayRef {
        let mut arr = self.clone();
        arr.metadata = metadata;
        Arc::new(arr)
    }

    fn as_ref(&self) -> &dyn NativeArray {
        self
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        Arc::new(self.slice(offset, length))
    }

    fn owned_slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        Arc::new(self.owned_slice(offset, length))
    }
}

impl<const D: usize> GeometryArraySelfMethods<D> for LineStringArray<D> {
    fn with_coords(self, coords: CoordBuffer<D>) -> Self {
        assert_eq!(coords.len(), self.coords.len());
        Self::new(coords, self.geom_offsets, self.validity, self.metadata)
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(self.coords.into_coord_type(coord_type), self.geom_offsets, self.validity, self.metadata)
    }
}

impl<'a, const D: usize> ArrayAccessor<'a> for LineStringArray<D> {
    type Item = LineString<'a, D>;
    type ItemGeo = geo::LineString;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        LineString::new(&self.coords, &self.geom_offsets, index)
    }
}

impl<const D: usize> IntoArrow for LineStringArray<D> {
    type ArrowArray = GenericListArray<i32>;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = self.vertices_field();
        let validity = self.validity;
        let coord_array = self.coords.into_array_ref();
        GenericListArray::new(vertices_field, self.geom_offsets, coord_array, validity)
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<&GenericListArray<O>> for LineStringArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: &GenericListArray<O>) -> Result<Self> {
        let coords: CoordBuffer<D> = value.values().as_ref().try_into()?;
        let geom_offsets = value.offsets();
        let validity = value.nulls();

        Ok(Self::new(coords, geom_offsets.clone(), validity.cloned(), Default::default()))
    }
}

impl<const D: usize> TryFrom<&dyn Array> for LineStringArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<LargeListArray>().unwrap();
                let geom_array: LineStringArray<i64, D> = downcasted.try_into()?;
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!("Unexpected type: {:?}", value.data_type()))),
        }
    }
}

impl<const D: usize> TryFrom<(&dyn Array, &Field)> for LineStringArray<D> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<G: LineStringTrait<T = f64>, const D: usize> From<Vec<Option<G>>> for LineStringArray<D> {
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: LineStringBuilder<D> = other.into();
        mut_arr.into()
    }
}

impl<G: LineStringTrait<T = f64>, const D: usize> From<&[G]> for LineStringArray<D> {
    fn from(other: &[G]) -> Self {
        let mut_arr: LineStringBuilder<D> = other.into();
        mut_arr.into()
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl<const D: usize> From<LineStringArray<D>> for MultiPointArray<D> {
    fn from(value: LineStringArray<D>) -> Self {
        Self::new(value.coords, value.geom_offsets, value.validity, value.metadata)
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<WKBArray<O>> for LineStringArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let mut_arr: LineStringBuilder<D> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

/// Default to an empty array
impl<const D: usize> Default for LineStringArray<D> {
    fn default() -> Self {
        LineStringBuilder::default().into()
    }
}

impl<const D: usize> PartialEq for LineStringArray<D> {
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

impl<const D: usize> TryFrom<MultiLineStringArray<D>> for LineStringArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: MultiLineStringArray<D>) -> Result<Self> {
        if !can_downcast_multi(&value.geom_offsets) {
            return Err(GeoArrowError::General("Unable to cast".to_string()));
        }

        Ok(LineStringArray::new(value.coords, value.ring_offsets, value.validity, value.metadata))
    }
}

impl<const D: usize> TryFrom<MixedGeometryArray<D>> for LineStringArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: MixedGeometryArray<D>) -> Result<Self> {
        if value.has_points() || value.has_polygons() || value.has_multi_points() || value.has_multi_polygons() {
            return Err(GeoArrowError::General("Unable to cast".to_string()));
        }

        if value.has_only_line_strings() {
            return Ok(value.line_strings);
        }

        if value.has_only_multi_line_strings() {
            return value.multi_line_strings.try_into();
        }

        let mut capacity = value.line_strings.buffer_lengths();
        let buffer_lengths = value.multi_line_strings.buffer_lengths();
        capacity.coord_capacity += buffer_lengths.coord_capacity;
        capacity.geom_capacity += buffer_lengths.ring_capacity;

        let mut builder = LineStringBuilder::<D>::with_capacity_and_options(capacity, value.coord_type(), value.metadata());
        value.iter().try_for_each(|x| builder.push_geometry(x.as_ref()))?;
        Ok(builder.finish())
    }
}

impl<const D: usize> TryFrom<GeometryCollectionArray<D>> for LineStringArray<D> {
    type Error = GeoArrowError;

    fn try_from(value: GeometryCollectionArray<D>) -> Result<Self> {
        MixedGeometryArray::try_from(value)?.try_into()
    }
}

#[cfg(test)]
mod test {
    use crate::test::geoarrow_data::{example_linestring_interleaved, example_linestring_separated, example_linestring_wkb};
    use crate::test::linestring::{ls0, ls1};

    use super::*;

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: LineStringArray<2> = vec![ls0(), ls1()].as_slice().into();
        assert_eq!(arr.value_as_geo(0), ls0());
        assert_eq!(arr.value_as_geo(1), ls1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: LineStringArray<2> = vec![Some(ls0()), Some(ls1()), None].into();
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
        let arr: LineStringArray<2> = vec![ls0(), ls1()].as_slice().into();
        let sliced = arr.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(ls1()));
    }

    #[test]
    fn owned_slice() {
        let arr: LineStringArray<2> = vec![ls0(), ls1()].as_slice().into();
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
        let parsed_linestring_arr: LineStringArray<2> = wkb_arr.try_into().unwrap();

        assert_eq!(linestring_arr, parsed_linestring_arr);
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        let linestring_arr = example_linestring_separated().into_coord_type(CoordType::Interleaved);

        let wkb_arr = example_linestring_wkb();
        let parsed_linestring_arr: LineStringArray<2> = wkb_arr.try_into().unwrap();

        assert_eq!(linestring_arr, parsed_linestring_arr);
    }
}
