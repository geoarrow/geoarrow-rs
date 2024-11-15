use std::sync::Arc;

use crate::algorithm::native::eq::offset_buffer_eq;
use crate::array::metadata::ArrayMetadata;
use crate::array::multilinestring::MultiLineStringCapacity;
use crate::array::util::{offsets_buffer_i64_to_i32, OffsetBufferUtils};
use crate::array::{
    CoordBuffer, CoordType, GeometryCollectionArray, LineStringArray, MixedGeometryArray,
    PolygonArray, WKBArray,
};
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::{Geometry, MultiLineString};
use crate::trait_::{ArrayAccessor, GeometryArraySelfMethods, IntoArrow, NativeGeometryAccessor};
use crate::util::{owned_slice_offsets, owned_slice_validity};
use crate::{ArrayBase, NativeArray};
use arrow::array::AsArray;
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use geo_traits::MultiLineStringTrait;

use super::MultiLineStringBuilder;

/// An immutable array of MultiLineString geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<MultiLineString>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
pub struct MultiLineStringArray {
    // Always NativeType::MultiLineString or NativeType::LargeMultiLineString
    data_type: NativeType,

    pub(crate) metadata: Arc<ArrayMetadata>,

    pub(crate) coords: CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i32>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetBuffer<i32>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

pub(super) fn check(
    coords: &CoordBuffer,
    geom_offsets: &OffsetBuffer<i32>,
    ring_offsets: &OffsetBuffer<i32>,
    validity_len: Option<usize>,
) -> Result<()> {
    if validity_len.map_or(false, |len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if *ring_offsets.last() as usize != coords.len() {
        return Err(GeoArrowError::General(
            "largest ring offset must match coords length".to_string(),
        ));
    }

    if *geom_offsets.last() as usize != ring_offsets.len_proxy() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match ring offsets length".to_string(),
        ));
    }

    Ok(())
}

impl MultiLineStringArray {
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
        geom_offsets: OffsetBuffer<i32>,
        ring_offsets: OffsetBuffer<i32>,
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
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        ring_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Result<Self> {
        check(
            &coords,
            &geom_offsets,
            &ring_offsets,
            validity.as_ref().map(|v| v.len()),
        )?;
        let data_type = NativeType::MultiLineString(coords.coord_type(), coords.dim());
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
        Field::new_list("linestrings", self.vertices_field(), false).into()
    }

    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    pub fn geom_offsets(&self) -> &OffsetBuffer<i32> {
        &self.geom_offsets
    }

    pub fn ring_offsets(&self) -> &OffsetBuffer<i32> {
        &self.ring_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> MultiLineStringCapacity {
        MultiLineStringCapacity::new(
            *self.ring_offsets.last() as usize,
            *self.geom_offsets.last() as usize,
            self.len(),
        )
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes()
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

    pub fn to_coord_type(&self, coord_type: CoordType) -> Self {
        self.clone().into_coord_type(coord_type)
    }

    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.geom_offsets,
            self.ring_offsets,
            self.validity,
            self.metadata,
        )
    }
}

impl ArrayBase for MultiLineStringArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
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

impl NativeArray for MultiLineStringArray {
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

impl GeometryArraySelfMethods for MultiLineStringArray {
    fn with_coords(self, coords: CoordBuffer) -> Self {
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

impl NativeGeometryAccessor for MultiLineStringArray {
    unsafe fn value_as_geometry_unchecked(&self, index: usize) -> crate::scalar::Geometry {
        Geometry::MultiLineString(MultiLineString::new(
            &self.coords,
            &self.geom_offsets,
            &self.ring_offsets,
            index,
        ))
    }
}

#[cfg(feature = "geos")]
impl<'a> crate::trait_::NativeGEOSGeometryAccessor<'a> for MultiLineStringArray {
    unsafe fn value_as_geometry_unchecked(
        &'a self,
        index: usize,
    ) -> std::result::Result<geos::Geometry, geos::Error> {
        let geom =
            MultiLineString::new(&self.coords, &self.geom_offsets, &self.ring_offsets, index);
        (&geom).try_into()
    }
}

impl<'a> ArrayAccessor<'a> for MultiLineStringArray {
    type Item = MultiLineString<'a>;
    type ItemGeo = geo::MultiLineString;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        MultiLineString::new(&self.coords, &self.geom_offsets, &self.ring_offsets, index)
    }
}

impl IntoArrow for MultiLineStringArray {
    type ArrowArray = GenericListArray<i32>;

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

impl TryFrom<&GenericListArray<i32>> for MultiLineStringArray {
    type Error = GeoArrowError;

    fn try_from(geom_array: &GenericListArray<i32>) -> Result<Self> {
        let geom_offsets = geom_array.offsets();
        let validity = geom_array.nulls();

        let rings_dyn_array = geom_array.values();
        let rings_array = rings_dyn_array.as_list::<i32>();

        let ring_offsets = rings_array.offsets();
        let coords = CoordBuffer::from_arrow(rings_array.values().as_ref(), D.try_into()?)?;

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
            Default::default(),
        ))
    }
}

impl TryFrom<&GenericListArray<i64>> for MultiLineStringArray {
    type Error = GeoArrowError;

    fn try_from(geom_array: &GenericListArray<i64>) -> Result<Self> {
        let geom_offsets = offsets_buffer_i64_to_i32(geom_array.offsets())?;
        let validity = geom_array.nulls();

        let rings_dyn_array = geom_array.values();
        let rings_array = rings_dyn_array.as_list::<i64>();

        let ring_offsets = offsets_buffer_i64_to_i32(rings_array.offsets())?;
        let coords = CoordBuffer::from_arrow(rings_array.values().as_ref(), D.try_into()?)?;

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
            Default::default(),
        ))
    }
}

impl TryFrom<&dyn Array> for MultiLineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_list::<i32>();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_list::<i64>();
                downcasted.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for MultiLineStringArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<G: MultiLineStringTrait<T = f64>, const D: usize> From<Vec<Option<G>>>
    for MultiLineStringArray
{
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: MultiLineStringBuilder = other.into();
        mut_arr.into()
    }
}

impl<G: MultiLineStringTrait<T = f64>, const D: usize> From<&[G]> for MultiLineStringArray {
    fn from(other: &[G]) -> Self {
        let mut_arr: MultiLineStringBuilder = other.into();
        mut_arr.into()
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl From<MultiLineStringArray> for PolygonArray {
    fn from(value: MultiLineStringArray) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
            value.metadata,
        )
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<WKBArray<O>> for MultiLineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let mut_arr: MultiLineStringBuilder = value.try_into()?;
        Ok(mut_arr.into())
    }
}

impl From<LineStringArray> for MultiLineStringArray {
    fn from(value: LineStringArray) -> Self {
        let coords = value.coords;
        let geom_offsets = OffsetBuffer::from_lengths(vec![1; coords.len()]);
        let ring_offsets = value.geom_offsets;
        let validity = value.validity;
        Self::new(coords, geom_offsets, ring_offsets, validity, value.metadata)
    }
}

/// Default to an empty array
impl Default for MultiLineStringArray {
    fn default() -> Self {
        MultiLineStringBuilder::default().into()
    }
}

impl PartialEq for MultiLineStringArray {
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

impl TryFrom<MixedGeometryArray> for MultiLineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: MixedGeometryArray) -> Result<Self> {
        if value.has_points()
            || value.has_polygons()
            || value.has_multi_points()
            || value.has_multi_polygons()
        {
            return Err(GeoArrowError::General("Unable to cast".to_string()));
        }

        if value.has_only_line_strings() {
            return Ok(value.line_strings.into());
        }

        if value.has_only_multi_line_strings() {
            return Ok(value.multi_line_strings);
        }

        let mut capacity = value.multi_line_strings.buffer_lengths();
        capacity += value.line_strings.buffer_lengths();

        let mut builder = MultiLineStringBuilder::with_capacity_and_options(
            capacity,
            value.coord_type(),
            value.metadata(),
        );
        value
            .iter()
            .try_for_each(|x| builder.push_geometry(x.as_ref()))?;
        Ok(builder.finish())
    }
}

impl TryFrom<GeometryCollectionArray> for MultiLineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: GeometryCollectionArray) -> Result<Self> {
        MixedGeometryArray::try_from(value)?.try_into()
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
        let arr: MultiLineStringArray = vec![ml0(), ml1()].as_slice().into();
        assert_eq!(arr.value_as_geo(0), ml0());
        assert_eq!(arr.value_as_geo(1), ml1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: MultiLineStringArray = vec![Some(ml0()), Some(ml1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(ml0()));
        assert_eq!(arr.get_as_geo(1), Some(ml1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    #[test]
    fn slice() {
        let arr: MultiLineStringArray = vec![ml0(), ml1()].as_slice().into();
        let sliced = arr.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(ml1()));
    }

    #[test]
    fn owned_slice() {
        let arr: MultiLineStringArray = vec![ml0(), ml1()].as_slice().into();
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
        let parsed_geom_arr: MultiLineStringArray = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        let geom_arr = example_multilinestring_separated().into_coord_type(CoordType::Interleaved);

        let wkb_arr = example_multilinestring_wkb();
        let parsed_geom_arr: MultiLineStringArray = wkb_arr.try_into().unwrap();

        assert_eq!(geom_arr, parsed_geom_arr);
    }
}
