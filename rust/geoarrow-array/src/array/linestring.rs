use std::sync::Arc;

use crate::algorithm::native::downcast::can_downcast_multi;
use crate::algorithm::native::eq::offset_buffer_eq;
use crate::array::{
    CoordBuffer, GeometryCollectionArray, MixedGeometryArray, MultiLineStringArray,
    MultiPointArray, WKBArray,
};
use crate::builder::LineStringBuilder;
use crate::capacity::LineStringCapacity;
use crate::datatypes::NativeType;
use crate::error::{GeoArrowError, Result};
use crate::scalar::{Geometry, LineString};
use crate::trait_::{
    ArrayAccessor, ArrayBase, GeometryArraySelfMethods, IntoArrow, NativeArray,
    NativeGeometryAccessor,
};
use crate::util::{offsets_buffer_i64_to_i32, OffsetBufferUtils};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::extension::ExtensionType;
use arrow_schema::{DataType, Field, FieldRef};
use geo_traits::LineStringTrait;
use geoarrow_schema::{CoordType, Dimension, LineStringType, Metadata};

/// An immutable array of LineString geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<LineString>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
pub struct LineStringArray {
    data_type: LineStringType,

    pub(crate) coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i32>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

pub(super) fn check(
    coords: &CoordBuffer,
    validity_len: Option<usize>,
    geom_offsets: &OffsetBuffer<i32>,
) -> Result<()> {
    if validity_len.is_some_and(|len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if *geom_offsets.last() as usize != coords.len() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match coords length".to_string(),
        ));
    }

    Ok(())
}

impl LineStringArray {
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
        geom_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<Metadata>,
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
        geom_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        check(&coords, validity.as_ref().map(|v| v.len()), &geom_offsets)?;
        Ok(Self {
            data_type: LineStringType::new(coords.coord_type(), coords.dim(), metadata),
            coords,
            geom_offsets,
            validity,
        })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("vertices", self.coords.storage_type(), false).into()
    }

    /// Access the underlying coordinate buffer
    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    #[allow(dead_code)]
    pub(crate) fn into_inner(self) -> (CoordBuffer, OffsetBuffer<i32>, Option<NullBuffer>) {
        (self.coords, self.geom_offsets, self.validity)
    }

    /// Access the underlying geometry offsets buffer
    pub fn geom_offsets(&self) -> &OffsetBuffer<i32> {
        &self.geom_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> LineStringCapacity {
        LineStringCapacity::new(*self.geom_offsets.last() as usize, self.len())
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
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        // Note: we **only** slice the geom_offsets and not any actual data. Otherwise the offsets
        // would be in the wrong location.
        Self {
            data_type: self.data_type.clone(),
            coords: self.coords.clone(),
            geom_offsets: self.geom_offsets.slice(offset, length),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
        }
    }

    /// Change the coordinate type of this array.
    pub fn to_coord_type(&self, coord_type: CoordType) -> Self {
        self.clone().into_coord_type(coord_type)
    }

    /// Change the coordinate type of this array.
    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        let metadata = self.metadata();
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.geom_offsets,
            self.validity,
            metadata,
        )
    }
}

impl ArrayBase for LineStringArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        self.data_type.data_type()
    }

    fn extension_field(&self) -> FieldRef {
        Field::new("", self.storage_type(), true)
            .with_extension_type(self.data_type.clone())
            .into()
    }

    fn extension_name(&self) -> &str {
        LineStringType::NAME
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
    }

    fn metadata(&self) -> Arc<Metadata> {
        self.data_type.metadata().clone()
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

impl NativeArray for LineStringArray {
    fn data_type(&self) -> NativeType {
        NativeType::LineString(self.data_type.clone())
    }

    fn coord_type(&self) -> CoordType {
        self.coords.coord_type()
    }

    fn to_coord_type(&self, coord_type: CoordType) -> Arc<dyn NativeArray> {
        Arc::new(self.clone().into_coord_type(coord_type))
    }

    fn with_metadata(&self, metadata: Arc<Metadata>) -> crate::trait_::NativeArrayRef {
        let mut arr = self.clone();
        arr.data_type = self.data_type.clone().with_metadata(metadata);
        Arc::new(arr)
    }

    fn as_ref(&self) -> &dyn NativeArray {
        self
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn NativeArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl GeometryArraySelfMethods for LineStringArray {
    fn with_coords(self, coords: CoordBuffer) -> Self {
        let metadata = self.metadata();
        assert_eq!(coords.len(), self.coords.len());
        Self::new(coords, self.geom_offsets, self.validity, metadata)
    }

    fn into_coord_type(self, coord_type: CoordType) -> Self {
        let metadata = self.metadata();
        Self::new(
            self.coords.into_coord_type(coord_type),
            self.geom_offsets,
            self.validity,
            metadata,
        )
    }
}

impl NativeGeometryAccessor for LineStringArray {
    unsafe fn value_as_geometry_unchecked(&self, index: usize) -> crate::scalar::Geometry {
        Geometry::LineString(LineString::new(&self.coords, &self.geom_offsets, index))
    }
}

impl<'a> ArrayAccessor<'a> for LineStringArray {
    type Item = LineString<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        LineString::new(&self.coords, &self.geom_offsets, index)
    }
}

impl IntoArrow for LineStringArray {
    type ArrowArray = GenericListArray<i32>;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = self.vertices_field();
        let validity = self.validity;
        let coord_array = self.coords.into_array_ref();
        GenericListArray::new(vertices_field, self.geom_offsets, coord_array, validity)
    }
}

impl TryFrom<(&GenericListArray<i32>, Dimension)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&GenericListArray<i32>, Dimension)) -> Result<Self> {
        let coords = CoordBuffer::from_arrow(value.values().as_ref(), dim)?;
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

impl TryFrom<(&GenericListArray<i64>, Dimension)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&GenericListArray<i64>, Dimension)) -> Result<Self> {
        let coords = CoordBuffer::from_arrow(value.values().as_ref(), dim)?;
        let geom_offsets = offsets_buffer_i64_to_i32(value.offsets())?;
        let validity = value.nulls();

        Ok(Self::new(
            coords,
            geom_offsets,
            validity.cloned(),
            Default::default(),
        ))
    }
}
impl TryFrom<(&dyn Array, Dimension)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&dyn Array, Dimension)) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_list::<i32>();
                (downcasted, dim).try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_list::<i64>();
                (downcasted, dim).try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let geom_type = NativeType::try_from(field)?;
        let dim = geom_type
            .dimension()
            .ok_or(GeoArrowError::General("Expected dimension".to_string()))?;
        let mut arr: Self = (arr, dim).try_into()?;
        let metadata = Arc::new(Metadata::try_from(field)?);
        arr.data_type = arr.data_type.clone().with_metadata(metadata);
        Ok(arr)
    }
}

impl<G: LineStringTrait<T = f64>> From<(Vec<Option<G>>, Dimension)> for LineStringArray {
    fn from(other: (Vec<Option<G>>, Dimension)) -> Self {
        let mut_arr: LineStringBuilder = other.into();
        mut_arr.into()
    }
}

impl<G: LineStringTrait<T = f64>> From<(&[G], Dimension)> for LineStringArray {
    fn from(other: (&[G], Dimension)) -> Self {
        let mut_arr: LineStringBuilder = other.into();
        mut_arr.into()
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl From<LineStringArray> for MultiPointArray {
    fn from(value: LineStringArray) -> Self {
        let metadata = value.metadata();
        Self::new(value.coords, value.geom_offsets, value.validity, metadata)
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, Dimension)> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: (WKBArray<O>, Dimension)) -> Result<Self> {
        let mut_arr: LineStringBuilder = value.try_into()?;
        Ok(mut_arr.into())
    }
}

/// Default to an empty array
impl Default for LineStringArray {
    fn default() -> Self {
        LineStringBuilder::default().into()
    }
}

impl PartialEq for LineStringArray {
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

impl TryFrom<MultiLineStringArray> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: MultiLineStringArray) -> Result<Self> {
        if !can_downcast_multi(&value.geom_offsets) {
            return Err(GeoArrowError::General("Unable to cast".to_string()));
        }

        let metadata = value.metadata();
        Ok(LineStringArray::new(
            value.coords,
            value.ring_offsets,
            value.validity,
            metadata,
        ))
    }
}

impl TryFrom<MixedGeometryArray> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: MixedGeometryArray) -> Result<Self> {
        if value.has_points()
            || value.has_polygons()
            || value.has_multi_points()
            || value.has_multi_polygons()
        {
            return Err(GeoArrowError::General("Unable to cast".to_string()));
        }

        let (offset, length) = value.slice_offset_length();
        if value.has_only_line_strings() {
            return Ok(value.line_strings.slice(offset, length));
        }

        if value.has_only_multi_line_strings() {
            return value.multi_line_strings.slice(offset, length).try_into();
        }

        let mut capacity = value.line_strings.buffer_lengths();
        let buffer_lengths = value.multi_line_strings.buffer_lengths();
        capacity.coord_capacity += buffer_lengths.coord_capacity;
        capacity.geom_capacity += buffer_lengths.ring_capacity;

        let mut builder = LineStringBuilder::with_capacity_and_options(
            value.dimension(),
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

impl TryFrom<GeometryCollectionArray> for LineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: GeometryCollectionArray) -> Result<Self> {
        MixedGeometryArray::try_from(value)?.try_into()
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
        let arr: LineStringArray = (vec![ls0(), ls1()].as_slice(), Dimension::XY).into();
        assert_eq!(arr.value_as_geo(0), ls0());
        assert_eq!(arr.value_as_geo(1), ls1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: LineStringArray = (vec![Some(ls0()), Some(ls1()), None], Dimension::XY).into();
        assert_eq!(arr.get_as_geo(0), Some(ls0()));
        assert_eq!(arr.get_as_geo(1), Some(ls1()));
        assert_eq!(arr.get_as_geo(2), None);
    }

    // #[test]
    // fn rstar_integration() {
    //     let arr: LineStringArray = (vec![ls0(), ls1()].as_slice(), Dimension::XY).into();
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
        let arr: LineStringArray = (vec![ls0(), ls1()].as_slice(), Dimension::XY).into();
        let sliced = arr.slice(1, 1);
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced.get_as_geo(0), Some(ls1()));
    }

    #[test]
    fn parse_wkb_geoarrow_interleaved_example() {
        let linestring_arr = example_linestring_interleaved();

        let wkb_arr = example_linestring_wkb();
        let parsed_linestring_arr: LineStringArray = (wkb_arr, Dimension::XY).try_into().unwrap();

        assert_eq!(linestring_arr, parsed_linestring_arr);
    }

    #[test]
    fn parse_wkb_geoarrow_separated_example() {
        let linestring_arr = example_linestring_separated().into_coord_type(CoordType::Interleaved);

        let wkb_arr = example_linestring_wkb();
        let parsed_linestring_arr: LineStringArray = (wkb_arr, Dimension::XY).try_into().unwrap();

        assert_eq!(linestring_arr, parsed_linestring_arr);
    }
}
