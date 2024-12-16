use std::sync::Arc;

use arrow::array::AsArray;
use arrow_array::{Array, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};

use crate::algorithm::native::eq::offset_buffer_eq;
use crate::array::geometrycollection::{GeometryCollectionBuilder, GeometryCollectionCapacity};
use crate::array::metadata::ArrayMetadata;
use crate::array::util::offsets_buffer_i64_to_i32;
use crate::array::{
    CoordBuffer, CoordType, LineStringArray, MixedGeometryArray, MultiLineStringArray,
    MultiPointArray, MultiPolygonArray, PointArray, PolygonArray, WKBArray,
};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::scalar::{Geometry, GeometryCollection};
use crate::trait_::{ArrayAccessor, GeometryArraySelfMethods, IntoArrow, NativeGeometryAccessor};
use crate::{ArrayBase, NativeArray};
use geo_traits::GeometryCollectionTrait;

/// An immutable array of GeometryCollection geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<GeometryCollection>>` due to the internal
/// validity bitmap.
#[derive(Debug, Clone)]
pub struct GeometryCollectionArray {
    // Always NativeType::GeometryCollection
    data_type: NativeType,

    metadata: Arc<ArrayMetadata>,

    pub(crate) array: MixedGeometryArray,

    /// Offsets into the mixed geometry array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i32>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

impl GeometryCollectionArray {
    /// Create a new GeometryCollectionArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    pub fn new(
        array: MixedGeometryArray,
        geom_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let coord_type = array.coord_type();
        let data_type = NativeType::GeometryCollection(coord_type, array.dimension());
        Self {
            data_type,
            array,
            geom_offsets,
            validity,
            metadata,
        }
    }

    fn geometries_field(&self) -> Arc<Field> {
        Field::new("geometries", self.array.storage_type(), false).into()
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> GeometryCollectionCapacity {
        GeometryCollectionCapacity::new(
            self.array.buffer_lengths(),
            *self.geom_offsets.last().unwrap() as usize,
        )
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes()
    }

    /// Slices this [`GeometryCollectionArray`] in place.
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
    ///
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
        // Note: we **only** slice the geom_offsets and not any actual data
        Self {
            data_type: self.data_type,
            array: self.array.clone(),
            geom_offsets: self.geom_offsets.slice(offset, length),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
            metadata: self.metadata(),
        }
    }

    pub fn to_coord_type(&self, coord_type: CoordType) -> Self {
        self.clone().into_coord_type(coord_type)
    }

    pub fn into_coord_type(self, coord_type: CoordType) -> Self {
        Self::new(
            self.array.into_coord_type(coord_type),
            self.geom_offsets,
            self.validity,
            self.metadata,
        )
    }

    fn value(&self, index: usize) -> GeometryCollection {
        GeometryCollection::new(self.slice(index, 1))
    }
}

impl ArrayBase for GeometryCollectionArray {
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
        // TODO: double check/make helper for this
        self.geom_offsets.len() - 1
    }

    /// Returns the optional validity.
    #[inline]
    fn nulls(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }
}

impl NativeArray for GeometryCollectionArray {
    fn data_type(&self) -> NativeType {
        self.data_type
    }

    fn coord_type(&self) -> CoordType {
        self.array.coord_type()
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
}

impl GeometryArraySelfMethods for GeometryCollectionArray {
    fn with_coords(self, _coords: CoordBuffer) -> Self {
        todo!()
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        todo!()
    }
}

impl NativeGeometryAccessor for GeometryCollectionArray {
    unsafe fn value_as_geometry_unchecked(&self, index: usize) -> crate::scalar::Geometry {
        Geometry::GeometryCollection(self.value(index))
    }
}

#[cfg(feature = "geos")]
impl<'a> crate::trait_::NativeGEOSGeometryAccessor<'a> for GeometryCollectionArray {
    unsafe fn value_as_geometry_unchecked(
        &'a self,
        index: usize,
    ) -> std::result::Result<geos::Geometry, geos::Error> {
        let geom = self.value(index);
        (&geom).try_into()
    }
}

impl ArrayAccessor for GeometryCollectionArray {
    type Item = GeometryCollection;
    type ItemGeo = geo::GeometryCollection;

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        self.value(index)
    }
}

impl IntoArrow for GeometryCollectionArray {
    type ArrowArray = GenericListArray<i32>;

    fn into_arrow(self) -> Self::ArrowArray {
        let geometries_field = self.geometries_field();
        let validity = self.validity;
        let values = self.array.into_array_ref();
        GenericListArray::new(geometries_field, self.geom_offsets, values, validity)
    }
}

impl TryFrom<(&GenericListArray<i32>, Dimension)> for GeometryCollectionArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&GenericListArray<i32>, Dimension)) -> Result<Self> {
        let geoms: MixedGeometryArray = (value.values().as_ref(), dim).try_into()?;
        let geom_offsets = value.offsets();
        let validity = value.nulls();

        Ok(Self::new(
            geoms,
            geom_offsets.clone(),
            validity.cloned(),
            Default::default(),
        ))
    }
}

impl TryFrom<(&GenericListArray<i64>, Dimension)> for GeometryCollectionArray {
    type Error = GeoArrowError;

    fn try_from((value, dim): (&GenericListArray<i64>, Dimension)) -> Result<Self> {
        let geoms: MixedGeometryArray = (value.values().as_ref(), dim).try_into()?;
        let geom_offsets = offsets_buffer_i64_to_i32(value.offsets())?;
        let validity = value.nulls();

        Ok(Self::new(
            geoms,
            geom_offsets,
            validity.cloned(),
            Default::default(),
        ))
    }
}

impl TryFrom<(&dyn Array, Dimension)> for GeometryCollectionArray {
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

impl TryFrom<(&dyn Array, &Field)> for GeometryCollectionArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let geom_type = NativeType::try_from(field)?;
        let dim = geom_type
            .dimension()
            .ok_or(GeoArrowError::General("Expected dimension".to_string()))?;
        let mut arr: Self = (arr, dim).try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<G: GeometryCollectionTrait<T = f64>> From<(&[G], Dimension)> for GeometryCollectionArray {
    fn from(other: (&[G], Dimension)) -> Self {
        let mut_arr: GeometryCollectionBuilder = other.into();
        mut_arr.into()
    }
}

impl<G: GeometryCollectionTrait<T = f64>> From<(Vec<Option<G>>, Dimension)>
    for GeometryCollectionArray
{
    fn from(other: (Vec<Option<G>>, Dimension)) -> Self {
        let mut_arr: GeometryCollectionBuilder = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, Dimension)> for GeometryCollectionArray {
    type Error = GeoArrowError;

    fn try_from(value: (WKBArray<O>, Dimension)) -> Result<Self> {
        let mut_arr: GeometryCollectionBuilder = value.try_into()?;
        Ok(mut_arr.into())
    }
}

/// Default to an empty array
impl Default for GeometryCollectionArray {
    fn default() -> Self {
        GeometryCollectionBuilder::default().into()
    }
}

impl PartialEq for GeometryCollectionArray {
    fn eq(&self, other: &Self) -> bool {
        if self.validity != other.validity {
            return false;
        }

        if !offset_buffer_eq(&self.geom_offsets, &other.geom_offsets) {
            return false;
        }

        if self.array != other.array {
            return false;
        }

        true
    }
}

impl From<PointArray> for GeometryCollectionArray {
    fn from(value: PointArray) -> Self {
        MixedGeometryArray::from(value).into()
    }
}

impl From<LineStringArray> for GeometryCollectionArray {
    fn from(value: LineStringArray) -> Self {
        MixedGeometryArray::from(value).into()
    }
}

impl From<PolygonArray> for GeometryCollectionArray {
    fn from(value: PolygonArray) -> Self {
        MixedGeometryArray::from(value).into()
    }
}
impl From<MultiPointArray> for GeometryCollectionArray {
    fn from(value: MultiPointArray) -> Self {
        MixedGeometryArray::from(value).into()
    }
}
impl From<MultiLineStringArray> for GeometryCollectionArray {
    fn from(value: MultiLineStringArray) -> Self {
        MixedGeometryArray::from(value).into()
    }
}
impl From<MultiPolygonArray> for GeometryCollectionArray {
    fn from(value: MultiPolygonArray) -> Self {
        MixedGeometryArray::from(value).into()
    }
}

impl From<MixedGeometryArray> for GeometryCollectionArray {
    // TODO: We should construct the correct validity buffer from the union's underlying arrays.
    fn from(value: MixedGeometryArray) -> Self {
        let metadata = value.metadata.clone();
        let geom_offsets = OffsetBuffer::from_lengths(vec![1; value.len()]);
        GeometryCollectionArray::new(value, geom_offsets, None, metadata)
    }
}
