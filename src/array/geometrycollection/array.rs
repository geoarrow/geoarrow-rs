use std::sync::Arc;

use arrow_array::{Array, GenericListArray, LargeListArray, ListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};

use crate::algorithm::native::eq::offset_buffer_eq;
use crate::array::geometrycollection::{GeometryCollectionBuilder, GeometryCollectionCapacity};
use crate::array::metadata::ArrayMetadata;
use crate::array::util::{
    offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32, offsets_buffer_to_i32,
    offsets_buffer_to_i64,
};
use crate::array::{
    CoordBuffer, CoordType, LineStringArray, MixedGeometryArray, MultiLineStringArray,
    MultiPointArray, MultiPolygonArray, PointArray, PolygonArray, WKBArray,
};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::GeometryCollectionTrait;
use crate::scalar::GeometryCollection;
use crate::trait_::{GeometryArraySelfMethods, IntoArrow, NativeArrayAccessor};
use crate::NativeArray;

/// An immutable array of GeometryCollection geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<GeometryCollection>>` due to the internal
/// validity bitmap.
#[derive(Debug, Clone)]
pub struct GeometryCollectionArray<O: OffsetSizeTrait, const D: usize> {
    // Always GeoDataType::GeometryCollection or GeoDataType::LargeGeometryCollection
    data_type: GeoDataType,

    metadata: Arc<ArrayMetadata>,

    pub(crate) array: MixedGeometryArray<O, D>,

    /// Offsets into the mixed geometry array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<O>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait, const D: usize> GeometryCollectionArray<O, D> {
    /// Create a new GeometryCollectionArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    pub fn new(
        array: MixedGeometryArray<O, D>,
        geom_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let coord_type = array.coord_type();
        let data_type = match O::IS_LARGE {
            true => GeoDataType::LargeGeometryCollection(coord_type, D.try_into().unwrap()),
            false => GeoDataType::GeometryCollection(coord_type, D.try_into().unwrap()),
        };

        Self {
            data_type,
            array,
            geom_offsets,
            validity,
            metadata,
        }
    }

    fn mixed_field(&self) -> Arc<Field> {
        self.array.extension_field()
    }

    fn geometries_field(&self) -> Arc<Field> {
        let name = "geometries";
        match O::IS_LARGE {
            true => Field::new_large_list(name, self.mixed_field(), false).into(),
            false => Field::new_list(name, self.mixed_field(), false).into(),
        }
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> GeometryCollectionCapacity {
        GeometryCollectionCapacity::new(
            self.array.buffer_lengths(),
            self.geom_offsets.last().unwrap().to_usize().unwrap(),
        )
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes::<O>()
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

    pub fn owned_slice(&self, _offset: usize, _length: usize) -> Self {
        todo!()
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

    pub fn to_small_offsets(&self) -> Result<GeometryCollectionArray<i32, D>> {
        Ok(GeometryCollectionArray::new(
            self.array.to_small_offsets()?,
            offsets_buffer_to_i32(&self.geom_offsets)?,
            self.validity.clone(),
            self.metadata.clone(),
        ))
    }

    pub fn to_large_offsets(&self) -> GeometryCollectionArray<i64, D> {
        GeometryCollectionArray::new(
            self.array.to_large_offsets(),
            offsets_buffer_to_i64(&self.geom_offsets),
            self.validity.clone(),
            self.metadata.clone(),
        )
    }
}

impl<O: OffsetSizeTrait, const D: usize> NativeArray for GeometryCollectionArray<O, D> {
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
        self.array.coord_type()
    }

    fn to_coord_type(&self, coord_type: CoordType) -> Arc<dyn NativeArray> {
        Arc::new(self.clone().into_coord_type(coord_type))
    }

    fn metadata(&self) -> Arc<ArrayMetadata> {
        self.metadata.clone()
    }

    fn with_metadata(&self, metadata: Arc<ArrayMetadata>) -> crate::trait_::NativeArrayRef {
        let mut arr = self.clone();
        arr.metadata = metadata;
        Arc::new(arr)
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

impl<O: OffsetSizeTrait, const D: usize> GeometryArraySelfMethods<D>
    for GeometryCollectionArray<O, D>
{
    fn with_coords(self, _coords: CoordBuffer<D>) -> Self {
        todo!()
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        todo!()
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> NativeArrayAccessor<'a>
    for GeometryCollectionArray<O, D>
{
    type Item = GeometryCollection<'a, O, D>;
    type ItemGeo = geo::GeometryCollection;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        GeometryCollection::new(&self.array, &self.geom_offsets, index)
    }
}

impl<O: OffsetSizeTrait, const D: usize> IntoArrow for GeometryCollectionArray<O, D> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let geometries_field = self.geometries_field();
        let validity = self.validity;
        let values = self.array.into_array_ref();
        GenericListArray::new(geometries_field, self.geom_offsets, values, validity)
    }
}

impl<const D: usize> TryFrom<&GenericListArray<i32>> for GeometryCollectionArray<i32, D> {
    type Error = GeoArrowError;

    fn try_from(value: &GenericListArray<i32>) -> Result<Self> {
        let geoms: MixedGeometryArray<i32, D> = value.values().as_ref().try_into()?;
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

impl<const D: usize> TryFrom<&GenericListArray<i64>> for GeometryCollectionArray<i64, D> {
    type Error = GeoArrowError;

    fn try_from(value: &GenericListArray<i64>) -> Result<Self> {
        let geoms: MixedGeometryArray<i64, D> = value.values().as_ref().try_into()?;
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

impl<const D: usize> TryFrom<&dyn Array> for GeometryCollectionArray<i32, D> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<LargeListArray>().unwrap();
                let geom_array: GeometryCollectionArray<i64, D> = downcasted.try_into()?;
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl<const D: usize> TryFrom<&dyn Array> for GeometryCollectionArray<i64, D> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                let geom_array: GeometryCollectionArray<i32, D> = downcasted.try_into()?;
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

impl<const D: usize> TryFrom<(&dyn Array, &Field)> for GeometryCollectionArray<i32, D> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<const D: usize> TryFrom<(&dyn Array, &Field)> for GeometryCollectionArray<i64, D> {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let mut arr: Self = arr.try_into()?;
        arr.metadata = Arc::new(ArrayMetadata::try_from(field)?);
        Ok(arr)
    }
}

impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>> From<&[G]>
    for GeometryCollectionArray<O, 2>
{
    fn from(other: &[G]) -> Self {
        let mut_arr: GeometryCollectionBuilder<O, 2> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>> From<Vec<Option<G>>>
    for GeometryCollectionArray<O, 2>
{
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: GeometryCollectionBuilder<O, 2> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for GeometryCollectionArray<O, 2> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let mut_arr: GeometryCollectionBuilder<O, 2> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

impl<const D: usize> From<GeometryCollectionArray<i32, D>> for GeometryCollectionArray<i64, D> {
    fn from(value: GeometryCollectionArray<i32, D>) -> Self {
        Self::new(
            value.array.into(),
            offsets_buffer_i32_to_i64(&value.geom_offsets),
            value.validity,
            value.metadata,
        )
    }
}

impl<const D: usize> TryFrom<GeometryCollectionArray<i64, D>> for GeometryCollectionArray<i32, D> {
    type Error = GeoArrowError;

    fn try_from(value: GeometryCollectionArray<i64, D>) -> Result<Self> {
        Ok(Self::new(
            value.array.try_into()?,
            offsets_buffer_i64_to_i32(&value.geom_offsets)?,
            value.validity,
            value.metadata,
        ))
    }
}

/// Default to an empty array
impl<O: OffsetSizeTrait, const D: usize> Default for GeometryCollectionArray<O, D> {
    fn default() -> Self {
        GeometryCollectionBuilder::default().into()
    }
}

impl<O: OffsetSizeTrait, const D: usize> PartialEq for GeometryCollectionArray<O, D> {
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

impl<O: OffsetSizeTrait, const D: usize> From<PointArray<D>> for GeometryCollectionArray<O, D> {
    fn from(value: PointArray<D>) -> Self {
        MixedGeometryArray::<O, D>::from(value).into()
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<LineStringArray<O, D>>
    for GeometryCollectionArray<O, D>
{
    fn from(value: LineStringArray<O, D>) -> Self {
        MixedGeometryArray::<O, D>::from(value).into()
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<PolygonArray<O, D>>
    for GeometryCollectionArray<O, D>
{
    fn from(value: PolygonArray<O, D>) -> Self {
        MixedGeometryArray::<O, D>::from(value).into()
    }
}
impl<O: OffsetSizeTrait, const D: usize> From<MultiPointArray<O, D>>
    for GeometryCollectionArray<O, D>
{
    fn from(value: MultiPointArray<O, D>) -> Self {
        MixedGeometryArray::<O, D>::from(value).into()
    }
}
impl<O: OffsetSizeTrait, const D: usize> From<MultiLineStringArray<O, D>>
    for GeometryCollectionArray<O, D>
{
    fn from(value: MultiLineStringArray<O, D>) -> Self {
        MixedGeometryArray::<O, D>::from(value).into()
    }
}
impl<O: OffsetSizeTrait, const D: usize> From<MultiPolygonArray<O, D>>
    for GeometryCollectionArray<O, D>
{
    fn from(value: MultiPolygonArray<O, D>) -> Self {
        MixedGeometryArray::<O, D>::from(value).into()
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<MixedGeometryArray<O, D>>
    for GeometryCollectionArray<O, D>
{
    // TODO: We should construct the correct validity buffer from the union's underlying arrays.
    fn from(value: MixedGeometryArray<O, D>) -> Self {
        let metadata = value.metadata.clone();
        let geom_offsets = OffsetBuffer::from_lengths(vec![1; value.len()]);
        GeometryCollectionArray::new(value, geom_offsets, None, metadata)
    }
}
