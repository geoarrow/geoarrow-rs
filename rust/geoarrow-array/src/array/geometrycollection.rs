use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::extension::ExtensionType;
use arrow_schema::{DataType, Field};
use geo_traits::GeometryCollectionTrait;
use geoarrow_schema::{CoordType, Dimension, GeometryCollectionType, Metadata};

use crate::array::{
    CoordBuffer, LineStringArray, MixedGeometryArray, MultiLineStringArray, MultiPointArray,
    MultiPolygonArray, PointArray, PolygonArray, WKBArray,
};
use crate::builder::GeometryCollectionBuilder;
use crate::capacity::GeometryCollectionCapacity;
use crate::datatypes::NativeType;
use crate::eq::offset_buffer_eq;
use crate::error::{GeoArrowError, Result};
use crate::scalar::{Geometry, GeometryCollection};
use crate::trait_::{ArrayAccessor, ArrayBase, IntoArrow, NativeArray, NativeGeometryAccessor};
use crate::util::offsets_buffer_i64_to_i32;

/// An immutable array of GeometryCollection geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<GeometryCollection>>` due to the internal
/// validity bitmap.
#[derive(Debug, Clone)]
pub struct GeometryCollectionArray {
    data_type: GeometryCollectionType,

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
        metadata: Arc<Metadata>,
    ) -> Self {
        let coord_type = array.coord_type();
        Self {
            data_type: GeometryCollectionType::new(coord_type, array.dimension(), metadata),
            array,
            geom_offsets,
            validity,
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
            data_type: self.data_type.clone(),
            array: self.array.clone(),
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
            self.array.into_coord_type(coord_type),
            self.geom_offsets,
            self.validity,
            metadata,
        )
    }
}

impl ArrayBase for GeometryCollectionArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn storage_type(&self) -> DataType {
        self.data_type.data_type()
    }

    fn extension_field(&self) -> Arc<Field> {
        self.data_type.to_field("geometry", true).into()
    }

    fn extension_name(&self) -> &str {
        GeometryCollectionType::NAME
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
        NativeType::GeometryCollection(self.data_type.clone())
    }

    fn coord_type(&self) -> CoordType {
        self.array.coord_type()
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

impl NativeGeometryAccessor for GeometryCollectionArray {
    unsafe fn value_as_geometry_unchecked(&self, index: usize) -> crate::scalar::Geometry {
        Geometry::GeometryCollection(GeometryCollection::new(
            &self.array,
            &self.geom_offsets,
            index,
        ))
    }
}

impl<'a> ArrayAccessor<'a> for GeometryCollectionArray {
    type Item = GeometryCollection<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        GeometryCollection::new(&self.array, &self.geom_offsets, index)
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
        let metadata = Arc::new(Metadata::try_from(field)?);
        arr.data_type = arr.data_type.clone().with_metadata(metadata);
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
