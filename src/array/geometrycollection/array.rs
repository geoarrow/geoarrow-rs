use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, GenericListArray, LargeListArray, ListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};

use crate::algorithm::native::eq::offset_buffer_eq;
use crate::array::geometrycollection::{GeometryCollectionBuilder, GeometryCollectionCapacity};
use crate::array::metadata::ArrayMetadata;
use crate::array::util::{offsets_buffer_i32_to_i64, offsets_buffer_i64_to_i32};
use crate::array::{CoordBuffer, CoordType, MixedGeometryArray, WKBArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::GeometryCollectionTrait;
use crate::scalar::GeometryCollection;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, IntoArrow};
use crate::GeometryArrayTrait;

/// An immutable array of GeometryCollection geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<GeometryCollection>>` due to the internal
/// validity bitmap.
#[derive(Debug, Clone)]
pub struct GeometryCollectionArray<O: OffsetSizeTrait> {
    // Always GeoDataType::GeometryCollection or GeoDataType::LargeGeometryCollection
    data_type: GeoDataType,

    metadata: Arc<ArrayMetadata>,

    pub(crate) array: MixedGeometryArray<O>,

    /// Offsets into the mixed geometry array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<O>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

impl<O: OffsetSizeTrait> GeometryCollectionArray<O> {
    /// Create a new GeometryCollectionArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    pub fn new(
        array: MixedGeometryArray<O>,
        geom_offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
        metadata: Arc<ArrayMetadata>,
    ) -> Self {
        let coord_type = array.coord_type();
        let data_type = match O::IS_LARGE {
            true => GeoDataType::LargeGeometryCollection(coord_type),
            false => GeoDataType::GeometryCollection(coord_type),
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
        let validity_len = self.validity().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes::<O>()
    }
}

impl<O: OffsetSizeTrait> GeometryArrayTrait for GeometryCollectionArray<O> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self) -> &GeoDataType {
        &self.data_type
    }

    fn storage_type(&self) -> DataType {
        todo!()
    }

    fn extension_field(&self) -> Arc<Field> {
        let mut metadata = HashMap::with_capacity(2);
        metadata.insert(
            "ARROW:extension:name".to_string(),
            self.extension_name().to_string(),
        );
        metadata.insert(
            "ARROW:extension:metadata".to_string(),
            serde_json::to_string(self.metadata.as_ref()).unwrap(),
        );
        Arc::new(Field::new("geometry", self.storage_type(), true).with_metadata(metadata))
    }

    fn extension_name(&self) -> &str {
        "geoarrow.geometrycollection"
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
    fn validity(&self) -> Option<&NullBuffer> {
        self.validity.as_ref()
    }

    fn as_ref(&self) -> &dyn GeometryArrayTrait {
        self
    }
}

impl<O: OffsetSizeTrait> GeometryArraySelfMethods for GeometryCollectionArray<O> {
    fn with_coords(self, _coords: CoordBuffer) -> Self {
        todo!()
    }

    fn into_coord_type(self, _coord_type: CoordType) -> Self {
        todo!()
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
    fn slice(&self, offset: usize, length: usize) -> Self {
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

    fn owned_slice(&self, _offset: usize, _length: usize) -> Self {
        todo!()
    }
}

impl<'a, O: OffsetSizeTrait> GeometryArrayAccessor<'a> for GeometryCollectionArray<O> {
    type Item = GeometryCollection<'a, O>;
    type ItemGeo = geo::GeometryCollection;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        GeometryCollection::new(&self.array, &self.geom_offsets, index)
    }
}

impl<O: OffsetSizeTrait> IntoArrow for GeometryCollectionArray<O> {
    type ArrowArray = GenericListArray<O>;

    fn into_arrow(self) -> Self::ArrowArray {
        let geometries_field = self.geometries_field();
        let validity = self.validity;
        let values = self.array.into_array_ref();
        GenericListArray::new(geometries_field, self.geom_offsets, values, validity)
    }
}

impl TryFrom<&GenericListArray<i32>> for GeometryCollectionArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: &GenericListArray<i32>) -> Result<Self> {
        let geoms: MixedGeometryArray<i32> = value.values().as_ref().try_into()?;
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

impl TryFrom<&GenericListArray<i64>> for GeometryCollectionArray<i64> {
    type Error = GeoArrowError;

    fn try_from(value: &GenericListArray<i64>) -> Result<Self> {
        let geoms: MixedGeometryArray<i64> = value.values().as_ref().try_into()?;
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

impl TryFrom<&dyn Array> for GeometryCollectionArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                downcasted.try_into()
            }
            DataType::LargeList(_) => {
                let downcasted = value.as_any().downcast_ref::<LargeListArray>().unwrap();
                let geom_array: GeometryCollectionArray<i64> = downcasted.try_into()?;
                geom_array.try_into()
            }
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<&dyn Array> for GeometryCollectionArray<i64> {
    type Error = GeoArrowError;

    fn try_from(value: &dyn Array) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => {
                let downcasted = value.as_any().downcast_ref::<ListArray>().unwrap();
                let geom_array: GeometryCollectionArray<i32> = downcasted.try_into()?;
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

impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>> From<&[G]>
    for GeometryCollectionArray<O>
{
    fn from(other: &[G]) -> Self {
        let mut_arr: GeometryCollectionBuilder<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait, G: GeometryCollectionTrait<T = f64>> From<Vec<Option<G>>>
    for GeometryCollectionArray<O>
{
    fn from(other: Vec<Option<G>>) -> Self {
        let mut_arr: GeometryCollectionBuilder<O> = other.into();
        mut_arr.into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<WKBArray<O>> for GeometryCollectionArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: WKBArray<O>) -> Result<Self> {
        let mut_arr: GeometryCollectionBuilder<O> = value.try_into()?;
        Ok(mut_arr.into())
    }
}

impl From<GeometryCollectionArray<i32>> for GeometryCollectionArray<i64> {
    fn from(value: GeometryCollectionArray<i32>) -> Self {
        Self::new(
            value.array.into(),
            offsets_buffer_i32_to_i64(&value.geom_offsets),
            value.validity,
            value.metadata,
        )
    }
}

impl TryFrom<GeometryCollectionArray<i64>> for GeometryCollectionArray<i32> {
    type Error = GeoArrowError;

    fn try_from(value: GeometryCollectionArray<i64>) -> Result<Self> {
        Ok(Self::new(
            value.array.try_into()?,
            offsets_buffer_i64_to_i32(&value.geom_offsets)?,
            value.validity,
            value.metadata,
        ))
    }
}

/// Default to an empty array
impl<O: OffsetSizeTrait> Default for GeometryCollectionArray<O> {
    fn default() -> Self {
        GeometryCollectionBuilder::default().into()
    }
}

impl<O: OffsetSizeTrait> PartialEq for GeometryCollectionArray<O> {
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
