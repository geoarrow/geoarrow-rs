use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use geoarrow_schema::{Metadata, MultiPointType};

use crate::array::{CoordBuffer, LineStringArray, PointArray, WKBArray};
use crate::builder::MultiPointBuilder;
use crate::capacity::MultiPointCapacity;
use crate::datatypes::GeoArrowType;
use crate::eq::offset_buffer_eq;
use crate::error::{GeoArrowError, Result};
use crate::scalar::MultiPoint;
use crate::trait_::{ArrayAccessor, GeoArrowArray, IntoArrow};
use crate::util::{offsets_buffer_i64_to_i32, OffsetBufferUtils};

/// An immutable array of MultiPoint geometries using GeoArrow's in-memory representation.
///
/// This is semantically equivalent to `Vec<Option<MultiPoint>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
pub struct MultiPointArray {
    pub(crate) data_type: MultiPointType,

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

impl MultiPointArray {
    /// Create a new MultiPointArray from parts
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

    /// Create a new MultiPointArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the geometry offsets do not match the number of coordinates
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        check(&coords, validity.as_ref().map(|v| v.len()), &geom_offsets)?;
        Ok(Self {
            data_type: MultiPointType::new(coords.coord_type(), coords.dim(), metadata),
            coords,
            geom_offsets,
            validity,
        })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("points", self.coords.storage_type(), false).into()
    }

    /// Access the underlying coord buffer
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
    pub fn buffer_lengths(&self) -> MultiPointCapacity {
        MultiPointCapacity::new(*self.geom_offsets.last() as usize, self.len())
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes()
    }

    /// Slices this [`MultiPointArray`] in place.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Examples
    /// ```ignore
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
}

impl GeoArrowArray for MultiPointArray {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_array_ref(self) -> ArrayRef {
        Arc::new(self.into_arrow())
    }

    fn to_array_ref(&self) -> ArrayRef {
        self.clone().into_array_ref()
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

    fn data_type(&self) -> GeoArrowType {
        GeoArrowType::MultiPoint(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl<'a> ArrayAccessor<'a> for MultiPointArray {
    type Item = MultiPoint<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        MultiPoint::new(&self.coords, &self.geom_offsets, index)
    }
}

impl IntoArrow for MultiPointArray {
    type ArrowArray = GenericListArray<i32>;
    type ExtensionType = MultiPointType;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = self.vertices_field();
        let validity = self.validity;
        let coord_array = self.coords.into();
        GenericListArray::new(vertices_field, self.geom_offsets, coord_array, validity)
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&GenericListArray<i32>, MultiPointType)> for MultiPointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&GenericListArray<i32>, MultiPointType)) -> Result<Self> {
        let coords = CoordBuffer::from_arrow(value.values().as_ref(), typ.dimension())?;
        let geom_offsets = value.offsets();
        let validity = value.nulls();

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            validity.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&GenericListArray<i64>, MultiPointType)> for MultiPointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&GenericListArray<i64>, MultiPointType)) -> Result<Self> {
        let coords = CoordBuffer::from_arrow(value.values().as_ref(), typ.dimension())?;
        let geom_offsets = offsets_buffer_i64_to_i32(value.offsets())?;
        let validity = value.nulls();

        Ok(Self::new(
            coords,
            geom_offsets,
            validity.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&dyn Array, MultiPointType)> for MultiPointArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, MultiPointType)) -> Result<Self> {
        match value.data_type() {
            DataType::List(_) => (value.as_list::<i32>(), typ).try_into(),
            DataType::LargeList(_) => (value.as_list::<i64>(), typ).try_into(),
            _ => Err(GeoArrowError::General(format!(
                "Unexpected type: {:?}",
                value.data_type()
            ))),
        }
    }
}

impl TryFrom<(&dyn Array, &Field)> for MultiPointArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<MultiPointType>()?;
        (arr, typ).try_into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, MultiPointType)> for MultiPointArray {
    type Error = GeoArrowError;

    fn try_from(value: (WKBArray<O>, MultiPointType)) -> Result<Self> {
        let mut_arr: MultiPointBuilder = value.try_into()?;
        Ok(mut_arr.finish())
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl From<MultiPointArray> for LineStringArray {
    fn from(value: MultiPointArray) -> Self {
        let metadata = value.data_type.metadata().clone();
        Self::new(value.coords, value.geom_offsets, value.validity, metadata)
    }
}

impl From<PointArray> for MultiPointArray {
    fn from(value: PointArray) -> Self {
        let metadata = value.data_type.metadata().clone();
        let coords = value.coords;
        let geom_offsets = OffsetBuffer::from_lengths(vec![1; coords.len()]);
        let validity = value.validity;
        Self::new(coords, geom_offsets, validity, metadata)
    }
}

impl PartialEq for MultiPointArray {
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
