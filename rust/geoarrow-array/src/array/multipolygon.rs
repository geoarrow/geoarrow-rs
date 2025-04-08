use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use geoarrow_schema::{Metadata, MultiPolygonType};

use crate::array::{CoordBuffer, PolygonArray, WKBArray};
use crate::builder::MultiPolygonBuilder;
use crate::capacity::MultiPolygonCapacity;
use crate::datatypes::GeoArrowType;
use crate::eq::offset_buffer_eq;
use crate::error::{GeoArrowError, Result};
use crate::scalar::MultiPolygon;
use crate::trait_::{ArrayAccessor, GeoArrowArray, IntoArrow};
use crate::util::{OffsetBufferUtils, offsets_buffer_i64_to_i32};

/// An immutable array of MultiPolygon geometries.
///
/// This is semantically equivalent to `Vec<Option<MultiPolygon>>` due to the internal validity
/// bitmap.
#[derive(Debug, Clone)]
pub struct MultiPolygonArray {
    pub(crate) data_type: MultiPolygonType,

    pub(crate) coords: CoordBuffer,

    /// Offsets into the polygon array where each geometry starts
    pub(crate) geom_offsets: OffsetBuffer<i32>,

    /// Offsets into the ring array where each polygon starts
    pub(crate) polygon_offsets: OffsetBuffer<i32>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: OffsetBuffer<i32>,

    /// Validity bitmap
    pub(crate) validity: Option<NullBuffer>,
}

pub(super) fn check(
    coords: &CoordBuffer,
    geom_offsets: &OffsetBuffer<i32>,
    polygon_offsets: &OffsetBuffer<i32>,
    ring_offsets: &OffsetBuffer<i32>,
    validity_len: Option<usize>,
) -> Result<()> {
    if validity_len.is_some_and(|len| len != geom_offsets.len_proxy()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }
    if *ring_offsets.last() as usize != coords.len() {
        return Err(GeoArrowError::General(
            "largest ring offset must match coords length".to_string(),
        ));
    }

    if *polygon_offsets.last() as usize != ring_offsets.len_proxy() {
        return Err(GeoArrowError::General(
            "largest polygon offset must match ring offsets length".to_string(),
        ));
    }

    if *geom_offsets.last() as usize != polygon_offsets.len_proxy() {
        return Err(GeoArrowError::General(
            "largest geometry offset must match polygon offsets length".to_string(),
        ));
    }

    Ok(())
}

impl MultiPolygonArray {
    /// Create a new MultiPolygonArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Panics
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest polygon offset does not match the size of ring offsets
    /// - if the largest geometry offset does not match the size of polygon offsets
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        polygon_offsets: OffsetBuffer<i32>,
        ring_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Self {
        Self::try_new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
            metadata,
        )
        .unwrap()
    }

    /// Create a new MultiPolygonArray from parts
    ///
    /// # Implementation
    ///
    /// This function is `O(1)`.
    ///
    /// # Errors
    ///
    /// - if the validity is not `None` and its length is different from the number of geometries
    /// - if the largest ring offset does not match the number of coordinates
    /// - if the largest polygon offset does not match the size of ring offsets
    /// - if the largest geometry offset does not match the size of polygon offsets
    pub fn try_new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        polygon_offsets: OffsetBuffer<i32>,
        ring_offsets: OffsetBuffer<i32>,
        validity: Option<NullBuffer>,
        metadata: Arc<Metadata>,
    ) -> Result<Self> {
        check(
            &coords,
            &geom_offsets,
            &polygon_offsets,
            &ring_offsets,
            validity.as_ref().map(|v| v.len()),
        )?;
        Ok(Self {
            data_type: MultiPolygonType::new(coords.coord_type(), coords.dim(), metadata),
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
        })
    }

    fn vertices_field(&self) -> Arc<Field> {
        Field::new("vertices", self.coords.storage_type(), false).into()
    }

    fn rings_field(&self) -> Arc<Field> {
        let name = "rings";
        Field::new_list(name, self.vertices_field(), false).into()
    }

    fn polygons_field(&self) -> Arc<Field> {
        let name = "polygons";
        Field::new_list(name, self.rings_field(), false).into()
    }

    /// Access the underlying coordinate buffer
    pub fn coords(&self) -> &CoordBuffer {
        &self.coords
    }

    #[allow(dead_code)]
    pub(crate) fn into_inner(
        self,
    ) -> (
        CoordBuffer,
        OffsetBuffer<i32>,
        OffsetBuffer<i32>,
        OffsetBuffer<i32>,
    ) {
        (
            self.coords,
            self.geom_offsets,
            self.polygon_offsets,
            self.ring_offsets,
        )
    }

    /// Access the underlying geometry offsets buffer
    pub fn geom_offsets(&self) -> &OffsetBuffer<i32> {
        &self.geom_offsets
    }

    /// Access the underlying polygon offsets buffer
    pub fn polygon_offsets(&self) -> &OffsetBuffer<i32> {
        &self.polygon_offsets
    }

    /// Access the underlying ring offsets buffer
    pub fn ring_offsets(&self) -> &OffsetBuffer<i32> {
        &self.ring_offsets
    }

    /// The lengths of each buffer contained in this array.
    pub fn buffer_lengths(&self) -> MultiPolygonCapacity {
        MultiPolygonCapacity::new(
            *self.ring_offsets.last() as usize,
            *self.polygon_offsets.last() as usize,
            *self.geom_offsets.last() as usize,
            self.len(),
        )
    }

    /// The number of bytes occupied by this array.
    pub fn num_bytes(&self) -> usize {
        let validity_len = self.nulls().map(|v| v.buffer().len()).unwrap_or(0);
        validity_len + self.buffer_lengths().num_bytes()
    }

    /// Slices this [`MultiPolygonArray`] in place.
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
            polygon_offsets: self.polygon_offsets.clone(),
            ring_offsets: self.ring_offsets.clone(),
            validity: self.validity.as_ref().map(|v| v.slice(offset, length)),
        }
    }
}

impl GeoArrowArray for MultiPolygonArray {
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
        GeoArrowType::MultiPolygon(self.data_type.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Arc<dyn GeoArrowArray> {
        Arc::new(self.slice(offset, length))
    }
}

impl<'a> ArrayAccessor<'a> for MultiPolygonArray {
    type Item = MultiPolygon<'a>;

    unsafe fn value_unchecked(&'a self, index: usize) -> Result<Self::Item> {
        Ok(MultiPolygon::new(
            &self.coords,
            &self.geom_offsets,
            &self.polygon_offsets,
            &self.ring_offsets,
            index,
        ))
    }
}

impl IntoArrow for MultiPolygonArray {
    type ArrowArray = GenericListArray<i32>;
    type ExtensionType = MultiPolygonType;

    fn into_arrow(self) -> Self::ArrowArray {
        let vertices_field = self.vertices_field();
        let rings_field = self.rings_field();
        let polygons_field = self.polygons_field();

        let validity = self.validity;
        let coord_array = ArrayRef::from(self.coords);
        let ring_array = Arc::new(GenericListArray::new(
            vertices_field,
            self.ring_offsets,
            coord_array,
            None,
        ));
        let polygons_array = Arc::new(GenericListArray::new(
            rings_field,
            self.polygon_offsets,
            ring_array,
            None,
        ));
        GenericListArray::new(polygons_field, self.geom_offsets, polygons_array, validity)
    }

    fn ext_type(&self) -> &Self::ExtensionType {
        &self.data_type
    }
}

impl TryFrom<(&GenericListArray<i32>, MultiPolygonType)> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from((geom_array, typ): (&GenericListArray<i32>, MultiPolygonType)) -> Result<Self> {
        let geom_offsets = geom_array.offsets();
        let validity = geom_array.nulls();

        let polygons_dyn_array = geom_array.values();
        let polygons_array = polygons_dyn_array.as_list::<i32>();

        let polygon_offsets = polygons_array.offsets();
        let rings_dyn_array = polygons_array.values();
        let rings_array = rings_dyn_array.as_list::<i32>();

        let ring_offsets = rings_array.offsets();
        let coords = CoordBuffer::from_arrow(rings_array.values().as_ref(), typ.dimension())?;

        Ok(Self::new(
            coords,
            geom_offsets.clone(),
            polygon_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&GenericListArray<i64>, MultiPolygonType)> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from((geom_array, typ): (&GenericListArray<i64>, MultiPolygonType)) -> Result<Self> {
        let geom_offsets = offsets_buffer_i64_to_i32(geom_array.offsets())?;
        let validity = geom_array.nulls();

        let polygons_dyn_array = geom_array.values();
        let polygons_array = polygons_dyn_array.as_list::<i64>();

        let polygon_offsets = offsets_buffer_i64_to_i32(polygons_array.offsets())?;
        let rings_dyn_array = polygons_array.values();
        let rings_array = rings_dyn_array.as_list::<i64>();

        let ring_offsets = offsets_buffer_i64_to_i32(rings_array.offsets())?;
        let coords = CoordBuffer::from_arrow(rings_array.values().as_ref(), typ.dimension())?;

        Ok(Self::new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity.cloned(),
            typ.metadata().clone(),
        ))
    }
}

impl TryFrom<(&dyn Array, MultiPolygonType)> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from((value, typ): (&dyn Array, MultiPolygonType)) -> Result<Self> {
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

impl TryFrom<(&dyn Array, &Field)> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from((arr, field): (&dyn Array, &Field)) -> Result<Self> {
        let typ = field.try_extension_type::<MultiPolygonType>()?;
        (arr, typ).try_into()
    }
}

impl<O: OffsetSizeTrait> TryFrom<(WKBArray<O>, MultiPolygonType)> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from(value: (WKBArray<O>, MultiPolygonType)) -> Result<Self> {
        let mut_arr: MultiPolygonBuilder = value.try_into()?;
        Ok(mut_arr.finish())
    }
}

impl From<PolygonArray> for MultiPolygonArray {
    fn from(value: PolygonArray) -> Self {
        let metadata = value.data_type.metadata().clone();
        let coords = value.coords;
        let geom_offsets = OffsetBuffer::from_lengths(vec![1; coords.len()]);
        let ring_offsets = value.ring_offsets;
        let polygon_offsets = value.geom_offsets;
        let validity = value.validity;
        Self::new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
            metadata,
        )
    }
}

impl PartialEq for MultiPolygonArray {
    fn eq(&self, other: &Self) -> bool {
        if self.validity != other.validity {
            return false;
        }

        if !offset_buffer_eq(&self.geom_offsets, &other.geom_offsets) {
            return false;
        }

        if !offset_buffer_eq(&self.polygon_offsets, &other.polygon_offsets) {
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
